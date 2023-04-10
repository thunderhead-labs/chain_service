import os
import typing
from math import ceil
from multiprocessing import Pool

import pandas as pd
from common.db_utils import (
    ConnFactory,
)
from common.loggers import get_logger
from common.orm.repository import PoktInfoRepository
from common.orm.schema import RewardsInfo, ServicesState
from common.utils import (
    get_inflation,
    get_relay_to_tokens_multiplier,
    get_reward_percentage,
    get_param,
    get_pip22_height,
    get_txs,
    get_claims,
    node_balance,
    get_address_from_pubkey,
)
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt

from utils import sandwalker_get_rewards

SERVICE_CLASS = RewardsInfo
SERVICE_NAME = SERVICE_CLASS.__tablename__
Pip22ExponentDenominator = 100

path = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(path, SERVICE_NAME, SERVICE_NAME)
perf_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_profiler")
sanity_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_sanity")

default_pip22_height = 69232
pip22_height = get_pip22_height(default_pip22_height)
servicer_stake_floor_multiplier = float(
    get_param(default_pip22_height, "pos/ServicerStakeFloorMultiplier")
)
servicer_stake_weight_ceiling = float(
    get_param(default_pip22_height, "pos/ServicerStakeWeightCeiling")
)
servicer_stake_floor_multiplier_exponent = float(
    get_param(default_pip22_height, "pos/ServicerStakeFloorMultiplier")
)
servicer_stake_weight_multiplier = float(
    get_param(default_pip22_height, "pos/ServicerStakeWeightMultiplier")
)


@retry(stop=stop_after_attempt(5))
def get_relays_wrapper(
    height, address="", session: typing.Optional[Session] = None
) -> typing.Optional[dict]:
    update_global_vars(height)

    relays_dict = None
    if session is None:
        return None

    txs = get_txs(height)
    if address != "":
        txs = filter_txs(txs, address)
    claims = get_claims(height - 1, address)
    is_genesis = True if height == 0 else False

    if txs and claims:
        relays_dict = get_relays(txs, claims, height, is_genesis)
        total_rewards = relays_dict["Report"]["TotalReward"]
        inflation = get_inflation(height) * get_reward_percentage(height)

        if total_rewards - inflation != 0:
            sanity_logger.info(
                f"Height: {height}, Diff: {total_rewards - inflation}, "
                f"Total rewards: {total_rewards}, Inflation: {inflation}"
            )

        if session and relays_dict and "Report" in relays_dict:
            has_added = PoktInfoRepository.save_many(
                session, list(relays_dict["Report"]["RewardsInfoObjs"].values())
            )
            if not has_added:
                has_added = PoktInfoRepository.upsert(
                    session,
                    ServicesState(service=SERVICE_NAME, height=height, status="fail"),
                )
                if not has_added:
                    print(f"Failed adding state entry: {SERVICE_NAME, height}, fail")
                    raise Exception(
                        f"Failed adding state entry: {SERVICE_NAME, height}, fail"
                    )
    return relays_dict


def update_global_vars(height: int) -> None:
    global servicer_stake_floor_multiplier, servicer_stake_weight_ceiling, servicer_stake_floor_multiplier_exponent, servicer_stake_weight_multiplier, pip22_height
    pip22_height = get_pip22_height(height)
    if height > pip22_height:
        servicer_stake_floor_multiplier = float(
            get_param(height, "pos/ServicerStakeFloorMultiplier")
        )
        servicer_stake_weight_ceiling = float(
            get_param(height, "pos/ServicerStakeWeightCeiling")
        )
        servicer_stake_floor_multiplier_exponent = float(
            get_param(height, "pos/ServicerStakeFloorMultiplier")
        )
        servicer_stake_weight_multiplier = float(
            get_param(height, "pos/ServicerStakeWeightMultiplier")
        )


def filter_txs(txs, address) -> typing.List[dict]:
    filtered_txs = []
    for tx in txs:
        signer = str(tx["tx_result"]["signer"]).lower()
        if signer == address:
            filtered_txs.append(tx)
    return filtered_txs


def get_relays(
    txs: typing.List[dict], claims: typing.List[dict], height: int, is_genesis: bool
) -> typing.Optional[dict]:
    """
    Gets proved txs details
    """
    logger.debug("GetRelays process started")
    ch_response = {}
    result = {
        "TotalBadTxs": 0,
        "TotalGoodTxs": 0,
        "TotalProofTxs": 0,
        "TotalChallengesCompleted": 0,
        "TotalRelaysCompleted": 0,
        "TotalReward": 0,
        "AppReports": {},
        "NodeReports": {},
        "RewardsInfoObjs": {},
    }

    if is_genesis:
        # there is nothing to do if is genesis block txs.
        logger.debug("RelaysReport created")
        ch_response["Report"] = result
        return ch_response

    token_multiplier = get_relay_to_tokens_multiplier(height)
    percentage = get_reward_percentage(height)
    claim_addresses = [claim["from_address"] for claim in claims]

    logger.debug(
        "Looping through all of the block-txs and matching "
        "them with the corresponding claims"
    )
    for tx in txs:
        result, ch_response = process_tx(
            tx,
            claims,
            height,
            result,
            claim_addresses,
            ch_response,
            token_multiplier,
            percentage,
        )
        if "Error" in ch_response:
            return ch_response
    logger.debug("GetRelays process completed")
    ch_response["Report"] = result
    return ch_response


def process_tx(
    tx: dict,
    claims: typing.List[dict],
    height: int,
    result: dict,
    claim_addresses: list,
    ch_response: dict,
    token_multiplier: int,
    percentage: float,
) -> typing.Tuple[dict, dict]:
    """
    Updates result with tx details if it has proof
    """
    # check if bad transaction
    if tx["tx_result"]["code"] != 0:
        result["TotalBadTxs"] += 1
        return result, ch_response
    # log good tx
    result["TotalGoodTxs"] += 1
    # if not proofTx, continue on
    if tx["tx_result"]["message_type"] != "proof":
        return result, ch_response

    # this is a proof msg - log good tx
    result["TotalProofTxs"] += 1
    signer = str(tx["tx_result"]["signer"]).lower()  # node address
    if signer not in claim_addresses:
        err = Exception("no claim for valid proof object")
        logger.error(err)
        ch_response["Error"] = err
        return result, ch_response

    indices = [i for i, x in enumerate(claim_addresses) if x == signer]
    signer_claims = [claims[ix] for ix in indices]  # claim tx
    signer_claims.sort(key=lambda x: x["expiration_height"], reverse=False)

    if len(signer_claims) == 1:
        claim = signer_claims[0]
        result, ch_response = get_relays_dict_of_claim(
            ch_response, claim, height, result, token_multiplier, percentage, tx
        )
    else:
        tx_app_pk = str(
            tx["stdTx"]["msg"]["value"]["leaf"]["value"]["aat"]["app_pub_key"]
        ).lower()
        tx_chain = tx["stdTx"]["msg"]["value"]["leaf"]["value"]["blockchain"]
        # Find matching claims
        for j in range(len(signer_claims)):
            claim_app_pk = str(signer_claims[j]["header"]["app_public_key"]).lower()
            if (
                claim_app_pk == tx_app_pk
                and signer_claims[j]["header"]["chain"] == tx_chain
            ):
                claim = signer_claims[j]
                result, ch_response = get_relays_dict_of_claim(
                    ch_response,
                    claim,
                    height,
                    result,
                    token_multiplier,
                    percentage,
                    tx,
                )
                if "Error" in ch_response:
                    break
    return result, ch_response


def get_relays_dict_of_claim(
    ch_response, claim, height, result, token_multiplier, percentage, tx
) -> typing.Tuple[dict, dict]:
    # check to see if claim is for relays
    et = int(claim["evidence_type"])
    if et != 1:
        result["TotalChallengesCompleted"] += 1
        return result, ch_response
    result = update_relays_dict(claim, height, result, token_multiplier, percentage, tx)
    return result, ch_response


def update_relays_dict(
    claim: dict,
    height: int,
    result: dict,
    token_multiplier: int,
    percentage: float,
    tx: dict,
) -> dict:
    # get app_address
    (
        app_address,
        app_report,
        chain,
        node_address,
        node_report,
        total_relays,
    ) = update_node_app_reports(claim, result, tx)

    if height >= pip22_height:
        stake = node_balance(node_address, height)
        floored_stake = min(
            stake - stake % servicer_stake_floor_multiplier,
            servicer_stake_weight_ceiling
            - servicer_stake_weight_ceiling % servicer_stake_floor_multiplier,
        )
        bin = floored_stake // servicer_stake_floor_multiplier
        stake_weight = bin / servicer_stake_weight_multiplier
    else:
        stake_weight = 1

    reward = int(total_relays * token_multiplier * percentage * stake_weight)

    app_details = {
        "TxHash": tx["hash"],
        "Address": node_address,
        "TotalRelays": total_relays,
        "RelayChain": chain,
        "Reward": reward,
    }
    node_details = {
        "TxHash": tx["hash"],
        "Address": app_address,
        "TotalRelays": total_relays,
        "RelayChain": chain,
        "Reward": reward,
    }

    # add an individual service report to the app_report
    app_report["Service"] = app_details
    # add an individual service report to the node_report
    node_report["Service"] = node_details
    result["TotalRelaysCompleted"] += total_relays
    # set the reports in the master report
    result["AppReports"][app_address] = app_report
    result["NodeReports"][node_address] = node_report
    result["TotalReward"] += reward
    if tx["hash"] not in result["RewardsInfoObjs"]:
        result["RewardsInfoObjs"][tx["hash"]] = RewardsInfo(
            tx_hash=tx["hash"],
            height=height,
            address=node_address,
            rewards=reward,
            chain=chain,
            relays=total_relays,
            token_multiplier=token_multiplier,
            percentage=percentage,
            stake_weight=stake_weight,
        )
    return result


def update_node_app_reports(
    claim: dict, result: dict, tx: dict
) -> typing.Tuple[str, dict, str, str, dict, int]:
    pubkey = claim["header"]["app_public_key"]
    app_address = get_address_from_pubkey(pubkey)
    node_address = claim["from_address"]
    node_address = str(node_address).lower()
    # get total # of relays
    total_relays = int(claim["total_proofs"])
    # get the relay chain id
    chain = claim["header"]["chain"]

    # retrieve the app/node reports
    if app_address in result["AppReports"]:
        app_report = result["AppReports"][app_address]
    else:
        app_report = {"TotalRelays": 0, "ServicedReportByChain": {}}

    if node_address in result["NodeReports"]:
        node_report = result["NodeReports"][node_address]
    else:
        node_report = {"TotalRelays": 0, "ServicedReportByChain": {}}

    node_report["Proof"] = tx["hash"]
    app_report["Proof"] = tx["hash"]
    # add to the reports totals
    app_report["TotalRelays"] += total_relays
    node_report["TotalRelays"] += total_relays

    # add to the chain statistics
    if chain not in app_report["ServicedReportByChain"]:
        app_report["ServicedReportByChain"][chain] = 0
    if chain not in node_report["ServicedReportByChain"]:
        node_report["ServicedReportByChain"][chain] = 0

    app_report["ServicedReportByChain"][chain] += total_relays
    node_report["ServicedReportByChain"][chain] += total_relays

    return app_address, app_report, chain, node_address, node_report, total_relays


def run_rewards(
    from_height: int,
    to_height: int,
    as_test=False,
    heights=None,
    save_state=False,
    skip_recorded=False,
) -> None:

    try:
        heights = (
            heights if heights is not None else list(range(from_height, to_height))
        )
        with ConnFactory.poktinfo_conn() as session:
            heights = (
                height
                for height in heights
                if not skip_recorded
                or not PoktInfoRepository.is_height_recorded(
                    session, SERVICE_NAME, height
                )
            )
        with Pool(processes=8) as tp:
            tp.starmap(
                record_rewards, [(height, as_test, save_state) for height in heights]
            )
        tp.join()

    except Exception as e:
        print(e)
        logger.error("Caught Exception: ", exc_info=e)


def record_rewards(height: int, as_test: bool, save_state: bool = False) -> None:
    try:
        now = pd.Timestamp.now()
        perf_logger.debug(f"{now}: Getting relays dict at {height}")
        with ConnFactory.poktinfo_conn() as session:
            relays_dict = get_relays_wrapper(height, session=session)
            perf_logger.info(
                f"{pd.Timestamp.now()}: Got relays dict at {height}, "
                f"took {pd.Timestamp.now() - now}"
            )

            if relays_dict is not None:
                if as_test:
                    save_state = rewards_test(height, relays_dict, save_state)
                else:
                    logger.debug(f"Finished {height}")
                if save_state:
                    has_added = PoktInfoRepository.upsert(
                        session,
                        ServicesState(
                            service=SERVICE_NAME, height=height, status="success"
                        ),
                    )
                    if not has_added:
                        logger.error(
                            f"Failed adding state entry: {SERVICE_NAME, height}, success"
                        )
            else:
                logger.info(f"Relays dict is None at {height}")

    except Exception as e:
        print(f"Error at block {height}, {e}")
        logger.error(f"Error at block {height}: ", exc_info=e)

        if save_state:
            has_added = PoktInfoRepository.upsert(
                session,
                ServicesState(service=SERVICE_NAME, height=height, status="fail"),
            )
            if not has_added:
                logger.error(f"Failed adding state entry: {SERVICE_NAME, height}, fail")


def rewards_test(height: int, relays_dict: dict, save_state: bool) -> bool:
    # Test rewards in relays dict by comparing it to rewards on sandwalker api
    sandwalker_rewards_dict = sandwalker_get_rewards(height)
    if "Report" not in relays_dict or "Error" in relays_dict:
        logger.error(f"Result is not valid at block {height}")
    app_reports = relays_dict["Report"]["AppReports"]
    addresses = list(app_reports.keys())
    for app_address in addresses:
        app_report = app_reports[app_address]
        node_address = app_report["Service"]["Address"]
        node_reward = ceil(app_report["Service"]["Reward"])
        expected_rewards = sandwalker_rewards_dict[node_address]
        if (
            node_reward not in expected_rewards
            and node_reward + 1 not in expected_rewards
        ):
            logger.error(
                f"{node_reward} is not in {expected_rewards} for address "
                f"{node_address} at block {height}"
            )
            save_state = False
    return save_state
