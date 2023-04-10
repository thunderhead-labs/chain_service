import os
import typing
from typing import List
from urllib.parse import urlparse

import pandas as pd
from common.db_utils import (
    ConnFactory,
)
from common.loggers import get_logger
from common.orm.repository import PoktInfoRepository
from common.orm.schema import NodesInfo, ServicesState
from common.utils import get_nodes, get_block_ts
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt

SERVICE_CLASS = NodesInfo
SERVICE_NAME = NodesInfo.__tablename__
path = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(path, SERVICE_NAME, SERVICE_NAME)
perf_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_profiler")


def run_nodes_info(
    from_height: int,
    to_height: int,
    skip=1,
    heights=None,
    save_state=False,
    skip_recorded=False,
) -> None:
    nodes_dict = {}

    try:
        heights = (
            heights
            if heights is not None
            else list(range(from_height, to_height, skip))
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
        for height in heights:
            record_nodes_info_wrapper(height, nodes_dict, save_state)
    except Exception as e:
        logger.error(e)
        logger.error("Caught Exception: ", exc_info=e)


@retry(stop=stop_after_attempt(5))
def record_nodes_info_wrapper(height, nodes_dict=None, save_state=False) -> None:
    """
    Wrapper for recording nodes info
    """
    if nodes_dict is None:
        nodes_dict = {}

    with ConnFactory.poktinfo_conn() as session:
        try:
            now = pd.Timestamp.now()
            perf_logger.debug(f"Started {height}")
            nodes_info = get_nodes(height)
            perf_logger.info(
                f"Got nodes info of {height}," f"took {pd.Timestamp.now() - now}"
            )
            if nodes_info is not None:
                now2 = pd.Timestamp.now()
                perf_logger.debug(f"Started recording nodes info {height}")
                has_saved = record_nodes_info(nodes_info, height, nodes_dict, session)
                if save_state and has_saved:
                    # Save height for service as success
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
                perf_logger.info(
                    f"Recorded nodes info {height}," f"took {pd.Timestamp.now() - now2}"
                )
                perf_logger.debug(
                    f"Finished {height}, " f"took {pd.Timestamp.now() - now}"
                )
        except Exception as e:
            logger.error(f"Error at block {height}: ", exc_info=e)

            if save_state:
                # Save height for service as fail
                has_added = PoktInfoRepository.upsert(
                    session,
                    ServicesState(service=SERVICE_NAME, height=height, status="fail"),
                )
                if not has_added:
                    logger.error(
                        f"Failed adding state entry: {SERVICE_NAME, height}, fail"
                    )


def record_nodes_info(
    nodes_info: List[dict], height: int, nodes_dict: dict, session: Session
) -> bool:
    """
    Records info of new nodes and updates current nodes
    """
    nodes = []
    has_saved = True
    current_block_ts = get_block_ts(height)
    logger.info(f"Processing {len(nodes_info)} nodes at {height}")
    for node_info in nodes_info:
        try:
            node = record_node(session, current_block_ts, height, node_info, nodes_dict)
            if node:
                nodes.append(node)
        except Exception as e:
            logger.error(f"Error at block {height}: ", exc_info=e)
    if nodes:
        has_saved = PoktInfoRepository.save_many(session, nodes)
    logger.info(f"Saved {len(nodes)} nodes - {has_saved} at {height}")
    logger.info(f"Nodes dict size: {len(nodes_dict)}")
    return has_saved


def has_chains_changed(chains: List[str], prev_chains: List[str]) -> bool:
    """
    Checks if chains have changed
    """
    return len(chains) != len(prev_chains) or any(
        chain not in prev_chains for chain in chains
    )


def record_node(
    session: Session,
    current_block_ts: pd.Timestamp,
    height: int,
    node_info: dict,
    nodes_dict: dict,
) -> typing.Optional[NodesInfo]:
    node = None
    address = node_info["address"]
    url = node_info["service_url"]
    chains = node_info["chains"]
    parsed_url = urlparse(url)
    domain = ".".join(parsed_url.netloc.split(".")[-2:]).split(":")[0]
    if parsed_url.hostname:
        subdomain = ".".join(parsed_url.hostname.split(".")[:-2])
    else:
        logger.error(f"Invalid url: {url}")
        subdomain = ""
    start_height, end_height = height, None

    skip = True if address in nodes_dict and not nodes_dict[address][1] else False
    if not skip and (
        address in nodes_dict or PoktInfoRepository.is_node_recorded(session, address)
    ):

        has_url_changed = (
            nodes_dict[address][0] != url
            if address in nodes_dict
            else PoktInfoRepository.has_url_changed(session, address, url)
        )
        has_chain_changed = (
            has_chains_changed(chains, nodes_dict[address][2])
            if address in nodes_dict
            else PoktInfoRepository.has_chain_changed(session, address, chains)
        )
        if has_url_changed or has_chain_changed:
            end_height = height - 1
            has_updated = PoktInfoRepository.update_node_end_height(
                session, address, end_height, False
            )
            if has_updated:
                if has_url_changed:
                    logger.info(
                        f"Updated {address} at {end_height} to {url} from {nodes_dict[address][0]}"
                    )
                elif has_chain_changed:
                    logger.info(
                        f"Updated {address} at {end_height} to {chains} from {nodes_dict[address][2]}"
                    )

                node = NodesInfo(
                    address=address,
                    url=url,
                    domain=domain,
                    subdomain=subdomain,
                    chains=";".join(chains),
                    height=height,
                    start_height=start_height,
                    end_height=None,
                    is_staked=True,
                )
            else:
                logger.error(f"Failed updating node info of {address, height}")
                raise Exception(f"Failed updating node info of {address, height}")

        nodes_dict[address] = (url, True, chains)

        handle_unstaked(
            address, session, current_block_ts, height, node_info, nodes_dict
        )

    else:
        if node_info["unstaking_time"] != "0001-01-01T00:00:00Z":
            unstaked_time = pd.Timestamp(node_info["unstaking_time"], tz="utc")
        else:
            unstaked_time = pd.Timestamp(0, tz="utc")

        # If unstaked time is in the past
        if current_block_ts > unstaked_time:
            node = NodesInfo(
                address=address,
                url=url,
                domain=domain,
                subdomain=subdomain,
                chains=";".join(chains),
                height=height,
                start_height=start_height,
                end_height=end_height,
                is_staked=True,
            )
            nodes_dict[address] = (url, True, chains)
        else:
            nodes_dict[address] = (url, False, chains)

    return node


def handle_unstaked(
    address, session, current_block_ts, height, node_info, nodes_dict
) -> None:
    # Check if unstaking_time is valid
    if node_info["unstaking_time"] != "0001-01-01T00:00:00Z":
        unstaked_time = pd.Timestamp(node_info["unstaking_time"], tz="utc")
    else:
        unstaked_time = None

    # If unstaking time is valid and set to the future
    # update node info in db and remove from nodes_dict
    if unstaked_time is not None and unstaked_time > current_block_ts:
        end_height = height
        has_updated = PoktInfoRepository.update_node_end_height(
            session, address, end_height, False
        )
        if has_updated:
            nodes_dict.pop(address, None)
            logger.info(
                f"Unstaked at {unstaked_time} - Updated {address} at {end_height}"
            )
        else:
            logger.error(f"Failed updating node info of {address, height}")
            raise Exception(f"Failed updating node info of {address, height}")
