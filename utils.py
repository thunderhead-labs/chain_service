import json

import requests
from common.utils import get_account_txs


def get_amount_out(height: int, address: str):
    amount_out = 0
    txs = get_account_txs(height, address)
    for tx in txs:
        if tx["stdTx"]["msg"]["type"] == "pos/Send" and tx["tx_result"]["code"] == 0:
            amount_out += int(tx["stdTx"]["msg"]["value"]["amount"])
    return amount_out


def sandwalker_get_rewards(height):
    rewards_req = requests.post(
        url="https://sandwalker.sbrk.org/api/block",
        headers={
            "Content-Type": "application/json",
            "Accept": "Accept: application/json",
        },
        data=json.dumps({"block": height}),
    )
    rewards_resp = rewards_req.json()["entries"]
    rewards_dict = {}
    for reward_entry in rewards_resp:
        if reward_entry["account"] not in rewards_dict:
            rewards_dict[reward_entry["account"]] = [reward_entry["reward"]]
        else:
            rewards_dict[reward_entry["account"]].append(reward_entry["reward"])
    return rewards_dict
