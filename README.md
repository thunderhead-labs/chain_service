# Summary

This repository contains the [PoktInfo](https://beta.pokt.info) services that depend on Pocket's chain data. Rewards info ingests the proofs and claims in order to index every time a node earns by documenting reward and relay amounts. Nodes Info indexes the network's nodes information over time. Location info indexes where the nodes in nodes info are geographically located and other related information such as ISP and IP.

# Installation

1. Follow the Installation steps of [common](https://github.com/thunderhead-labs/common-os#readme), a requisite for PoktInfo which contains the ORM, generic interactions, and more.
2. Clone this repository
3. Follow the steps for the desired service below

# Rewards Info

To run this service and gather historical data:

`python run_rewards.py history <START_HEIGHT> <END_HEIGHT>`

To run this service "live" and collect current and future data:

`python run_rewards.py live`

`run_rewards.py` is the main file which invokes the logic in `rewards_calc.py`.

## Logic

### Historical:
1. `run_rewards.py` invokes `run_rewards()` which uses process pools to call `record_rewards()` for each block within the specified range
   1. Calls `get_relays_wrapper()`which queries all the claims on the state at `height - 1` and passes them to `get_relays()` which matches the proofs from `height` to the claims and multiplies them by the correct `relaysToTokensMultiplier` and `validatorPercentage`
   2. Perform sanity check to ensure that `(totalSupply(height) - totalSupply(height - 1)) * validatorPercentage == totalRewards(height)`
   3. Saves reward information in db

### Live:
Performs the same process as historical mode, but calls `record_rewards` starting at the last cached block sequentially. If the last cached block is equal to the current height, the process will sleep until the current height increments. We separate `historical` and `live` processes because `historical` allows for indexing multiple blocks at once which is beneficial given Pocket's long query times.

### Schema

Please see [here](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L229) for the rewards info schema definition.

# Nodes Info

To run this service in historical mode:

`python run_rewards.py history <START_HEIGHT> <END_HEIGHT>`

To run this service in live mode:

`python run_rewards.py live`

`run_nodes.py` is the main file which invokes the logic in `nodes_info.py`.

## Logic

### Historical:
1. `run_nodes_info(from_height: int, to_height: int)` is called and sequentially calls `record_nodes_info_wrapper` for each block
   1. The calls are made in series because the state of `height - 1` is required to manage changes to a node at `height`
2. Queries all nodes at `height` and iterates through each node to check for changes to the nodes `service_url`, `chains` or `unstaking_time`.
3. If there is a change, the `end_height` of the node in the table will be marked to `height - 1`, and it will create a new row with `start_height = height`

### Live:

Live mode starts from the last indexed height and will call `record_nodes_info_wrapper` until the indexed height is equal to the current height. It will sleep until the current height increments.

### Schema

Please see [here](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L210) for the nodes info schema definition.

# Location Info

To run location info:

`python3 location_service.py <RAN_FROM>`

`ran_from` specifies the region where the instance of location info is running from. Because of modifications like [geomesh](https://github.com/pokt-scan/pocket-core), where operators are able to have multiple clients per `address` by using a regional load balancing service, one must run location info in several regions in order to detect all the actual instances of a given servicer/validator. Based on where this service is ran, you will get different results. (e.g. If you run from the U.S, you will find U.S versions of the network's nodes rather than an exhaustive list) Options: `na, eu, sg`

Logic and main is in `location_service.py`.

Be sure that you update the ip-api key in common repo `ip_api_utils.py`.

## Logic

### Live:
1. Every 6 hours `run_location_service()` is called
   1. Active nodes are queried from nodes_info
   2. For each node, location data is queried from [ip-api](https://ip-api.com) using the node's `service_url`
   3. If`address`, `city`, `ip` or `isp` differ for the recorded node, than the old one's `end_height` will be specified and a new row will be created. The `ran_from` column will be set to the value specified in the CLI argument.

Because you cannot find out where something was physically located in the past, location info only has a live mode.

### Schema

Please see [here](https://github.com/thunderhead-labs/common-os/blob/master/common/orm/schema/poktinfo.py#L149) for the nodes info schema definition.
