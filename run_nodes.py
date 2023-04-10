import sys
from time import sleep

from common.db_utils import (
    ConnFactory,
)
from common.orm.repository import PoktInfoRepository
from common.utils import get_last_block_height

from nodes_info import (
    run_nodes_info,
    record_nodes_info_wrapper,
    SERVICE_CLASS,
)

SAVE_STATE = True

if __name__ == "__main__":
    mode = str(sys.argv[1])

    # python3 run_nodes.py history 500 50000
    if mode == "history":
        skip_recorded = True
        # Historical mode - gets rewards for addresses between heights.
        from_height, to_height = int(sys.argv[2]), int(sys.argv[3])
        skip = int(sys.argv[4]) if len(sys.argv) > 4 else 1
        run_nodes_info(
            from_height,
            to_height,
            skip=skip,
            save_state=SAVE_STATE,
            skip_recorded=skip_recorded,
        )
    # python3 run_nodes.py live (optional to add height to start from)
    elif mode == "live":
        # Live mode - checks if new block has been created and if so, get rewards.
        with ConnFactory.poktinfo_conn() as session:
            last_height = (
                PoktInfoRepository.get_last_recorded_node_height(session)
                if len(sys.argv) < 3 or not sys.argv[2].isdigit()
                else int(sys.argv[2])
            )

        nodes_dict = {}
        while True:
            try:
                height = get_last_block_height()
                if height - 1 > last_height:
                    record_nodes_info_wrapper(
                        last_height, nodes_dict, save_state=SAVE_STATE
                    )
                    last_height += 1
            except Exception as e:
                print(e)
            sleep(60)
    # python3 run_nodes.py complete
    elif mode == "complete":
        # Retrieves from db failed blocks and reruns them
        with ConnFactory.poktinfo_conn() as session:
            heights = PoktInfoRepository.get_failed_blocks_of_service(
                session, SERVICE_CLASS
            )
        run_nodes_info(0, 0, heights=heights, save_state=SAVE_STATE)
