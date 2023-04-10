import sys
from time import sleep

from common.db_utils import (
    ConnFactory,
)
from common.orm.repository import PoktInfoRepository
from common.utils import get_last_block_height

from rewards_calc import run_rewards, record_rewards, SERVICE_CLASS

SAVE_STATE = True

if __name__ == "__main__":
    mode = str(sys.argv[1])

    # generate_valid_urls()

    # python3 run_rewards.py history 500 50000
    if mode == "history":
        skip_recorded = False
        # Historical mode - gets rewards for addresses between heights.
        from_height, to_height = int(sys.argv[2]), int(sys.argv[3])
        as_test = False
        run_rewards(
            from_height,
            to_height,
            as_test,
            save_state=SAVE_STATE,
            skip_recorded=skip_recorded,
        )
    # python3 run_rewards.py live (optional to add height to start from)
    elif mode == "live":
        # Live mode - checks if new block has been created and if so, get rewards.
        as_test = False
        with ConnFactory.poktinfo_conn() as session:
            last_height = (
                PoktInfoRepository.get_last_recorded_reward_height(session)
                if len(sys.argv) < 3 or not sys.argv[2].isdigit()
                else int(sys.argv[2])
            )
        while True:
            try:
                height = get_last_block_height()
                if height - 1 > last_height:
                    record_rewards(last_height, as_test, save_state=SAVE_STATE)
                    last_height += 1
            except Exception as e:
                print(e)
            sleep(60)
    # python3 run_rewards.py complete
    elif mode == "complete":
        as_test = False
        # Retrieves from db failed blocks and reruns them
        with ConnFactory.poktinfo_conn() as session:
            heights = PoktInfoRepository.get_failed_blocks_of_service(
                session, SERVICE_CLASS
            )
        run_rewards(0, 0, as_test, heights=heights, save_state=SAVE_STATE)
