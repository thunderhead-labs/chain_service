from math import ceil
from unittest import TestCase

from rewards_calc import get_relays_wrapper
from utils import sandwalker_get_rewards


class RewardsTest(TestCase):
    def test_relays_wrapper(self):
        height = 57112
        self.run_relays_wrapper_test_case(height)

        height = 49856
        self.run_relays_wrapper_test_case(height)

        height = 52985
        self.run_relays_wrapper_test_case(height)

        height = 47230
        self.run_relays_wrapper_test_case(height)

        height = 42951
        self.run_relays_wrapper_test_case(height)

    def run_relays_wrapper_test_case(self, height):
        relays_dict = get_relays_wrapper(height)
        if relays_dict is not None:
            sandwalker_rewards_dict = sandwalker_get_rewards(height)
            self.assertTrue(
                "Report" in relays_dict and "Error" not in relays_dict,
                "Result is not valid",
            )
            app_reports = relays_dict["Report"]["AppReports"]
            addresses = list(app_reports.keys())
            for app_address in addresses:
                app_report = app_reports[app_address]
                node_address = app_report["Service"]["Address"]
                node_reward = ceil(app_report["Service"]["Reward"])
                expected_rewards = sandwalker_rewards_dict[node_address]
                self.assertTrue(
                    node_reward in expected_rewards
                    or node_reward + 1 in expected_rewards,
                    f"{node_reward} is not in {expected_rewards} for address "
                    f"{node_address} at block {height}",
                )
