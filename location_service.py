import os
import sys
from time import sleep
from urllib.parse import urlparse

import pandas as pd
from common.db_utils import (
    ConnFactory,
)
from common.ip_api_utils import get_location_data
from common.loggers import get_logger
from common.orm.repository import PoktInfoRepository
from common.orm.schema import LocationInfo, ServicesState
from common.utils import get_last_block_height
from sqlalchemy.orm import Session

SERVICE_CLASS = LocationInfo
SERVICE_NAME = SERVICE_CLASS.__tablename__
ran_from = str(sys.argv[1]) if len(sys.argv) > 1 else "local"
path = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(path, SERVICE_NAME, SERVICE_NAME)
perf_logger = get_logger(path, SERVICE_NAME, f"{SERVICE_NAME}_profiler")


def run_location_service(session: Session, height: int) -> None:
    active_nodes = PoktInfoRepository.get_all_active_nodes(session)
    active_nodes_dict = {node.address: node for node in active_nodes}
    locations = []
    for address in active_nodes_dict:
        node = active_nodes_dict[address]
        url = str(node.url)
        url = ".".join(urlparse(url).hostname.split("."))
        location_data = get_location_data(url)
        if location_data[0] != "fail":
            (
                ip,
                continent,
                country,
                region,
                city,
                lat,
                lon,
                isp,
                org,
                as_,
            ) = location_data
            if PoktInfoRepository.is_location_recorded(
                session, address, ran_from=ran_from
            ):
                if PoktInfoRepository.has_location_changed(
                    session, address, city, ip, isp, ran_from=ran_from
                ):
                    has_updated = PoktInfoRepository.update_location(
                        session,
                        address,
                        height - 1,
                        ran_from=ran_from,
                    )
                    if has_updated:
                        location_info = LocationInfo(
                            address=address,
                            ip=ip,
                            continent=continent,
                            country=country,
                            region=region,
                            city=city,
                            lat=lat,
                            lon=lon,
                            isp=isp,
                            org=org,
                            as_=as_,
                            height=height,
                            start_height=height,
                            ran_from=ran_from,
                        )
                        locations.append(location_info)
                        logger.info(
                            f"Updated location: {address, ip, city, height - 1}"
                        )
                    else:
                        logger.error(
                            f"Failed updating location: {address, ip, city, height - 1}"
                        )
            else:
                location_info = LocationInfo(
                    address=address,
                    ip=ip,
                    continent=continent,
                    country=country,
                    region=region,
                    city=city,
                    lat=lat,
                    lon=lon,
                    isp=isp,
                    org=org,
                    as_=as_,
                    height=height,
                    start_height=height,
                    ran_from=ran_from,
                )
                locations.append(location_info)
        else:
            logger.error(f"{address} failed - {url} - {location_data[1]}")

    logger.info(f"Save many {len(locations), height}")
    PoktInfoRepository.save_many(session, locations)

    open_locations = PoktInfoRepository.get_open_locations(session, ran_from=ran_from)
    for location in open_locations:
        if location.address not in active_nodes_dict:
            PoktInfoRepository.update_location(
                session,
                location.address,
                height - 1,
                ran_from=ran_from,
            )


if __name__ == "__main__":
    save_state = True
    while True:
        current_height = get_last_block_height()
        with ConnFactory.poktinfo_conn() as session_:
            try:
                now = pd.Timestamp.now()
                perf_logger.info(f"Saving locations for {current_height}")
                run_location_service(session_, current_height)
                perf_logger.info(
                    f"Finished saving locations for "
                    f"{current_height}, took {pd.Timestamp.now() - now}"
                )
                if save_state:
                    # Save height for service as success
                    has_added_state = PoktInfoRepository.upsert(
                        session_,
                        ServicesState(
                            service=SERVICE_NAME,
                            height=current_height,
                            status="success",
                        ),
                    )
                    if not has_added_state:
                        logger.error(
                            f"Failed adding state entry: "
                            f"{SERVICE_NAME, current_height}, success"
                        )
            except Exception as e:
                print(f"Error at block {current_height}, {e}")
                logger.error(f"Error at block {current_height}: ", exc_info=e)

                if save_state:
                    # Save height for service as fail
                    has_added_state = PoktInfoRepository.upsert(
                        session_,
                        ServicesState(
                            service=SERVICE_NAME, height=current_height, status="fail"
                        ),
                    )
                    if not has_added_state:
                        logger.error(
                            f"Failed adding state entry: "
                            f"{SERVICE_NAME, current_height}, fail"
                        )
        sleep(3600 * 6)
