from typing import Any, Callable, Dict, Optional

import hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class ZtdxPerpetualRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.headers is None:
            request.headers = {}
        request.headers["Content-Type"] = (
            "application/json" if request.method in (RESTMethod.POST, RESTMethod.PUT) else "application/x-www-form-urlencoded"
        )
        return request


def public_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    base_url = CONSTANTS.REST_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_REST_URL
    return base_url + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN) -> str:
    return public_rest_url(path_url=path_url, domain=domain)


def wss_url(domain: str = CONSTANTS.DOMAIN) -> str:
    return CONSTANTS.WS_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_WS_URL


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    domain: str = CONSTANTS.DOMAIN,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (
        lambda: get_current_server_time(throttler=throttler, domain=domain)
    )
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(
                synchronizer=time_synchronizer,
                time_provider=time_provider,
            ),
            ZtdxPerpetualRESTPreProcessor(),
        ],
    )


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler,
) -> WebAssistantsFactory:
    return WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[ZtdxPerpetualRESTPreProcessor()],
    )


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DOMAIN,
) -> float:
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    return response["serverTime"]


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    contract_type = rule.get("contractType", "PERPETUAL")
    status = rule.get("status", "TRADING")
    return contract_type == "PERPETUAL" and status == "TRADING"
