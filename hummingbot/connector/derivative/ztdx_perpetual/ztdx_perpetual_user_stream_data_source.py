import asyncio
import time
from typing import TYPE_CHECKING, Optional

from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_auth import ZtdxPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_derivative import ZtdxPerpetualDerivative


class ZtdxPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """Private ZTDX user stream using the documented listenKey lifecycle."""

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = CONSTANTS.LISTEN_KEY_KEEP_ALIVE_INTERVAL
    LISTEN_KEY_RETRY_INTERVAL = 5.0
    HEARTBEAT_TIME_INTERVAL = CONSTANTS.HEARTBEAT_TIME_INTERVAL
    AUTH_TIMEOUT = 10.0
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: ZtdxPerpetualAuth,
        connector: "ZtdxPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._current_listen_key: Optional[str] = None
        self._last_listen_key_ping_ts: Optional[float] = None
        self._manage_listen_key_task: Optional[asyncio.Task] = None
        self._listen_key_initialized_event = asyncio.Event()

    async def _get_listen_key(self) -> str:
        rest_assistant = await self._api_factory.get_rest_assistant()
        response = await rest_assistant.execute_request(
            url=web_utils.private_rest_url(CONSTANTS.USER_STREAM_ENDPOINT, self._domain),
            method=RESTMethod.POST,
            is_auth_required=True,
            throttler_limit_id=CONSTANTS.USER_STREAM_ENDPOINT,
        )
        return response["listenKey"]

    async def _ping_listen_key(self) -> bool:
        try:
            response = await self._connector._api_put(
                path_url=CONSTANTS.USER_STREAM_ENDPOINT,
                data={},
                is_auth_required=True,
                return_err=True,
            )
            return not (isinstance(response, dict) and "code" in response)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().warning("Failed to refresh ZTDX listenKey", exc_info=True)
            return False

    async def _manage_listen_key_task_loop(self):
        while True:
            try:
                now = time.time()
                if self._current_listen_key is None:
                    self._current_listen_key = await self._get_listen_key()
                    self._last_listen_key_ping_ts = now
                    self._listen_key_initialized_event.set()

                if now - self._last_listen_key_ping_ts >= self.LISTEN_KEY_KEEP_ALIVE_INTERVAL:
                    if not await self._ping_listen_key():
                        raise IOError("Unable to refresh ZTDX listenKey")
                    self._last_listen_key_ping_ts = now

                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)
            except asyncio.CancelledError:
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                raise
            except Exception:
                self.logger().error("Error managing ZTDX listenKey; requesting a fresh key.", exc_info=True)
                self._current_listen_key = None
                self._listen_key_initialized_event.clear()
                await self._sleep(self.LISTEN_KEY_RETRY_INTERVAL)

    async def _ensure_listen_key_task_running(self):
        if self._manage_listen_key_task is None or self._manage_listen_key_task.done():
            self._manage_listen_key_task = safe_ensure_future(self._manage_listen_key_task_loop())

    async def _connected_websocket_assistant(self) -> WSAssistant:
        await self._ensure_listen_key_task_running()
        await self._listen_key_initialized_event.wait()

        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.wss_url(self._domain),
            ping_timeout=self.HEARTBEAT_TIME_INTERVAL,
        )
        await ws.send(WSJSONRequest({"type": "auth", "listenKey": self._current_listen_key}))

        auth_response = await asyncio.wait_for(ws.receive(), timeout=self.AUTH_TIMEOUT)
        auth_data = auth_response.data if auth_response is not None else {}
        if auth_data.get("type") != "auth_result" or auth_data.get("success") is not True:
            raise IOError(f"ZTDX WebSocket authentication failed: {auth_data}")

        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        for channel in ("orders", "balances", "positions"):
            await websocket_assistant.send(WSJSONRequest({"type": "subscribe", "channel": channel}))
        self.logger().info("Subscribed to ZTDX private orders, balances and positions channels.")

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        if self._manage_listen_key_task is not None and not self._manage_listen_key_task.done():
            self._manage_listen_key_task.cancel()
            try:
                await self._manage_listen_key_task
            except asyncio.CancelledError:
                pass
            self._manage_listen_key_task = None

        if websocket_assistant is not None:
            await websocket_assistant.disconnect()

        self._current_listen_key = None
        self._listen_key_initialized_event.clear()
