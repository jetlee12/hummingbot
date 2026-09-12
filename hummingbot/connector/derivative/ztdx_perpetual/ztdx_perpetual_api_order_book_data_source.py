import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_web_utils as web_utils
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_derivative import ZtdxPerpetualDerivative


class ZtdxPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    ZTDX public market-data source.

    ZTDX Futures publishes a complete Top-30 book snapshot after subscription and then
    another complete Top-30 snapshot every 500 ms. It does not publish WebSocket
    sequence IDs or deltas, so each WebSocket message is routed through Hummingbot's
    snapshot queue and replaces the local book wholesale.
    """

    def __init__(
        self,
        trading_pairs: List[str],
        connector: "ZtdxPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        data = await self._request_complete_funding_info(trading_pair)
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(data["indexPrice"])),
            mark_price=Decimal(str(data["markPrice"])),
            next_funding_utc_timestamp=int(float(data["nextFundingTime"]) * 1e-3),
            rate=Decimal(str(data.get("lastFundingRate", data.get("fundingRate", "0")))),
        )

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return await self._connector._api_get(
            path_url=CONSTANTS.SNAPSHOT_REST_URL,
            params={"symbol": symbol, "limit": 30},
        )

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        data = await self._request_order_book_snapshot(trading_pair)
        update_id = data.get("lastUpdateId", data.get("updateId", int(time.time() * 1e3)))
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
            },
            timestamp=time.time(),
        )

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=web_utils.wss_url(self._domain), ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to one full-snapshot order-book channel per configured symbol."""
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await ws.send(WSJSONRequest(payload={
                "type": "subscribe",
                "channel": f"orderbook:{symbol}",
            }))
        self.logger().info("Subscribed to ZTDX full order-book snapshot channels.")

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket connection not established")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await self._ws_assistant.send(WSJSONRequest(payload={
                "type": "subscribe",
                "channel": f"orderbook:{symbol}",
            }))
            self.add_trading_pair(trading_pair)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error subscribing to ZTDX order book for {trading_pair}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket connection not established")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await self._ws_assistant.send(WSJSONRequest(payload={
                "type": "unsubscribe",
                "channel": f"orderbook:{symbol}",
            }))
            self.remove_trading_pair(trading_pair)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Error unsubscribing from ZTDX order book for {trading_pair}")
            return False

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        # Subscription confirmations and other control messages are intentionally ignored.
        if event_message.get("type") != "orderbook":
            return ""
        data = event_message.get("data", event_message)
        if not isinstance(data, dict) or not data.get("symbol"):
            return ""
        return self._snapshot_messages_queue_key

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # ZTDX has no delta feed. Full updates are routed to the snapshot queue instead.
        return

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data", raw_message)
        if not isinstance(data, dict) or data.get("type") == "subscribed":
            return

        symbol = data.get("symbol")
        if symbol is None:
            return
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        if trading_pair is None:
            return

        timestamp_ms = int(data.get("timestamp", time.time() * 1e3))
        # ZTDX does not provide a WS sequence/update ID. Hummingbot still requires an
        # integer update_id, so use the message timestamp as a synthetic identifier only.
        message_queue.put_nowait(
            OrderBookMessage(
                OrderBookMessageType.SNAPSHOT,
                {
                    "trading_pair": trading_pair,
                    "update_id": timestamp_ms,
                    "bids": [(level["price"], level["size"]) for level in data.get("bids", [])],
                    "asks": [(level["price"], level["size"]) for level in data.get("asks", [])],
                },
                timestamp=timestamp_ms * 1e-3,
            )
        )

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        # Public trade payload is not required for REST order/account functionality and
        # will be wired once ZTDX confirms the futures WS general schema.
        raise NotImplementedError("Pending confirmed ZTDX futures public trade WS schema")

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data = raw_message.get("data", raw_message)
        symbol = data.get("symbol", data.get("s"))
        if symbol is None:
            return
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
        message_queue.put_nowait(
            FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(data.get("indexPrice", data.get("i", "0")))),
                mark_price=Decimal(str(data.get("markPrice", data.get("p", "0")))),
                next_funding_utc_timestamp=int(float(data.get("nextFundingTime", data.get("T", 0))) * 1e-3),
                rate=Decimal(str(data.get("lastFundingRate", data.get("fundingRate", data.get("r", "0"))))),
            )
        )

    async def _request_complete_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        return await self._connector._api_get(
            path_url=CONSTANTS.MARK_PRICE_URL,
            params={"symbol": symbol},
        )
