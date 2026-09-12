import asyncio
import unittest
from unittest.mock import AsyncMock

from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_api_order_book_data_source import (
    ZtdxPerpetualAPIOrderBookDataSource,
)
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class _FakeConnector:
    async def exchange_symbol_associated_to_pair(self, trading_pair: str) -> str:
        return trading_pair.replace("-", "")

    async def trading_pair_associated_to_exchange_symbol(self, symbol: str) -> str:
        return symbol.replace("USDT", "-USDT")


class _TestSource(ZtdxPerpetualAPIOrderBookDataSource):
    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


class ZtdxPerpetualAPIOrderBookDataSourceUnitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.source = _TestSource(
            trading_pairs=["BTC-USDT"],
            connector=_FakeConnector(),
            api_factory=None,
        )

    async def test_subscribes_using_confirmed_full_snapshot_channel(self):
        ws = type("FakeWS", (), {"send": AsyncMock()})()

        await self.source._subscribe_channels(ws)

        ws.send.assert_awaited_once()
        request = ws.send.await_args.args[0]
        self.assertEqual(
            {"type": "subscribe", "channel": "orderbook:BTCUSDT"},
            request.payload,
        )

    def test_routes_only_orderbook_messages_to_snapshot_queue(self):
        self.assertEqual(
            self.source._snapshot_messages_queue_key,
            self.source._channel_originating_message({
                "type": "orderbook",
                "symbol": "BTCUSDT",
                "bids": [],
                "asks": [],
                "timestamp": 1711000000000,
            }),
        )
        self.assertEqual("", self.source._channel_originating_message({
            "type": "subscribed",
            "channel": "orderbook:BTCUSDT",
        }))

    async def test_parses_full_snapshot_and_uses_timestamp_as_synthetic_update_id(self):
        output = asyncio.Queue()
        raw_message = {
            "type": "orderbook",
            "symbol": "BTCUSDT",
            "bids": [{"price": "65430.00", "size": "1.2"}],
            "asks": [{"price": "65431.00", "size": "0.5"}],
            "timestamp": 1711000000000,
        }

        await self.source._parse_order_book_snapshot_message(raw_message, output)
        message = await output.get()

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual("BTC-USDT", message.trading_pair)
        self.assertEqual(1711000000000, message.update_id)
        self.assertEqual([("65430.00", "1.2")], message.content["bids"])
        self.assertEqual([("65431.00", "0.5")], message.content["asks"])
        self.assertEqual(1711000000.0, message.timestamp)

    async def test_ignores_subscription_confirmation(self):
        output = asyncio.Queue()

        await self.source._parse_order_book_snapshot_message(
            {"type": "subscribed", "channel": "orderbook:BTCUSDT"},
            output,
        )

        self.assertTrue(output.empty())

    def test_mainnet_ws_endpoint_matches_ztdx_answer(self):
        self.assertEqual("wss://api.ztdx.com/fapi/ws", CONSTANTS.WS_URL)
