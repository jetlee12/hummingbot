import unittest

from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_derivative import ZtdxPerpetualDerivative
from hummingbot.core.data_type.common import OrderType, PositionMode


class ZtdxPerpetualDerivativeUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connector = ZtdxPerpetualDerivative(
            ztdx_perpetual_api_key="test_key",
            ztdx_perpetual_api_secret="test_secret",
            trading_pairs=["BTC-USDT"],
            trading_required=False,
        )

    def test_name_and_domain(self):
        self.assertEqual(CONSTANTS.DOMAIN, self.connector.name)
        self.assertEqual(CONSTANTS.DOMAIN, self.connector.domain)

    def test_supported_order_types(self):
        self.assertEqual(
            [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER],
            self.connector.supported_order_types(),
        )

    def test_only_oneway_position_mode_is_supported(self):
        self.assertEqual([PositionMode.ONEWAY], self.connector.supported_position_modes())

    def test_position_mode_rejects_hedge(self):
        success, message = self.async_run(self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, "BTC-USDT"))
        self.assertFalse(success)
        self.assertIn("one-way", message.lower())

    def test_position_mode_accepts_oneway(self):
        success, message = self.async_run(self.connector._trading_pair_position_mode_set(PositionMode.ONEWAY, "BTC-USDT"))
        self.assertTrue(success)
        self.assertEqual("", message)

    @staticmethod
    def async_run(coro):
        import asyncio
        return asyncio.get_event_loop().run_until_complete(coro)
