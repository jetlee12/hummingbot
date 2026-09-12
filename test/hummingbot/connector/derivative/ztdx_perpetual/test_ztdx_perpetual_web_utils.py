import unittest

from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.ztdx_perpetual import ztdx_perpetual_web_utils as web_utils


class ZtdxPerpetualWebUtilsUnitTests(unittest.TestCase):
    def test_mainnet_rest_url(self):
        self.assertEqual(
            "https://api.ztdx.io/fapi/v1/ping",
            web_utils.public_rest_url(CONSTANTS.PING_URL, CONSTANTS.DOMAIN),
        )

    def test_testnet_rest_url(self):
        self.assertEqual(
            "https://api-sepolia.ztdx.io/fapi/v1/ping",
            web_utils.public_rest_url(CONSTANTS.PING_URL, CONSTANTS.TESTNET_DOMAIN),
        )

    def test_mainnet_websocket_url(self):
        self.assertEqual("wss://api.ztdx.com/fapi/ws", web_utils.wss_url(CONSTANTS.DOMAIN))

    def test_testnet_websocket_url(self):
        self.assertEqual("wss://api-sepolia.ztdx.io/ws", web_utils.wss_url(CONSTANTS.TESTNET_DOMAIN))
