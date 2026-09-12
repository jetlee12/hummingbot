import asyncio
import copy
import hashlib
import hmac
import json
import unittest
from typing import Awaitable
from urllib.parse import urlencode

from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_auth import ZtdxPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSJSONRequest


class ZtdxPerpetualAuthUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()
        cls.api_key = "TEST_API_KEY"
        cls.secret_key = "TEST_SECRET_KEY"

    def setUp(self) -> None:
        super().setUp()
        self.emulated_time = 1640001112.223
        self.timestamp = int(self.emulated_time * 1e3)
        self.auth = ZtdxPerpetualAuth(
            api_key=self.api_key,
            api_secret=self.secret_key,
            time_provider=self,
        )

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        return self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))

    def time(self):
        return self.emulated_time

    def _signature(self, payload: str):
        return hmac.new(
            self.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def test_generate_signature_from_payload(self):
        payload = f"symbol=BTCUSDT&timestamp={self.timestamp}"
        self.assertEqual(self.auth.generate_signature_from_payload(payload), self._signature(payload))

    def test_rest_authenticate_get(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="/TEST_PATH_URL",
            params={"symbol": "BTCUSDT"},
            is_auth_required=True,
        )
        signed_request = self.async_run_with_timeout(self.auth.rest_authenticate(request))
        payload = urlencode({"symbol": "BTCUSDT", "timestamp": self.timestamp})

        self.assertEqual(signed_request.headers["X-MBX-APIKEY"], self.api_key)
        self.assertEqual(signed_request.params["timestamp"], self.timestamp)
        self.assertEqual(signed_request.params["signature"], self._signature(payload))

    def test_rest_authenticate_post_signs_query_plus_raw_json_body(self):
        body = json.dumps({"symbol": "BTCUSDT", "leverage": 20}, separators=(",", ":"))
        request = RESTRequest(
            method=RESTMethod.POST,
            url="/TEST_PATH_URL",
            data=body,
            is_auth_required=True,
        )
        signed_request = self.async_run_with_timeout(self.auth.rest_authenticate(request))
        query = urlencode({"timestamp": self.timestamp})
        expected_signature = self._signature(f"{query}{body}")

        self.assertEqual(signed_request.headers["X-MBX-APIKEY"], self.api_key)
        self.assertEqual(signed_request.params["timestamp"], self.timestamp)
        self.assertEqual(signed_request.params["signature"], expected_signature)
        self.assertEqual(signed_request.data, body)

    def test_ws_authenticate_is_passthrough(self):
        request = WSJSONRequest(payload={"type": "ping"}, is_auth_required=True)
        signed_request = self.async_run_with_timeout(self.auth.ws_authenticate(request))
        self.assertEqual(request, signed_request)
