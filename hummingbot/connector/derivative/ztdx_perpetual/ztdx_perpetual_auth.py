import hashlib
import hmac
from collections import OrderedDict
from typing import Any, Dict
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class ZtdxPerpetualAuth(AuthBase):
    """HMAC-SHA256 authentication for ZTDX /fapi programmatic endpoints."""

    def __init__(self, api_key: str, api_secret: str, time_provider: TimeSynchronizer):
        self._api_key = api_key
        self._api_secret = api_secret
        self._time_provider = time_provider

    def generate_signature_from_payload(self, payload: str) -> str:
        return hmac.new(
            self._api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        query_params = OrderedDict(request.params or {})
        query_params["timestamp"] = int(self._time_provider.time() * 1e3)
        query_string = urlencode(query_params)

        if request.method in (RESTMethod.POST, RESTMethod.PUT):
            body = request.data or ""
            if not isinstance(body, str):
                raise TypeError("ZTDX signed POST/PUT requests require the JSON body to be serialized before auth")
            payload = f"{query_string}{body}"
        else:
            payload = query_string

        query_params["signature"] = self.generate_signature_from_payload(payload)
        request.params = query_params
        request.headers = {**(request.headers or {}), **self.header_for_authentication()}
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        request_params = OrderedDict(params or {})
        request_params["timestamp"] = int(self._time_provider.time() * 1e3)
        payload = urlencode(request_params)
        request_params["signature"] = self.generate_signature_from_payload(payload)
        return request_params

    def header_for_authentication(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self._api_key}
