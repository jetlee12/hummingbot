from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
    buy_percent_fee_deducted_from_returns=True,
)

CENTRALIZED = False
EXAMPLE_PAIR = "BTC-USDT"


class ZtdxPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "ztdx_perpetual"
    ztdx_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your ZTDX Perpetual API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    ztdx_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your ZTDX Perpetual API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


KEYS = ZtdxPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["ztdx_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"ztdx_perpetual_testnet": "ztdx_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"ztdx_perpetual_testnet": EXAMPLE_PAIR}
OTHER_DOMAINS_DEFAULT_FEES = {"ztdx_perpetual_testnet": [0, 0]}


class ZtdxPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "ztdx_perpetual_testnet"
    ztdx_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your ZTDX Perpetual testnet API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    ztdx_perpetual_testnet_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your ZTDX Perpetual testnet API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="ztdx_perpetual")


OTHER_DOMAINS_KEYS = {
    "ztdx_perpetual_testnet": ZtdxPerpetualTestnetConfigMap.model_construct()
}
