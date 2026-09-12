from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "ztdx_perpetual"
DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "ztdx_perpetual_testnet"

REST_URL = "https://api.ztdx.io/fapi/"
TESTNET_REST_URL = "https://api-sepolia.ztdx.io/fapi/"
# Futures public WebSocket endpoint confirmed by ZTDX: full Top-30 order-book snapshots.
WS_URL = "wss://api.ztdx.com/fapi/ws"
# ZTDX has not provided a separate testnet WebSocket endpoint in the answer sheet;
# retain the previously configured testnet host until they confirm its exact path.
TESTNET_WS_URL = "wss://api-sepolia.ztdx.io/ws"

TIME_IN_FORCE_GTC = "GTC"
TIME_IN_FORCE_GTX = "GTX"
TIME_IN_FORCE_IOC = "IOC"
TIME_IN_FORCE_FOK = "FOK"

# Public REST endpoints
SNAPSHOT_REST_URL = "v1/depth"
EXCHANGE_INFO_URL = "v1/exchangeInfo"
PING_URL = "v1/ping"
SERVER_TIME_PATH_URL = "v1/time"
MARK_PRICE_URL = "v1/premiumIndex"
TICKER_PRICE_CHANGE_URL = "v1/ticker/24hr"

# Private REST endpoints
ORDER_URL = "v1/order"
OPEN_ORDERS_URL = "v1/openOrders"
CANCEL_ALL_OPEN_ORDERS_URL = "v1/allOpenOrders"
ACCOUNT_TRADE_LIST_URL = "v1/userTrades"
SET_LEVERAGE_URL = "v1/leverage"
POSITION_INFORMATION_URL = "v1/positionRisk"
ACCOUNT_BALANCE_URL = "v2/balance"
POSITION_MODE_URL = "v1/positionSide/dual"
USER_STREAM_ENDPOINT = "v1/listenKey"
COMMISSION_RATE_URL = "v1/commissionRate"

GET_ORDER_LIMIT_ID = f"GET{ORDER_URL}"
POST_ORDER_LIMIT_ID = f"POST{ORDER_URL}"
DELETE_ORDER_LIMIT_ID = f"DELETE{ORDER_URL}"
GET_POSITION_MODE_LIMIT_ID = f"GET{POSITION_MODE_URL}"
POST_POSITION_MODE_LIMIT_ID = f"POST{POSITION_MODE_URL}"

ORDER_STATE = {
    "NEW": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "EXPIRED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
}

# ZTDX currently documents nginx traffic shaping rather than stable endpoint weights.
# These conservative local limits protect the connector until production MM limits
# are formally published by ZTDX.
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ONE_MINUTE = 60
MAX_REQUESTS_PER_MINUTE = 600
MAX_ORDERS_PER_MINUTE = 300

RATE_LIMITS = [
    RateLimit(limit_id=REQUEST_WEIGHT, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=MAX_ORDERS_PER_MINUTE, time_interval=ONE_MINUTE),
    RateLimit(limit_id=SNAPSHOT_REST_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=PING_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=MARK_PRICE_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_ORDER_LIMIT_ID, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=POST_ORDER_LIMIT_ID, limit=MAX_ORDERS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1), LinkedLimitWeightPair(ORDERS, weight=1)]),
    RateLimit(limit_id=DELETE_ORDER_LIMIT_ID, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=OPEN_ORDERS_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=CANCEL_ALL_OPEN_ORDERS_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ACCOUNT_TRADE_LIST_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=SET_LEVERAGE_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=POSITION_INFORMATION_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=ACCOUNT_BALANCE_URL, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=GET_POSITION_MODE_LIMIT_ID, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=POST_POSITION_MODE_LIMIT_ID, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
    RateLimit(limit_id=USER_STREAM_ENDPOINT, limit=MAX_REQUESTS_PER_MINUTE, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, weight=1)]),
]

HEARTBEAT_TIME_INTERVAL = 30.0
LISTEN_KEY_KEEP_ALIVE_INTERVAL = 30 * 60

ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
