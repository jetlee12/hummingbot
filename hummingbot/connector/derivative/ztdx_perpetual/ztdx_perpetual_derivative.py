import asyncio
import time
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.ztdx_perpetual import (
    ztdx_perpetual_constants as CONSTANTS,
    ztdx_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_api_order_book_data_source import (
    ZtdxPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_auth import ZtdxPerpetualAuth
from hummingbot.connector.derivative.ztdx_perpetual.ztdx_perpetual_user_stream_data_source import (
    ZtdxPerpetualUserStreamDataSource,
)
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class ZtdxPerpetualDerivative(PerpetualDerivativePyBase):
    """Hummingbot connector for ZTDX USDⓈ-M perpetual futures."""

    web_utils = web_utils
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    def __init__(
        self,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        ztdx_perpetual_api_key: str = None,
        ztdx_perpetual_api_secret: str = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = CONSTANTS.DOMAIN,
    ):
        self.ztdx_perpetual_api_key = ztdx_perpetual_api_key
        self.ztdx_perpetual_api_secret = ztdx_perpetual_api_secret
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        self._domain = domain
        self._position_mode = PositionMode.ONEWAY
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self) -> str:
        return self._domain

    @property
    def authenticator(self) -> ZtdxPerpetualAuth:
        return ZtdxPerpetualAuth(
            api_key=self.ztdx_perpetual_api_key,
            api_secret=self.ztdx_perpetual_api_secret,
            time_provider=self._time_synchronizer,
        )

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return 36

    @property
    def client_order_id_prefix(self) -> str:
        return "HBOT-ZTDX-"

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.EXCHANGE_INFO_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 600

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self):
        # ZTDX /fapi currently rejects hedge-mode=true with -4059.
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return self._trading_rules[trading_pair].sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        text = str(request_exception)
        return "-1021" in text or "timestamp" in text.lower()

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ZtdxPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ZtdxPerpetualUserStreamDataSource(
            auth=self._auth,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        position_action: PositionAction,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        return build_trade_fee(
            self.name,
            bool(is_maker),
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )

    async def _update_trading_fees(self):
        # Commission endpoint is available; dynamic fee-schema integration can be added after
        # the base connector is exercised on testnet.
        pass

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": "BUY" if trade_type is TradeType.BUY else "SELL",
            "quantity": f"{amount:f}",
            "type": "MARKET" if order_type is OrderType.MARKET else "LIMIT",
            "newClientOrderId": order_id,
        }
        if order_type.is_limit_type():
            params["price"] = f"{price:f}"
            params["timeInForce"] = (
                CONSTANTS.TIME_IN_FORCE_GTX if order_type is OrderType.LIMIT_MAKER else CONSTANTS.TIME_IN_FORCE_GTC
            )
        if position_action == PositionAction.CLOSE:
            params["reduceOnly"] = True

        try:
            result = await self._api_post(
                path_url=CONSTANTS.ORDER_URL,
                data=params,
                is_auth_required=True,
                limit_id=CONSTANTS.POST_ORDER_LIMIT_ID,
            )
            exchange_order_id = str(result["orderId"])
            update_time = result.get("updateTime", result.get("transactTime", int(time.time() * 1e3)))
            return exchange_order_id, float(update_time) * 1e-3
        except IOError as exc:
            if "503" in str(exc):
                return "UNKNOWN", time.time()
            raise

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        result = await self._api_delete(
            path_url=CONSTANTS.ORDER_URL,
            params={"symbol": symbol, "origClientOrderId": order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.DELETE_ORDER_LIMIT_ID,
        )
        return result.get("status") == "CANCELED"

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        result = await self._api_get(
            path_url=CONSTANTS.ORDER_URL,
            params={"symbol": symbol, "origClientOrderId": tracked_order.client_order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.GET_ORDER_LIMIT_ID,
        )
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=float(result.get("updateTime", int(time.time() * 1e3))) * 1e-3,
            new_state=CONSTANTS.ORDER_STATE[result["status"]],
            client_order_id=result["clientOrderId"],
            exchange_order_id=str(result["orderId"]),
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        exchange_order_id = await order.get_exchange_order_id()
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        trades = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_TRADE_LIST_URL,
            params={"symbol": symbol, "orderId": exchange_order_id},
            is_auth_required=True,
        )
        updates: List[TradeUpdate] = []
        for trade in trades:
            if str(trade.get("orderId")) != exchange_order_id:
                continue
            commission_asset = trade.get("commissionAsset", order.quote_asset)
            commission = Decimal(str(trade.get("commission", "0")))
            flat_fees = [] if commission == 0 else [TokenAmount(amount=commission, token=commission_asset)]
            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=order.position,
                percent_token=commission_asset,
                flat_fees=flat_fees,
            )
            price = Decimal(str(trade["price"]))
            qty = Decimal(str(trade["qty"]))
            updates.append(
                TradeUpdate(
                    trade_id=str(trade.get("id", trade.get("tradeId"))),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(trade["orderId"]),
                    trading_pair=order.trading_pair,
                    fill_timestamp=float(trade.get("time", int(time.time() * 1e3))) * 1e-3,
                    fill_price=price,
                    fill_base_amount=qty,
                    fill_quote_amount=Decimal(str(trade.get("quoteQty", price * qty))),
                    fee=fee,
                )
            )
        return updates

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        rules: List[TradingRule] = []
        for rule in exchange_info_dict.get("symbols", []):
            try:
                if not web_utils.is_exchange_information_valid(rule):
                    continue
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(rule["symbol"])
                filters = {f["filterType"]: f for f in rule.get("filters", [])}
                lot = filters["LOT_SIZE"]
                price_filter = filters["PRICE_FILTER"]
                min_notional_filter = filters.get("MIN_NOTIONAL", {})
                collateral = rule.get("marginAsset", rule.get("quoteAsset"))
                rules.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=Decimal(str(lot["minQty"])),
                        min_price_increment=Decimal(str(price_filter["tickSize"])),
                        min_base_amount_increment=Decimal(str(lot["stepSize"])),
                        min_notional_size=Decimal(str(min_notional_filter.get("notional", "0"))),
                        buy_order_collateral_token=collateral,
                        sell_order_collateral_token=collateral,
                    )
                )
            except Exception:
                self.logger().error(f"Error parsing ZTDX trading rule: {rule}", exc_info=True)
        return rules

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for item in filter(web_utils.is_exchange_information_valid, exchange_info.get("symbols", [])):
            exchange_symbol = item.get("symbol", item.get("pair"))
            base = item["baseAsset"]
            quote = item["quoteAsset"]
            hb_pair = combine_to_hb_trading_pair(base, quote)
            if hb_pair not in mapping.inverse:
                mapping[exchange_symbol] = hb_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        result = await self._api_get(
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_URL,
            params={"symbol": symbol},
        )
        return float(result["lastPrice"])

    async def _update_balances(self):
        result = await self._api_get(path_url=CONSTANTS.ACCOUNT_BALANCE_URL, is_auth_required=True)
        local_assets = set(self._account_balances)
        remote_assets = set()
        for row in result:
            asset = row["asset"]
            self._account_balances[asset] = Decimal(str(row["balance"]))
            self._account_available_balances[asset] = Decimal(str(row["availableBalance"]))
            remote_assets.add(asset)
        for asset in local_assets - remote_assets:
            self._account_balances.pop(asset, None)
            self._account_available_balances.pop(asset, None)

    async def _update_positions(self):
        positions = await self._api_get(path_url=CONSTANTS.POSITION_INFORMATION_URL, is_auth_required=True)
        for row in positions:
            symbol = row["symbol"]
            try:
                hb_pair = await self.trading_pair_associated_to_exchange_symbol(symbol)
            except KeyError:
                continue
            amount = Decimal(str(row.get("positionAmt", "0")))
            side = PositionSide.LONG if amount > 0 else PositionSide.SHORT if amount < 0 else PositionSide.BOTH
            pos_key = self._perpetual_trading.position_key(hb_pair, side)
            if amount == 0:
                self._perpetual_trading.remove_position(pos_key)
                continue
            position = Position(
                trading_pair=hb_pair,
                position_side=side,
                unrealized_pnl=Decimal(str(row.get("unRealizedProfit", "0"))),
                entry_price=Decimal(str(row.get("entryPrice", "0"))),
                amount=amount,
                leverage=Decimal(str(row.get("leverage", "1"))),
            )
            self._perpetual_trading.set_position(pos_key, position)

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        if mode is not PositionMode.ONEWAY:
            return False, "ZTDX currently supports one-way position mode only."
        return True, ""

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        result = await self._api_post(
            path_url=CONSTANTS.SET_LEVERAGE_URL,
            data={"symbol": symbol, "leverage": leverage},
            is_auth_required=True,
        )
        if int(result.get("leverage", -1)) == leverage:
            return True, ""
        return False, str(result)

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        # ZTDX funding-income endpoint will be wired once its canonical fapi path/payload is
        # confirmed. Funding rate itself is already available through premiumIndex.
        return 0, Decimal("-1"), Decimal("-1")

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, Any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Error reading ZTDX user stream.", exc_info=True)
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            # Private WS transport/auth/subscription is implemented. Exact futures push
            # payload mapping is kept isolated until ZTDX confirms the field schema.
            event_type = event_message.get("type")
            if event_type in {"auth_result", "subscribed", "pong"}:
                continue
            self.logger().debug(f"Received ZTDX private event: {event_type}")
