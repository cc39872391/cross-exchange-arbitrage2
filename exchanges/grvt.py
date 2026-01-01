"""
GRVT exchange client implementation.
Based on common REST and WebSocket API patterns for crypto exchanges.
Note: This is a template implementation. You may need to adjust based on GRVT's actual API documentation.
"""

import os
import asyncio
import json
import hashlib
import hmac
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
import aiohttp
import websockets

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger


class GrvtClient(BaseExchangeClient):
    """GRVT exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize GRVT client."""
        super().__init__(config)

        # GRVT credentials from environment
        self.api_key = os.getenv('GRVT_API_KEY')
        self.api_secret = os.getenv('GRVT_API_SECRET')
        self.base_url = os.getenv('GRVT_BASE_URL', 'https://api.grvt.io')
        self.ws_url = os.getenv('GRVT_WS_URL', 'wss://ws.grvt.io')

        if not self.api_key or not self.api_secret:
            raise ValueError("GRVT_API_KEY and GRVT_API_SECRET must be set in environment variables")

        # Initialize HTTP session
        self.session = None
        self.ws = None

        # Initialize logger
        self.logger = TradingLogger(exchange="grvt", ticker=self.config.ticker, log_to_console=False)

        self._order_update_handler = None

        # Market configuration (will be set during initialization)
        self.contract_id = None
        self.tick_size = None
        self.min_order_size = None

        # --- reconnection state ---
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop = asyncio.Event()
        self._ws_disconnected = asyncio.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _validate_config(self) -> None:
        """Validate GRVT configuration."""
        required_env_vars = ['GRVT_API_KEY', 'GRVT_API_SECRET']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    def _generate_signature(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """Generate HMAC signature for GRVT API authentication."""
        message = f"{timestamp}{method.upper()}{path}{body}"
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    async def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make authenticated request to GRVT API."""
        url = f"{self.base_url}{endpoint}"
        timestamp = str(int(asyncio.get_event_loop().time() * 1000))

        body = json.dumps(data) if data else ""
        signature = self._generate_signature(timestamp, method, endpoint, body)

        headers = {
            'GRVT-KEY': self.api_key,
            'GRVT-SIGN': signature,
            'GRVT-TIMESTAMP': timestamp,
            'Content-Type': 'application/json'
        }

        async with self.session.request(method, url, headers=headers, data=body) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_text = await response.text()
                raise Exception(f"GRVT API error {response.status}: {error_text}")

    # ---------------------------
    # Connection / Reconnect
    # ---------------------------

    async def connect(self) -> None:
        """Connect to GRVT API and WebSocket."""
        self._loop = asyncio.get_running_loop()

        # Create HTTP session
        self.session = aiohttp.ClientSession()

        # Start WebSocket connection
        if not self._ws_task or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_ws())

        await asyncio.sleep(0.5)

    async def disconnect(self) -> None:
        """Disconnect from GRVT."""
        self._ws_stop.set()

        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self.ws:
            await self.ws.close()

        if self.session:
            await self.session.close()

    async def _run_ws(self):
        """WebSocket connection and reconnection loop."""
        backoff = 1.0
        while not self._ws_stop.is_set():
            try:
                # Connect to WebSocket
                async with websockets.connect(self.ws_url) as websocket:
                    self.ws = websocket
                    self.logger.log("[WS] connected", "INFO")
                    backoff = 1.0

                    # Authenticate WebSocket
                    await self._authenticate_ws()

                    # Subscribe to private channels
                    await self._subscribe_private_channels()

                    # Listen for messages
                    async for message in websocket:
                        try:
                            await self._handle_ws_message(message)
                        except Exception as e:
                            self.logger.log(f"Error handling WS message: {e}", "ERROR")

            except websockets.exceptions.ConnectionClosed:
                if not self._ws_stop.is_set():
                    self.logger.log(f"[WS] disconnected, reconnecting in {backoff}s", "WARNING")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)  # Exponential backoff, max 30s
            except Exception as e:
                if not self._ws_stop.is_set():
                    self.logger.log(f"[WS] error: {e}, reconnecting in {backoff}s", "ERROR")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)

    async def _authenticate_ws(self):
        """Authenticate WebSocket connection."""
        timestamp = str(int(asyncio.get_event_loop().time() * 1000))
        signature = self._generate_signature(timestamp, "GET", "/ws")

        auth_msg = {
            "type": "auth",
            "key": self.api_key,
            "signature": signature,
            "timestamp": timestamp
        }

        await self.ws.send(json.dumps(auth_msg))

    async def _subscribe_private_channels(self):
        """Subscribe to private WebSocket channels."""
        subscribe_msg = {
            "type": "subscribe",
            "channels": ["orders", "positions"]
        }

        await self.ws.send(json.dumps(subscribe_msg))

    async def _handle_ws_message(self, message: str):
        """Handle WebSocket message."""
        try:
            data = json.loads(message)

            if data.get('type') == 'order':
                if self._order_update_handler:
                    await self._order_update_handler(data)
            elif data.get('type') == 'position':
                # Handle position updates if needed
                pass

        except Exception as e:
            self.logger.log(f"Error parsing WS message: {e}", "ERROR")

    # ---------------------------
    # Trading Operations
    # ---------------------------

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place a post-only limit order."""
        try:
            # Get current market price for limit order
            market_data = await self._make_request("GET", f"/v1/market/{contract_id}")
            current_price = Decimal(str(market_data.get('price', '0')))

            if current_price == 0:
                raise Exception("Unable to get market price")

            # Set limit price slightly better than market for post-only
            if direction == 'buy':
                limit_price = current_price * Decimal('0.9999')  # Slightly below ask
            else:
                limit_price = current_price * Decimal('1.0001')  # Slightly above bid

            limit_price = self.round_to_tick(limit_price)

            order_data = {
                "contract_id": contract_id,
                "side": direction.upper(),
                "quantity": str(quantity),
                "price": str(limit_price),
                "type": "LIMIT",
                "post_only": True
            }

            response = await self._make_request("POST", "/v1/orders", order_data)

            return OrderResult(
                success=True,
                order_id=str(response.get('order_id', '')),
                side=direction,
                size=quantity,
                price=limit_price,
                status='OPEN'
            )

        except Exception as e:
            self.logger.log(f"Error placing open order: {e}", "ERROR")
            return OrderResult(
                success=False,
                error_message=str(e)
            )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a market order to close position."""
        try:
            order_data = {
                "contract_id": contract_id,
                "side": side.upper(),
                "quantity": str(quantity),
                "type": "MARKET"
            }

            response = await self._make_request("POST", "/v1/orders", order_data)

            return OrderResult(
                success=True,
                order_id=str(response.get('order_id', '')),
                side=side,
                size=quantity,
                status='FILLED'  # Market orders fill immediately
            )

        except Exception as e:
            self.logger.log(f"Error placing close order: {e}", "ERROR")
            return OrderResult(
                success=False,
                error_message=str(e)
            )

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order."""
        try:
            await self._make_request("DELETE", f"/v1/orders/{order_id}")

            return OrderResult(
                success=True,
                order_id=order_id,
                status='CANCELLED'
            )

        except Exception as e:
            self.logger.log(f"Error cancelling order {order_id}: {e}", "ERROR")
            return OrderResult(
                success=False,
                order_id=order_id,
                error_message=str(e)
            )

    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information."""
        try:
            response = await self._make_request("GET", f"/v1/orders/{order_id}")

            return OrderInfo(
                order_id=str(response.get('order_id', '')),
                side=response.get('side', '').lower(),
                size=Decimal(str(response.get('quantity', '0'))),
                price=Decimal(str(response.get('price', '0'))),
                status=response.get('status', ''),
                filled_size=Decimal(str(response.get('filled_quantity', '0'))),
                remaining_size=Decimal(str(response.get('remaining_quantity', '0')))
            )

        except Exception as e:
            self.logger.log(f"Error getting order info {order_id}: {e}", "ERROR")
            return None

    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract."""
        try:
            response = await self._make_request("GET", "/v1/orders", {"contract_id": contract_id, "status": "active"})

            orders = []
            for order_data in response.get('orders', []):
                orders.append(OrderInfo(
                    order_id=str(order_data.get('order_id', '')),
                    side=order_data.get('side', '').lower(),
                    size=Decimal(str(order_data.get('quantity', '0'))),
                    price=Decimal(str(order_data.get('price', '0'))),
                    status=order_data.get('status', ''),
                    filled_size=Decimal(str(order_data.get('filled_quantity', '0'))),
                    remaining_size=Decimal(str(order_data.get('remaining_quantity', '0')))
                ))

            return orders

        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            return []

    async def get_account_positions(self) -> Decimal:
        """Get account positions."""
        try:
            response = await self._make_request("GET", "/v1/positions")

            for position in response.get('positions', []):
                if str(position.get('contract_id', '')) == self.contract_id:
                    return Decimal(str(position.get('size', '0')))

            return Decimal('0')

        except Exception as e:
            self.logger.log(f"Error getting positions: {e}", "ERROR")
            return Decimal('0')

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "grvt"

    async def get_market_data(self, contract_id: str) -> Dict:
        """Get market data for a contract."""
        return await self._make_request("GET", f"/v1/market/{contract_id}")

    async def get_contract_info(self, symbol: str) -> Tuple[str, Decimal]:
        """Get contract information."""
        try:
            response = await self._make_request("GET", "/v1/contracts")

            for contract in response.get('contracts', []):
                if contract.get('symbol', '') == symbol + 'USD':
                    contract_id = str(contract.get('contract_id', ''))
                    tick_size = Decimal(str(contract.get('tick_size', '0.1')))
                    return contract_id, tick_size

            raise Exception(f"Contract {symbol} not found")

        except Exception as e:
            self.logger.log(f"Error getting contract info: {e}", "ERROR")
            raise
