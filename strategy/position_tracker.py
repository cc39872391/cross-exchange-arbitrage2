"""Position tracking for EdgeX and Lighter exchanges."""
import asyncio
import json
import logging
import requests
import sys
from decimal import Decimal


class PositionTracker:
    """Tracks positions on both exchanges."""

    def __init__(self, ticker: str, grvt_client, grvt_contract_id: str,
                 lighter_base_url: str, account_index: int, logger: logging.Logger):
        """Initialize position tracker."""
        self.ticker = ticker
        self.grvt_client = grvt_client
        self.grvt_contract_id = grvt_contract_id
        self.lighter_base_url = lighter_base_url
        self.account_index = account_index
        self.logger = logger

        self.grvt_position = Decimal('0')
        self.lighter_position = Decimal('0')

    async def get_grvt_position(self) -> Decimal:
        """Get GRVT position."""
        if not self.grvt_client:
            raise Exception("GRVT client not initialized")

        return await self.grvt_client.get_account_positions()

    async def get_lighter_position(self) -> Decimal:
        """Get Lighter position."""
        url = f"{self.lighter_base_url}/api/v1/account"
        headers = {"accept": "application/json"}

        current_position = None
        parameters = {"by": "index", "value": self.account_index}
        attempts = 0
        while current_position is None and attempts < 10:
            try:
                response = requests.get(url, headers=headers, params=parameters, timeout=10)
                response.raise_for_status()

                if not response.text.strip():
                    self.logger.warning("⚠️ Empty response from Lighter API for position check")
                    return self.lighter_position

                data = response.json()

                if 'accounts' not in data or not data['accounts']:
                    self.logger.warning(f"⚠️ Unexpected response format from Lighter API: {data}")
                    return self.lighter_position

                positions = data['accounts'][0].get('positions', [])
                for position in positions:
                    if position.get('symbol') == self.ticker:
                        current_position = Decimal(position['position']) * position['sign']
                        break
                if current_position is None:
                    current_position = 0

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"⚠️ Network error getting position: {e}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"⚠️ JSON parsing error in position response: {e}")
                self.logger.warning(f"Response text: {response.text[:200]}...")
            except Exception as e:
                self.logger.warning(f"⚠️ Unexpected error getting position: {e}")
            finally:
                attempts += 1
                await asyncio.sleep(1)

        if current_position is None:
            self.logger.error(f"❌ Failed to get Lighter position after {attempts} attempts")
            sys.exit(1)

        return current_position

    def update_grvt_position(self, delta: Decimal):
        """Update GRVT position by delta."""
        self.grvt_position += delta

    def update_lighter_position(self, delta: Decimal):
        """Update Lighter position by delta."""
        self.lighter_position += delta

    def get_current_grvt_position(self) -> Decimal:
        """Get current GRVT position (cached)."""
        return self.grvt_position

    def get_current_lighter_position(self) -> Decimal:
        """Get current Lighter position (cached)."""
        return self.lighter_position

    def get_net_position(self) -> Decimal:
        """Get net position across both exchanges."""
        return self.grvt_position + self.lighter_position
