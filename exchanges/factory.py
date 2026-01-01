"""
Simple ExchangeFactory to construct exchange client instances.
"""
from typing import Dict, Any

from .edgex import EdgeXClient
from .lighter import LighterClient
from .grvt import GrvtClient


class ExchangeFactory:
    """Factory for creating exchange client instances by name."""

    @staticmethod
    def create(name: str, config: Dict[str, Any]):
        """Create and return an exchange client instance.

        Args:
            name: exchange name (case-insensitive), e.g. 'edgex', 'lighter', 'grvt'
            config: configuration dictionary passed to the client constructor
        """
        if not name:
            raise ValueError("Exchange name must be provided")

        key = name.strip().lower()
        if key == 'edgex' or key == 'edgeX'.lower():
            return EdgeXClient(config)
        if key == 'lighter':
            return LighterClient(config)
        if key == 'grvt':
            return GrvtClient(config)

        raise ValueError(f"Unsupported exchange: {name}")


