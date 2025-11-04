"""
Strategy registry and base classes for assignment strategies.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional, Any


class BaseStrategy(ABC):
    """
    Abstract base class for all assignment strategies.

    Provides common interface for both batch and real-time modes.
    """

    def __init__(self, cost_function):
        """
        Initialize strategy with a cost function.

        Args:
            cost_function: Instance of BaseCostFunction
        """
        self.cost_function = cost_function

    @abstractmethod
    def get_name(self) -> str:
        """Get strategy name for logging."""
        pass

    @abstractmethod
    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make batch assignments.

        Args:
            waiting_orders: Orders or bundles to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidate pairs
            bundle_mapping: Optional bundle to orders mapping

        Returns:
            List of (order/bundle, courier, cost) tuples
        """
        pass

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order (for real-time mode).

        Default implementation uses make_assignments with single order.
        Can be overridden for efficiency.

        Args:
            order: Single order to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            (courier, cost) tuple if successful, None otherwise
        """
        if not available_couriers:
            return None

        # Use batch method with single order
        assignments = self.make_assignments(
            [order],
            available_couriers,
            waybill_lookup
        )

        if assignments:
            _, courier, cost = assignments[0]
            return (courier, cost)

        return None


class StrategyRegistry:
    """
    Registry for assignment strategies.

    Maps strategy keys to strategy classes.
    """

    _strategies = {}

    @classmethod
    def register(cls, key: str, strategy_class: type):
        """
        Register a strategy class.

        Args:
            key: Strategy identifier
            strategy_class: Class implementing BaseStrategy
        """
        cls._strategies[key] = strategy_class

    @classmethod
    def get(cls, key: str, cost_function) -> BaseStrategy:
        """
        Get an instance of a strategy.

        Args:
            key: Strategy identifier
            cost_function: Cost function to inject

        Returns:
            Strategy instance

        Raises:
            ValueError: If strategy not found
        """
        if key not in cls._strategies:
            available = list(cls._strategies.keys())
            raise ValueError(f"Strategy '{key}' not found. Available: {available}")

        strategy_class = cls._strategies[key]
        return strategy_class(cost_function)

    @classmethod
    def list_strategies(cls) -> List[str]:
        """Get list of available strategy keys."""
        return list(cls._strategies.keys())


# Import and register concrete strategies
from .hungarian import HungarianStrategy
from .greedy import GreedyStrategy
from .batch_greedy import BatchGreedyStrategy

# Register strategies
StrategyRegistry.register('hungarian', HungarianStrategy)
StrategyRegistry.register('greedy', GreedyStrategy)
StrategyRegistry.register('batch_greedy', BatchGreedyStrategy)

# MILP strategy (requires OR-Tools: pip3 install --break-system-packages ortools)
from .milp_vrp import MILPStrategy
StrategyRegistry.register('milp', MILPStrategy)

# Clustered MILP strategy (spatial partitioning + MILP)
from .clustered_milp_vrp import ClusteredMILPStrategy
StrategyRegistry.register('clustered_milp', ClusteredMILPStrategy)