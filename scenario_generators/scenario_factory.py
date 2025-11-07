
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Any
from .layout_generators import generate_restaurant_layout, generate_courier_layout
from .demand_generators import generate_demand

class ScenarioFactory:

    def __init__(self, config: Dict[str, Any]):

        self.config = config

    def create_scenario(self) -> Dict[str, Any]:

        # Generate geographic layout
        print(f"  Generating {self.config['restaurants']['count']} restaurants...")
        restaurants = generate_restaurant_layout(self.config)

        print(f"  Generating {self.config['couriers']['count']} couriers...")
        couriers = generate_courier_layout(self.config)

        # Generate demand
        total_orders = self.config['demand']['total_orders']
        profile = self.config['demand']['profile']
        print(f"  Generating {total_orders} orders (profile: {profile})...")
        order_schedule = generate_demand(self.config, restaurants)

        duration_seconds = self.config['scenario']['duration_hours'] * 3600

        print(f"  ✓ Scenario '{self.config['scenario']['name']}' generated successfully")
        print(f"    - Restaurants: {len(restaurants)}")
        print(f"    - Couriers: {len(couriers)}")
        print(f"    - Orders: {len(order_schedule)}")
        print(f"    - Duration: {self.config['scenario']['duration_hours']} hours")

        return {
            'restaurants': restaurants,
            'couriers': couriers,
            'order_schedule': order_schedule,
            'duration': duration_seconds,
            'config': self.config  # Include config for runtime access
        }

    def get_scenario_summary(self) -> str:

        lines = []
        lines.append(f"Scenario: {self.config['scenario']['name']}")
        lines.append(f"Description: {self.config['scenario'].get('description', 'N/A')}")
        lines.append(f"Duration: {self.config['scenario']['duration_hours']} hours")
        lines.append("")
        lines.append("Geography:")
        lines.append(f"  Map: {self.config['physics']['map_size_m']}m x {self.config['physics']['map_size_m']}m")
        lines.append(f"  Distance Metric: {self.config['physics']['distance_metric']}")
        lines.append(f"  Restaurants: {self.config['restaurants']['count']} ({self.config['restaurants'].get('layout', 'random')} layout)")
        lines.append(f"  Couriers: {self.config['couriers']['count']} ({self.config['couriers'].get('layout', 'random')} layout)")
        lines.append("")
        lines.append("Demand:")
        lines.append(f"  Total Orders: {self.config['demand']['total_orders']}")
        lines.append(f"  Profile: {self.config['demand']['profile']}")
        lines.append("")
        lines.append("Physics:")
        lines.append(f"  Courier Speed: {self.config['physics']['courier_speed_kmh']} km/h")
        lines.append(f"  Meal Prep: {self.config['physics']['meal_prep_time_s']}s")
        lines.append(f"  Batch Interval: {self.config['physics']['batch_interval_s']}s")

        return "\n".join(lines)

def create_scenario_from_config_file(config_path: str) -> Dict[str, Any]:

    from config_loader import load_config

    config = load_config(config_path)
    factory = ScenarioFactory(config)
    return factory.create_scenario()
