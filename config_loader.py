
import yaml
import os
from typing import Dict, Any, Optional
from copy import deepcopy

# Default "physics" parameters (shared across all scenarios)
DEFAULT_PHYSICS = {
    'map_size_m': 5000,
    'distance_metric': 'manhattan',  # ENFORCED: Manhattan-only
    'courier_speed_kmh': 30.0,
    'pickup_service_time_s': 90,
    'dropoff_service_time_s': 45,
    'meal_prep_time_s': 300,  # 5 minutes
    'order_expiration_minutes': 30,
    'batch_interval_s': 300  # ENFORCED: 5 minutes
}

# Default algorithm parameters
DEFAULT_ALGORITHMS = {
    'bundling': {
        'max_bundle_size': 3
    },
    'anticipated': {
        'lookahead_window_s': 900,  # 15 minutes
        'alpha_penalty': 0.5,  # Wait time penalty
        'beta_penalty': 0.3   # Freshness penalty
    }
}

# Default visualization parameters
DEFAULT_VISUALIZATION = {
    'frames': 60,
    'fps': 2,
    'sample_interval_s': 60
}

def load_config(config_path: str) -> Dict[str, Any]:

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Failed to parse YAML config: {e}")

    # Validate and fill in defaults
    config = _apply_defaults(config)
    _validate_config(config)

    return config

def _apply_defaults(config: Dict[str, Any]) -> Dict[str, Any]:

    config = deepcopy(config)  # Don't modify original

    # Apply physics defaults
    if 'physics' not in config:
        config['physics'] = {}
    for key, default_value in DEFAULT_PHYSICS.items():
        if key not in config['physics']:
            config['physics'][key] = default_value

    # Apply algorithm defaults
    if 'algorithms' not in config:
        config['algorithms'] = deepcopy(DEFAULT_ALGORITHMS)
    else:
        if 'bundling' not in config['algorithms']:
            config['algorithms']['bundling'] = DEFAULT_ALGORITHMS['bundling']
        if 'anticipated' not in config['algorithms']:
            config['algorithms']['anticipated'] = DEFAULT_ALGORITHMS['anticipated']

    # Apply visualization defaults
    if 'visualization' not in config:
        config['visualization'] = DEFAULT_VISUALIZATION
    else:
        for key, default_value in DEFAULT_VISUALIZATION.items():
            if key not in config['visualization']:
                config['visualization'][key] = default_value

    return config

def _validate_config(config: Dict[str, Any]) -> None:

    # Validate scenario section
    if 'scenario' not in config:
        raise ValueError("Config must have 'scenario' section")

    required_scenario_fields = ['name', 'duration_hours']
    for field in required_scenario_fields:
        if field not in config['scenario']:
            raise ValueError(f"scenario.{field} is required")

    # Validate restaurants section
    if 'restaurants' not in config:
        raise ValueError("Config must have 'restaurants' section")
    if 'count' not in config['restaurants']:
        raise ValueError("restaurants.count is required")
    if config['restaurants']['count'] < 1:
        raise ValueError("restaurants.count must be >= 1")

    # Validate couriers section
    if 'couriers' not in config:
        raise ValueError("Config must have 'couriers' section")
    if 'count' not in config['couriers']:
        raise ValueError("couriers.count is required")
    if config['couriers']['count'] < 1:
        raise ValueError("couriers.count must be >= 1")

    # Validate demand section
    if 'demand' not in config:
        raise ValueError("Config must have 'demand' section")
    if 'total_orders' not in config['demand']:
        raise ValueError("demand.total_orders is required")
    if config['demand']['total_orders'] < 1:
        raise ValueError("demand.total_orders must be >= 1")
    if 'profile' not in config['demand']:
        raise ValueError("demand.profile is required")

    # Validate distance metric (ENFORCED: Manhattan-only)
    if config['physics']['distance_metric'] != 'manhattan':
        raise ValueError(
            f"Only 'manhattan' distance metric is supported, "
            f"got: {config['physics']['distance_metric']}"
        )

    # Validate batch interval (ENFORCED: 5 minutes)
    if config['physics']['batch_interval_s'] != 300:
        raise ValueError(
            f"batch_interval_s must be 300 (5 minutes), "
            f"got: {config['physics']['batch_interval_s']}"
        )

def save_config_snapshot(config: Dict[str, Any], output_path: str) -> None:

    import json
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)

def get_scenario_name(config: Dict[str, Any]) -> str:

    return config['scenario']['name']

def get_output_directory(config: Dict[str, Any], base_dir: str = 'outputs') -> str:

    scenario_name = get_scenario_name(config)
    return os.path.join(base_dir, scenario_name)
