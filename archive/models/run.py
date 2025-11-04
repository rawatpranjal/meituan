#!/usr/bin/env python3
"""
Unified CLI entry point for dual-mode dispatch simulation.

Usage:
    python run.py --mode batch --strategy hungarian --cost distance_to_pickup
    python run.py --mode realtime --strategy greedy --micro-batch-sec 10
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import components
from models.simulator.orchestration.batch_orchestrator import BatchOrchestrator
from models.simulator.orchestration.realtime_orchestrator import RealtimeOrchestrator
from models.simulator.logger import SimulationLogger
from models.simulator.courier_timeline_logger import CourierTimelineLogger
from models.simulator import physics
from models.strategies import StrategyRegistry
from models.cost import get_cost_function
from models.simulator.services.bundling import BundlingService
from models.simulator.services.candidate_generation import CandidateGenerator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Run dispatch simulation with pluggable strategies'
    )

    # Mode selection
    parser.add_argument(
        '--mode',
        type=str,
        choices=['batch', 'realtime'],
        required=True,
        help='Simulation mode: batch (dispatch_time) or realtime (platform_order_time)'
    )

    # Strategy selection
    available_strategies = StrategyRegistry.list_strategies()
    parser.add_argument(
        '--strategy',
        type=str,
        choices=available_strategies,
        required=True,
        help='Assignment strategy to use'
    )

    # Cost function
    parser.add_argument(
        '--cost',
        type=str,
        default='distance_to_pickup',
        help='Cost function to use (default: distance_to_pickup)'
    )

    # Bundling
    parser.add_argument(
        '--bundling',
        type=str,
        choices=['on', 'off'],
        default='off',
        help='Enable same-restaurant bundling (default: off)'
    )

    # Real-time specific
    parser.add_argument(
        '--micro-batch-sec',
        type=int,
        default=10,
        help='Micro-batch window size in seconds for realtime mode (default: 10)'
    )

    # Candidate Generation
    parser.add_argument(
        '--candidate-radius',
        type=float,
        default=75000.0,
        help='Maximum radius for candidate generation (default: 75000 for microdegree coordinates, ~8.3km)'
    )

    parser.add_argument(
        '--disable-candidates',
        action='store_true',
        help='Disable candidate generation entirely (use full cost matrix)'
    )

    parser.add_argument(
        '--max-candidates-per-order',
        type=int,
        default=500,
        help='Maximum candidates per order (default: 500 for dense urban delivery)'
    )

    parser.add_argument(
        '--max-candidates-per-courier',
        type=int,
        default=500,
        help='Maximum candidates per courier (default: 500 for dense urban delivery)'
    )

    parser.add_argument(
        '--shared-candidates',
        action='store_true',
        help='Generate candidate graph once per wave and share across all strategies (ensures fairness in comparisons)'
    )

    # MILP specific
    parser.add_argument(
        '--milp-time-limit-sec',
        type=int,
        default=5,
        help='Time limit for MILP solver in seconds (default: 5)'
    )

    parser.add_argument(
        '--milp-orders-cap',
        type=int,
        default=150,
        help='Max orders MILP will process internally (default: 150)'
    )

    # Output
    parser.add_argument(
        '--output-dir',
        type=str,
        default='models/logs',
        help='Output directory for logs and visualizations (default: models/logs)'
    )

    # Seed for reproducibility
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    # Data paths
    parser.add_argument(
        '--data-dir',
        type=str,
        default='/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data',
        help='Directory containing data files'
    )

    return parser.parse_args()


def setup_components(args) -> Dict[str, Any]:
    """
    Set up simulation components based on arguments.

    Args:
        args: Parsed command-line arguments

    Returns:
        Dictionary with initialized components
    """
    # Set random seed
    import random
    import numpy as np
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Get cost function
    try:
        cost_function = get_cost_function(args.cost)
    except ValueError as e:
        logger.error(f"Invalid cost function: {e}")
        sys.exit(1)

    # Get strategy
    try:
        strategy = StrategyRegistry.get(args.strategy, cost_function)
    except ValueError as e:
        logger.error(f"Invalid strategy: {e}")
        sys.exit(1)

    # Set up model name for logging
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_name = f"{args.mode}_{args.strategy}_{args.cost}_{timestamp}"

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize loggers
    simulation_logger = SimulationLogger(model_name, str(output_dir))
    timeline_logger = CourierTimelineLogger(str(output_dir), model_name)  # Note: reversed order

    # Initialize services
    bundling_service = None
    if args.bundling == 'on':
        bundling_service = BundlingService(max_bundle_size=5)
        logger.info("Bundling service enabled")

    # Create candidate generator only if not disabled
    candidate_generator = None
    if not args.disable_candidates and args.candidate_radius > 0:
        candidate_generator = CandidateGenerator(
            max_pickup_radius=args.candidate_radius,
            max_candidates_per_order=args.max_candidates_per_order,
            max_candidates_per_courier=args.max_candidates_per_courier
        )
        logger.info(f"Candidate generation enabled with radius={args.candidate_radius}")
    else:
        logger.info("Candidate generation disabled - using full cost matrix")

    # Prepare data paths
    data_paths = {
        'waybill_path': os.path.join(args.data_dir, 'all_waybill_info_meituan_0322.csv'),
        'dispatch_waybill_path': os.path.join(args.data_dir, 'dispatch_waybill_meituan.csv'),
        'dispatch_rider_path': os.path.join(args.data_dir, 'dispatch_rider_meituan.csv')
    }

    # Verify data files exist
    for name, path in data_paths.items():
        if not Path(path).exists():
            logger.error(f"Data file not found: {path}")
            sys.exit(1)

    # Create orchestrator based on mode
    if args.mode == 'batch':
        orchestrator = BatchOrchestrator(
            assignment_strategy=strategy,
            cost_function=cost_function,
            simulation_logger=simulation_logger,
            timeline_logger=timeline_logger,
            physics=physics,
            bundling_service=bundling_service,
            candidate_generator=candidate_generator,
            shared_candidates=args.shared_candidates
        )
        logger.info(f"Created BatchOrchestrator with strategy: {args.strategy}, "
                   f"shared_candidates: {args.shared_candidates}")
    else:  # realtime
        orchestrator = RealtimeOrchestrator(
            assignment_strategy=strategy,
            cost_function=cost_function,
            simulation_logger=simulation_logger,
            timeline_logger=timeline_logger,
            physics=physics,
            bundling_service=bundling_service,
            candidate_generator=candidate_generator,
            shared_candidates=args.shared_candidates,
            micro_batch_sec=args.micro_batch_sec
        )
        logger.info(f"Created RealtimeOrchestrator with strategy: {args.strategy}, "
                   f"micro-batch: {args.micro_batch_sec}s")

    return {
        'orchestrator': orchestrator,
        'data_paths': data_paths,
        'model_name': model_name,
        'output_dir': output_dir,
        'args': args
    }


def write_manifest(output_dir: Path, model_name: str, args, results: Dict[str, Any]):
    """
    Write run manifest for reproducibility.

    Args:
        output_dir: Output directory
        model_name: Model name for this run
        args: Command-line arguments
        results: Simulation results
    """
    manifest = {
        'model_name': model_name,
        'timestamp': datetime.now().isoformat(),
        'configuration': {
            'mode': args.mode,
            'strategy': args.strategy,
            'cost_function': args.cost,
            'bundling': args.bundling,
            'micro_batch_sec': args.micro_batch_sec if args.mode == 'realtime' else None,
            'milp_time_limit_sec': args.milp_time_limit_sec if args.strategy == 'milp' else None,
            'milp_orders_cap': args.milp_orders_cap if args.strategy == 'milp' else None,
            'seed': args.seed,
            'data_dir': args.data_dir
        },
        'results': results,
        'output_files': {
            'assignment_log': f"{model_name}_assignment_log.csv",
            'cycle_summary': f"{model_name}_cycle_summary.csv",
            'courier_timeline': f"{model_name}_courier_timeline.csv",
            'manifest': f"{model_name}_manifest.json"
        }
    }

    manifest_path = output_dir / f"{model_name}_manifest.json"
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    logger.info(f"Wrote manifest to {manifest_path}")


def main():
    """Main entry point."""
    # Parse arguments
    args = parse_arguments()

    logger.info("="*60)
    logger.info("DISPATCH SIMULATION")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Strategy: {args.strategy}")
    logger.info(f"Cost Function: {args.cost}")
    logger.info(f"Bundling: {args.bundling}")
    if args.mode == 'realtime':
        logger.info(f"Micro-batch: {args.micro_batch_sec} seconds")
    logger.info("="*60)

    # Set up components
    components = setup_components(args)

    # Run simulation
    logger.info("\nStarting simulation...")
    try:
        results = components['orchestrator'].run_simulation(
            data_paths=components['data_paths'],
            model_name=components['model_name']
        )
        logger.info("\nSimulation completed successfully!")
    except Exception as e:
        logger.error(f"Simulation failed: {e}", exc_info=True)
        sys.exit(1)

    # Write manifest
    write_manifest(
        components['output_dir'],
        components['model_name'],
        args,
        results
    )

    # Print summary
    logger.info("\n" + "="*60)
    logger.info("SIMULATION SUMMARY")
    logger.info("="*60)
    for key, value in results.items():
        if isinstance(value, float):
            logger.info(f"{key}: {value:.4f}")
        else:
            logger.info(f"{key}: {value}")
    logger.info("="*60)

    # Print output locations
    logger.info("\nOutput files:")
    logger.info(f"  Assignment log: {components['output_dir']}/{components['model_name']}_assignment_log.csv")
    logger.info(f"  Cycle summary: {components['output_dir']}/{components['model_name']}_cycle_summary.csv")
    logger.info(f"  Courier timeline: {components['output_dir']}/{components['model_name']}_courier_timeline.csv")
    logger.info(f"  Manifest: {components['output_dir']}/{components['model_name']}_manifest.json")

    return 0


if __name__ == '__main__':
    sys.exit(main())