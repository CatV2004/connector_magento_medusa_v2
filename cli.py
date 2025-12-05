#!/usr/bin/env python3
"""
Command Line Interface for Magento to Medusa Sync
"""

import sys
import argparse
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger, logger
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from services.category_sync_service import CategorySyncService
from services.product_sync_service import ProductSyncService
from services.customer_sync_service import CustomerSyncService
from core.pipeline import create_pipeline, SyncPipeline


def create_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(
        description='Magento to Medusa Data Sync Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s sync categories
  %(prog)s sync products --batch-size 50 --max-pages 10
  %(prog)s sync all --dry-run
  %(prog)s dlq export --entity products --format csv
  %(prog)s pipeline run
  %(prog)s pipeline run --async --dry-run
  %(prog)s pipeline status
  %(prog)s pipeline cancel --pipeline-id pipeline_20231201_123456
  %(prog)s pipeline resume --state-file pipeline_state_20231201_123456.json
        """
    )
    
    # Main command
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Sync data from Magento to Medusa')
    sync_parser.add_argument('entity', choices=['all', 'categories', 'products', 'customers'],
                           help='Entity type to sync')
    sync_parser.add_argument('--batch-size', type=int, default=50,
                           help='Batch size for processing (default: 50)')
    sync_parser.add_argument('--max-pages', type=int,
                           help='Maximum number of pages to process')
    sync_parser.add_argument('--dry-run', action='store_true',
                           help='Test run without making changes')
    sync_parser.add_argument('--resume', action='store_true',
                           help='Resume from last sync position')
    sync_parser.add_argument('--output', type=str,
                           help='Output file for sync results')
    
    # DLQ command
    dlq_parser = subparsers.add_parser('dlq', help='Manage Dead Letter Queue')
    dlq_parser.add_argument('action', choices=['list', 'export', 'retry', 'clear'],
                          help='DLQ action to perform')
    dlq_parser.add_argument('--entity', choices=['all', 'products', 'categories', 'customers'],
                          help='Entity type (default: all)')
    dlq_parser.add_argument('--format', choices=['json', 'csv'], default='json',
                          help='Export format (default: json)')
    dlq_parser.add_argument('--output', type=str,
                          help='Output file for export')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='View sync statistics')
    stats_parser.add_argument('--entity', choices=['all', 'products', 'categories', 'customers'],
                            help='Entity type (default: all)')
    stats_parser.add_argument('--format', choices=['table', 'json', 'csv'], default='table',
                            help='Output format (default: table)')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Manage configuration')
    config_parser.add_argument('action', choices=['validate', 'test', 'generate'],
                             help='Configuration action')
    config_parser.add_argument('--mapping', type=str,
                             help='Mapping file to validate/generate')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test connections')
    test_parser.add_argument('--system', choices=['both', 'magento', 'medusa'], default='both',
                           help='System to test (default: both)')
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Pipeline management')
    pipeline_subparsers = pipeline_parser.add_subparsers(dest='pipeline_action', 
                                                       help='Pipeline action', required=True)
    
    # Run pipeline
    run_parser = pipeline_subparsers.add_parser('run', help='Run pipeline')
    run_parser.add_argument('--async', action='store_true', dest='async_run',
                          help='Run pipeline asynchronously')
    run_parser.add_argument('--dry-run', action='store_true',
                          help='Test run without making changes')
    run_parser.add_argument('--config', type=str,
                          help='Pipeline configuration file')
    
    # Pipeline status
    status_parser = pipeline_subparsers.add_parser('status', help='Pipeline status')
    status_parser.add_argument('--pipeline-id', type=str,
                             help='Specific pipeline ID')
    status_parser.add_argument('--format', choices=['table', 'json', 'yaml'], default='table',
                             help='Output format')
    
    # Cancel pipeline
    cancel_parser = pipeline_subparsers.add_parser('cancel', help='Cancel pipeline')
    cancel_parser.add_argument('--pipeline-id', type=str, required=True,
                             help='Pipeline ID to cancel')
    
    # Resume pipeline
    resume_parser = pipeline_subparsers.add_parser('resume', help='Resume pipeline')
    resume_parser.add_argument('--state-file', type=str, required=True,
                             help='State file to resume from')
    resume_parser.add_argument('--dry-run', action='store_true',
                             help='Test run without making changes')
    
    return parser


def main():
    """Main CLI entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Configure logging
    setup_logger()
    
    logger.info("=" * 70)
    logger.info(f"Magento to Medusa Sync Tool - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    try:
        # Initialize connectors (only if needed)
        magento = None
        medusa = None
        
        # Commands that need connectors
        needs_connectors = ['sync', 'test', 'pipeline']
        if args.command in needs_connectors:
            logger.info("Initializing connectors...")
            magento = MagentoConnector()
            medusa = MedusaConnector()
        
        # Execute command
        if args.command == 'sync':
            handle_sync_command(args, magento, medusa)
        elif args.command == 'dlq':
            handle_dlq_command(args)
        elif args.command == 'stats':
            handle_stats_command(args)
        elif args.command == 'config':
            handle_config_command(args)
        elif args.command == 'test':
            handle_test_command(args, magento, medusa)
        elif args.command == 'pipeline':
            handle_pipeline_command(args, magento, medusa)
        
        logger.info("Command completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


def test_connections(magento: MagentoConnector, medusa: MedusaConnector):
    """Test connections to both systems"""
    try:
        magento.test_connection()
        logger.info("✓ Magento connection successful")
    except Exception as e:
        logger.error(f"✗ Magento connection failed: {e}")
        raise
    
    try:
        medusa.test_connection()
        logger.info("✓ Medusa connection successful")
    except Exception as e:
        logger.error(f"✗ Medusa connection failed: {e}")
        raise


def handle_sync_command(args, magento: MagentoConnector, medusa: MedusaConnector):
    """Handle sync command"""
    logger.info(f"Starting sync for: {args.entity}")
    logger.info(f"Batch size: {args.batch_size}, Dry run: {args.dry_run}")
    
    if args.dry_run:
        logger.warning("DRY RUN MODE - No changes will be made")
    
    results = {}
    
    if args.entity in ['all', 'categories']:
        logger.info("\n" + "=" * 60)
        logger.info("SYNCING CATEGORIES")
        logger.info("=" * 60)
        
        service = CategorySyncService(magento, medusa)
        category_result = service.sync_all()
        results['categories'] = category_result
        
        if args.dry_run:
            logger.info("Dry run - would have synced categories")
        else:
            logger.info(f"Category sync completed: {len(category_result.get('mapping', {}))} items")
    
    if args.entity in ['all', 'products']:
        logger.info("\n" + "=" * 60)
        logger.info("SYNCING PRODUCTS")
        logger.info("=" * 60)
        
        # Get category mapping if categories were synced
        category_mapping = results.get('categories', {}).get('mapping', {})
        
        service = ProductSyncService(magento, medusa, category_mapping)
        product_result = service.sync_all(
            batch_size=args.batch_size,
            max_pages=args.max_pages
        )
        results['products'] = product_result
        
        if args.dry_run:
            logger.info("Dry run - would have synced products")
        else:
            stats = product_result.get('stats', {})
            logger.info(f"Product sync completed: {stats.get('successful', 0)} successful")
    
    if args.entity in ['all', 'customers']:
        logger.info("\n" + "=" * 60)
        logger.info("SYNCING CUSTOMERS")
        logger.info("=" * 60)
        
        service = CustomerSyncService(magento, medusa)
        customer_result = service.sync_all(
            batch_size=args.batch_size,
            max_pages=args.max_pages
        )
        results['customers'] = customer_result
        
        if args.dry_run:
            logger.info("Dry run - would have synced customers")
        else:
            stats = customer_result.get('stats', {})
            logger.info(f"Customer sync completed: {stats.get('successful', 0)} successful")
    
    # Save results if output specified
    if args.output:
        save_results(results, args.output)
    
    # Print summary
    print_sync_summary(results)


def handle_dlq_command(args):
    """Handle DLQ command"""
    from core.dlq_handler import DLQHandler
    
    entities = []
    if args.entity == 'all' or not args.entity:
        entities = ['products', 'categories', 'customers']
    else:
        entities = [args.entity]
    
    if args.action == 'list':
        for entity in entities:
            dlq = DLQHandler(entity)
            count = dlq.get_count()
            print(f"{entity.capitalize()}: {count} items")
    
    elif args.action == 'export':
        for entity in entities:
            dlq = DLQHandler(entity)
            output_file = args.output or f"{entity}_dlq_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{args.format}"
            if args.format == 'csv':
                dlq.export_to_csv(output_file)
            else:
                # Export as JSON
                import json
                items = []
                pattern = f"{entity}_*.json"
                dlq_dir = Path("dlq")
                for filepath in dlq_dir.glob(pattern):
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            items.extend(data)
                    except Exception as e:
                        logger.warning(f"Failed to read DLQ file {filepath}: {e}")
                
                with open(output_file, 'w') as f:
                    json.dump(items, f, indent=2)
            
            print(f"Exported {entity} DLQ to {output_file}")
    
    elif args.action == 'retry':
        print("Retry functionality not implemented yet")
        # This would require retry logic in each sync service
    
    elif args.action == 'clear':
        confirm = input("Are you sure you want to clear DLQ? (yes/no): ")
        if confirm.lower() == 'yes':
            for entity in entities:
                # In production, you'd archive instead of delete
                print(f"Clearing {entity} DLQ...")
                # Implementation would delete DLQ files


def handle_stats_command(args):
    """Handle stats command"""
    # This would read from a stats database or file
    print("Statistics functionality not implemented yet")


def handle_config_command(args):
    """Handle config command"""
    if args.action == 'validate':
        validate_mapping_config(args.mapping)
    elif args.action == 'test':
        print("Config test not implemented yet")
    elif args.action == 'generate':
        generate_mapping_template(args.mapping)


def handle_test_command(args, magento: MagentoConnector, medusa: MedusaConnector):
    """Handle test command"""
    if args.system in ['both', 'magento']:
        try:
            result = magento.test_connection()
            print(f"✓ Magento: Connected successfully")
            if isinstance(result, dict):
                print(f"   Response keys: {list(result.keys())}")
        except Exception as e:
            print(f"✗ Magento: Connection failed - {e}")
    
    if args.system in ['both', 'medusa']:
        try:
            result = medusa.test_connection()
            print(f"✓ Medusa: Connected successfully")
            if isinstance(result, dict):
                print(f"   Response keys: {list(result.keys())}")
        except Exception as e:
            print(f"✗ Medusa: Connection failed - {e}")


def handle_pipeline_command(args, magento: MagentoConnector, medusa: MedusaConnector):
    """Handle pipeline commands"""
    if args.pipeline_action == 'run':
        handle_pipeline_run(args, magento, medusa)
    elif args.pipeline_action == 'status':
        handle_pipeline_status(args)
    elif args.pipeline_action == 'cancel':
        handle_pipeline_cancel(args)
    elif args.pipeline_action == 'resume':
        handle_pipeline_resume(args, magento, medusa)
    else:
        print(f"Unknown pipeline action: {args.pipeline_action}")


def handle_pipeline_run(args, magento: MagentoConnector, medusa: MedusaConnector):
    """Run pipeline"""
    logger.info(f"Running {'async ' if args.async_run else ''}pipeline...")
    
    # Test connections first
    test_connections(magento, medusa)
    
    # Load config if specified
    config = {}
    if args.config:
        import yaml
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    
    # Create pipeline
    pipeline_type = "async" if args.async_run else "default"
    pipeline = create_pipeline(magento, medusa, pipeline_type, config)
    
    # Run pipeline
    try:
        if args.async_run:
            import asyncio
            result = asyncio.run(pipeline.run_async(dry_run=args.dry_run))
        else:
            result = pipeline.run(dry_run=args.dry_run)
        
        # Display results
        print_pipeline_results(result)
        
        # Save results to file
        if args.dry_run:
            output_file = f"pipeline_dry_run_{pipeline.pipeline_id}.json"
        else:
            output_file = f"pipeline_results_{pipeline.pipeline_id}.json"
        
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        logger.info(f"Pipeline results saved to {output_file}")
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        
        # Save state for possible resume
        if hasattr(pipeline, 'pipeline_id'):
            state_file = f"pipeline_state_{pipeline.pipeline_id}.json"
            pipeline._handle_interruption()
            logger.info(f"Pipeline state saved to {state_file}")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


def handle_pipeline_status(args):
    """Check pipeline status"""
    pipeline_id = args.pipeline_id
    
    if pipeline_id:
        # Check specific pipeline
        state_file = f"pipeline_state_{pipeline_id}.json"
        report_file = f"pipeline_results_{pipeline_id}.json"
        
        if os.path.exists(state_file):
            print(f"Pipeline {pipeline_id}: IN PROGRESS (state file exists)")
            with open(state_file, 'r') as f:
                state = json.load(f)
            print(f"  Status: {state.get('status', 'unknown')}")
            print(f"  Last update: {state.get('timestamp', 'unknown')}")
        elif os.path.exists(report_file):
            print(f"Pipeline {pipeline_id}: COMPLETED (report file exists)")
            with open(report_file, 'r') as f:
                report = json.load(f)
            print(f"  Final status: {report.get('status', 'unknown')}")
            print(f"  Duration: {report.get('stats', {}).get('duration', 'unknown')}")
        else:
            print(f"Pipeline {pipeline_id}: NOT FOUND")
    else:
        # List all pipelines
        print("Looking for pipeline files...")
        
        state_files = list(Path('.').glob('pipeline_state_*.json'))
        report_files = list(Path('.').glob('pipeline_results_*.json'))
        
        print("\nActive Pipelines (state files):")
        for file in state_files:
            try:
                with open(file, 'r') as f:
                    state = json.load(f)
                pid = state.get('pipeline_id', 'unknown')
                status = state.get('status', 'unknown')
                timestamp = state.get('timestamp', 'unknown')
                print(f"  {pid}: {status} ({timestamp})")
            except Exception:
                print(f"  {file.stem}: Error reading file")
        
        print("\nCompleted Pipelines (report files):")
        for file in report_files:
            try:
                with open(file, 'r') as f:
                    report = json.load(f)
                pid = report.get('pipeline_id', 'unknown')
                status = report.get('status', 'unknown')
                duration = report.get('stats', {}).get('duration', 'unknown')
                if duration and isinstance(duration, (int, float)):
                    duration_str = f"{duration:.2f}s"
                else:
                    duration_str = str(duration)
                print(f"  {pid}: {status} ({duration_str})")
            except Exception:
                print(f"  {file.stem}: Error reading file")


def handle_pipeline_cancel(args):
    """Cancel a pipeline"""
    pipeline_id = args.pipeline_id
    
    # Look for active pipeline with this ID
    state_file = f"pipeline_state_{pipeline_id}.json"
    
    if not os.path.exists(state_file):
        print(f"No active pipeline found with ID: {pipeline_id}")
        print("Note: Only pipelines with saved state files can be cancelled")
        return
    
    # Check if pipeline is actually running
    with open(state_file, 'r') as f:
        state = json.load(f)
    
    current_status = state.get('status', 'unknown')
    if current_status not in ['running', 'paused']:
        print(f"Pipeline {pipeline_id} is not running (status: {current_status})")
        return
    
    # Ask for confirmation
    confirm = input(f"Are you sure you want to cancel pipeline {pipeline_id}? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Cancellation aborted")
        return
    
    # Update state file to indicate cancellation
    state['status'] = 'cancelled'
    state['cancelled_at'] = datetime.now().isoformat()
    
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"Pipeline {pipeline_id} marked as cancelled")
    
    # Rename file to indicate cancellation
    cancelled_file = f"pipeline_cancelled_{pipeline_id}.json"
    os.rename(state_file, cancelled_file)
    print(f"State file moved to: {cancelled_file}")


def handle_pipeline_resume(args, magento: MagentoConnector, medusa: MedusaConnector):
    """Resume a pipeline from saved state"""
    state_file = args.state_file
    
    if not os.path.exists(state_file):
        print(f"State file not found: {state_file}")
        return
    
    logger.info(f"Resuming pipeline from {state_file}")
    
    # Test connections first
    test_connections(magento, medusa)
    
    # Load state
    with open(state_file, 'r') as f:
        state = json.load(f)
    
    pipeline_id = state.get('pipeline_id')
    current_status = state.get('status', 'unknown')
    
    if current_status not in ['running', 'paused']:
        print(f"Cannot resume pipeline with status: {current_status}")
        print("Only pipelines with status 'running' or 'paused' can be resumed")
        return
    
    # Create pipeline
    pipeline = SyncPipeline(magento, medusa, pipeline_id)
    pipeline.build_default_pipeline()  # Rebuild steps
    
    # Resume pipeline
    result = pipeline.resume(state_file)
    
    # Display results
    print_pipeline_results(result)
    
    # Save results
    output_file = f"pipeline_resumed_{pipeline_id}.json"
    with open(output_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    logger.info(f"Resumed pipeline results saved to {output_file}")
    
    # Archive old state file
    archived_file = f"archived_{os.path.basename(state_file)}"
    os.rename(state_file, archived_file)
    logger.info(f"Original state file archived to {archived_file}")


def validate_mapping_config(mapping_file: str):
    """Validate mapping configuration file"""
    from mappers.base_mapper import BaseMapper
    
    try:
        if not mapping_file:
            mapping_files = ['category_mapping.yaml', 'product_mapping.yaml', 'customer_mapping.yaml']
        else:
            mapping_files = [mapping_file]
        
        for file in mapping_files:
            print(f"Validating {file}...")
            try:
                mapper = BaseMapper(file, None, None)
                print(f"  ✓ Valid YAML structure")
                print(f"  ✓ Entity: {mapper.entity}")
                print(f"  ✓ Source: {mapper.source_system} -> Target: {mapper.target_system}")
                
                # Check required sections
                required_sections = ['fields', 'validation']
                for section in required_sections:
                    if section in mapper.mapping_config:
                        print(f"  ✓ Has '{section}' section")
                    else:
                        print(f"  ✗ Missing '{section}' section")
                
            except Exception as e:
                print(f"  ✗ Validation failed: {e}")
    
    except Exception as e:
        print(f"Validation error: {e}")


def generate_mapping_template(entity_type: str):
    """Generate mapping template for an entity"""
    templates = {
        'category': {
            'version': '1.0',
            'source': 'magento',
            'target': 'medusa',
            'entity': 'category',
            'fields': {
                'name': {
                    'target': 'name',
                    'required': True,
                    'type': 'string'
                }
            }
        },
        'product': {
            'version': '1.0',
            'source': 'magento',
            'target': 'medusa',
            'entity': 'product',
            'fields': {
                'sku': {
                    'target': 'sku',
                    'required': True,
                    'type': 'string'
                }
            }
        }
    }
    
    if not entity_type:
        print("Please specify entity type with --mapping (category, product, customer)")
        return
    
    if entity_type not in templates:
        print(f"Unknown entity type: {entity_type}")
        return
    
    output_file = f"{entity_type}_mapping_template.yaml"
    import yaml
    
    with open(output_file, 'w') as f:
        yaml.dump(templates[entity_type], f, default_flow_style=False)
    
    print(f"Generated template: {output_file}")


def save_results(results: Dict, output_file: str):
    """Save sync results to file"""
    import json
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Results saved to {output_file}")


def print_sync_summary(results: Dict):
    """Print sync summary to console"""
    print("\n" + "=" * 70)
    print("SYNC SUMMARY")
    print("=" * 70)
    
    for entity, result in results.items():
        if entity == 'categories':
            mapping = result.get('mapping', {})
            print(f"Categories: {len(mapping)} items mapped")
        
        elif entity in ['products', 'customers']:
            stats = result.get('stats', {})
            print(f"{entity.capitalize()}:")
            print(f"  Total: {stats.get('total_processed', 0)}")
            print(f"  Successful: {stats.get('successful', 0)}")
            print(f"  Failed: {stats.get('failed', 0)}")
            print(f"  Skipped: {stats.get('skipped', 0)}")
        
        dlq_count = result.get('dlq_count', 0)
        if dlq_count > 0:
            print(f"  DLQ items: {dlq_count}")
    
    print("=" * 70)


def print_pipeline_results(result: Dict[str, Any]):
    """Print pipeline results"""
    print("\n" + "=" * 70)
    print("PIPELINE RESULTS")
    print("=" * 70)
    print(f"Pipeline ID: {result['pipeline_id']}")
    print(f"Status: {result['status']}")
    
    stats = result['stats']
    if stats.get('duration'):
        print(f"Duration: {stats['duration']:.2f}s")
    
    print(f"Steps: {stats.get('completed_steps', 0)}/{stats.get('total_steps', 0)} completed")
    print(f"Items: {stats.get('successful_items', 0)} successful, "
          f"{stats.get('failed_items', 0)} failed")
    
    if stats.get('success_rate') is not None:
        print(f"Success Rate: {stats['success_rate']:.2f}%")
    
    # Display errors if any
    errors = result.get('errors', [])
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for error in errors[:5]:  # Show first 5 errors
            print(f"  - {error.get('error', 'Unknown error')}")
        if len(errors) > 5:
            print(f"  ... and {len(errors) - 5} more")
    
    print("=" * 70)


if __name__ == "__main__":
    main()