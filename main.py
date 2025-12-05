import sys
import json
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
from core.dlq_handler import DLQHandler
from core.pipeline import create_pipeline

# Configure logging
logger = setup_logger()


def print_header():
    """Print application header"""
    print("\n" + "=" * 70)
    print("MAGENTO TO MEDUSA SYNC TOOL")
    print("=" * 70)


def print_menu():
    """Print main menu"""
    print("\nMAIN MENU:")
    print("1. Test Connections")
    print("2. Sync Categories")
    print("3. Sync Products")
    print("4. Sync Customers")
    print("5. Run Full Sync Pipeline")
    print("6. View DLQ (Failed Items)")
    print("7. Export DLQ to CSV")
    print("8. View Sync Statistics")
    print("9. Advanced CLI Mode")
    print("0. Exit")
    print("-" * 70)


def get_choice(prompt: str = "Enter your choice: ") -> str:
    """Get user choice with validation"""
    try:
        choice = input(prompt).strip()
        return choice
    except (EOFError, KeyboardInterrupt):
        print("\nOperation cancelled.")
        return "0"


def test_connections(magento: MagentoConnector, medusa: MedusaConnector) -> bool:
    """Test connections and return success status"""
    print("\n" + "=" * 50)
    print("TESTING CONNECTIONS")
    print("=" * 50)
    
    success = True
    
    try:
        print("Testing Magento connection...")
        result = magento.test_connection()
        print("✓ Magento: Connected successfully")
        if isinstance(result, dict):
            print(f"  Response keys: {list(result.keys())[:5]}...")
    except Exception as e:
        print(f"✗ Magento: Connection failed - {e}")
        success = False
    
    try:
        print("\nTesting Medusa connection...")
        result = medusa.test_connection()
        print("✓ Medusa: Connected successfully")
        if isinstance(result, dict):
            print(f"  Response keys: {list(result.keys())[:5]}...")
    except Exception as e:
        print(f"✗ Medusa: Connection failed - {e}")
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("ALL CONNECTIONS SUCCESSFUL")
    else:
        print("SOME CONNECTIONS FAILED")
    print("=" * 50)
    
    return success


def sync_categories_interactive(magento: MagentoConnector, medusa: MedusaConnector):
    """Interactive category sync"""
    print("\n" + "=" * 50)
    print("SYNC CATEGORIES")
    print("=" * 50)
    
    # Get batch size
    batch_size = get_batch_size()
    
    # Confirm
    print(f"\nSettings:")
    print(f"  Batch size: {batch_size}")
    confirm = get_choice("\nProceed with sync? (yes/no): ").lower()
    
    if confirm != 'yes':
        print("Sync cancelled.")
        return
    
    print("\nStarting category sync...")
    
    try:
        service = CategorySyncService(magento, medusa)
        result = service.sync_all()
        
        print("\n" + "=" * 50)
        print("CATEGORY SYNC COMPLETED")
        print("=" * 50)
        
        stats = result.get('stats', {})
        print(f"Total processed: {stats.get('total_processed', 0)}")
        print(f"Successful: {stats.get('successful', 0)}")
        print(f"Failed: {stats.get('failed', 0)}")
        print(f"Skipped: {stats.get('skipped', 0)}")
        
        mapping = result.get('mapping', {})
        print(f"Categories mapped: {len(mapping)}")
        
        dlq_count = result.get('dlq_count', 0)
        if dlq_count > 0:
            print(f"\n⚠️  {dlq_count} items failed and moved to DLQ")
            print("  Use option 6 to view DLQ items")
        
        # Save mapping for product sync
        if mapping:
            mapping_file = f"category_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(mapping_file, 'w') as f:
                json.dump(mapping, f, indent=2)
            print(f"\nCategory mapping saved to: {mapping_file}")
        
    except Exception as e:
        print(f"\n❌ Category sync failed: {e}")
        logger.error(f"Category sync failed: {e}")


def sync_products_interactive(magento: MagentoConnector, medusa: MedusaConnector):
    """Interactive product sync"""
    print("\n" + "=" * 50)
    print("SYNC PRODUCTS")
    print("=" * 50)
    
    # Check for category mapping
    mapping_files = list(Path('.').glob('category_mapping_*.json'))
    category_mapping = {}
    
    if mapping_files:
        print("\nFound existing category mapping files:")
        for i, file in enumerate(mapping_files[:5], 1):
            print(f"  {i}. {file.name}")
        if len(mapping_files) > 5:
            print(f"  ... and {len(mapping_files) - 5} more")
        
        use_existing = get_choice("\nUse existing category mapping? (yes/no): ").lower()
        
        if use_existing == 'yes':
            if len(mapping_files) == 1:
                mapping_file = mapping_files[0]
            else:
                choice = get_choice(f"Select mapping file (1-{len(mapping_files)}): ")
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(mapping_files):
                        mapping_file = mapping_files[idx]
                    else:
                        print("Invalid selection, using latest.")
                        mapping_file = max(mapping_files, key=lambda p: p.stat().st_mtime)
                except ValueError:
                    print("Invalid input, using latest.")
                    mapping_file = max(mapping_files, key=lambda p: p.stat().st_mtime)
            
            with open(mapping_file, 'r') as f:
                category_mapping = json.load(f)
            print(f"Using category mapping from: {mapping_file.name}")
    
    # Get sync settings
    batch_size = get_batch_size()
    
    max_pages = None
    limit_pages = get_choice("Limit number of pages? (yes/no): ").lower()
    if limit_pages == 'yes':
        try:
            max_pages = int(get_choice("Max pages to sync: "))
        except ValueError:
            print("Invalid number, no limit will be applied.")
    
    # Confirm
    print(f"\nSettings:")
    print(f"  Batch size: {batch_size}")
    print(f"  Max pages: {max_pages or 'No limit'}")
    print(f"  Category mapping: {len(category_mapping)} categories")
    
    dry_run = get_choice("\nPerform dry run? (yes/no): ").lower() == 'yes'
    if dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
    
    confirm = get_choice("\nProceed with sync? (yes/no): ").lower()
    
    if confirm != 'yes':
        print("Sync cancelled.")
        return
    
    print("\nStarting product sync...")
    
    try:
        service = ProductSyncService(magento, medusa, category_mapping)
        result = service.sync_all(
            batch_size=batch_size,
            max_pages=max_pages
        )
        
        print("\n" + "=" * 50)
        print("PRODUCT SYNC COMPLETED")
        print("=" * 50)
        
        stats = result.get('stats', {})
        print(f"Total processed: {stats.get('total_processed', 0)}")
        print(f"Successful: {stats.get('successful', 0)}")
        print(f"Failed: {stats.get('failed', 0)}")
        print(f"Skipped: {stats.get('skipped', 0)}")
        print(f"Simple products: {stats.get('simple_products', 0)}")
        print(f"Configurable products: {stats.get('configurable_products', 0)}")
        print(f"Variants created: {stats.get('variants_created', 0)}")
        
        dlq_count = result.get('dlq_count', 0)
        if dlq_count > 0:
            print(f"\n⚠️  {dlq_count} items failed and moved to DLQ")
            print("  Use option 6 to view DLQ items")
        
        # Save results
        if not dry_run:
            result_file = f"product_sync_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(result_file, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\nResults saved to: {result_file}")
        
    except Exception as e:
        print(f"\n❌ Product sync failed: {e}")
        logger.error(f"Product sync failed: {e}")


def get_batch_size() -> int:
    """Get batch size from user"""
    default = 50
    
    try:
        choice = get_choice(f"Batch size [{default}]: ")
        if choice:
            batch_size = int(choice)
            if batch_size <= 0:
                print(f"Invalid batch size, using default: {default}")
                return default
            if batch_size > 500:
                print(f"Batch size too large, capping at 500")
                return 500
            return batch_size
    except ValueError:
        print(f"Invalid number, using default: {default}")
    
    return default


def view_dlq():
    """View DLQ items"""
    print("\n" + "=" * 50)
    print("DEAD LETTER QUEUE (FAILED ITEMS)")
    print("=" * 50)
    
    entities = ['products', 'categories', 'customers']
    total_items = 0
    
    for entity in entities:
        dlq = DLQHandler(entity)
        count = dlq.get_count()
        total_items += count
        print(f"{entity.capitalize()}: {count} items")
    
    print(f"\nTotal failed items: {total_items}")
    
    if total_items > 0:
        print("\nActions:")
        print("  1. View details of failed items")
        print("  2. Export to CSV for manual review")
        print("  3. Retry failed items")
        print("  4. Back to main menu")
        
        choice = get_choice("\nSelect action: ")
        
        if choice == '1':
            # Show recent failed items
            for entity in entities:
                dlq = DLQHandler(entity)
                pattern = f"{entity}_*.json"
                dlq_dir = Path("dlq")
                files = list(dlq_dir.glob(pattern))
                if files:
                    latest_file = max(files, key=lambda p: p.stat().st_mtime)
                    try:
                        with open(latest_file, 'r') as f:
                            items = json.load(f)
                            print(f"\nRecent {entity} failures:")
                            for i, item in enumerate(items[:3], 1):
                                error = item.get('error', 'Unknown error')
                                sku = item.get('source_data', {}).get('sku', 'N/A')
                                print(f"  {i}. SKU: {sku}")
                                print(f"     Error: {error[:100]}...")
                    except Exception as e:
                        print(f"  Error reading {entity} DLQ: {e}")
        
        elif choice == '2':
            export_dlq_to_csv_interactive()
        
        elif choice == '3':
            print("Retry functionality requires CLI mode.")
            print("Use: python connectors/cli.py pipeline run")
    
    else:
        print("\n✅ No failed items in DLQ")


def export_dlq_to_csv_interactive():
    entities = ['products', 'categories', 'customers']
    
    print("\nSelect entity to export:")
    for i, entity in enumerate(entities, 1):
        dlq = DLQHandler(entity)
        count = dlq.get_count()
        print(f"  {i}. {entity.capitalize()} ({count} items)")
    print(f"  4. All entities")
    
    choice = get_choice("\nSelect: ")
    
    try:
        idx = int(choice) - 1
        if idx == 3:  # All
            selected_entities = entities
        elif 0 <= idx < 3:
            selected_entities = [entities[idx]]
        else:
            print("Invalid selection")
            return
    except ValueError:
        print("Invalid input")
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for entity in selected_entities:
        dlq = DLQHandler(entity)
        output_file = f"{entity}_dlq_export_{timestamp}.csv"
        dlq.export_to_csv(output_file)
        print(f"Exported {entity} DLQ to: {output_file}")


def run_pipeline_interactive(magento: MagentoConnector, medusa: MedusaConnector):
    print("\n" + "=" * 50)
    print("RUN SYNC PIPELINE")
    print("=" * 50)
    
    print("\nPipeline steps:")
    print("  1. Test connections")
    print("  2. Sync categories")
    print("  3. Sync products")
    print("  4. Sync customers")
    print("  5. Process DLQ items")
    print("  6. Generate report")
    
    print("\nOptions:")
    print("  1. Run full pipeline")
    print("  2. Custom pipeline")
    print("  3. Dry run (test mode)")
    
    choice = get_choice("\nSelect option: ")
    
    if choice == '1':
        # Full pipeline
        dry_run = get_choice("\nPerform dry run? (yes/no): ").lower() == 'yes'
        run_full_pipeline(magento, medusa, dry_run)
    
    elif choice == '2':
        # Custom pipeline
        configure_custom_pipeline(magento, medusa)
    
    elif choice == '3':
        # Dry run
        run_full_pipeline(magento, medusa, dry_run=True)
    
    else:
        print("Invalid selection")


def run_full_pipeline(magento: MagentoConnector, medusa: MedusaConnector, dry_run: bool = False):
    print(f"\n{'🚀 STARTING FULL SYNC PIPELINE' if not dry_run else '🔧 STARTING DRY RUN'}")
    if dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
    
    try:
        pipeline = create_pipeline(magento, medusa, "default")
        result = pipeline.run(dry_run=dry_run)
        
        print("\n" + "=" * 50)
        print("PIPELINE COMPLETED")
        print("=" * 50)
        
        print(f"Status: {result['status']}")
        print(f"Duration: {result['stats'].get('duration', 'N/A')}")
        print(f"Steps completed: {result['stats'].get('completed_steps', 0)}/{result['stats'].get('total_steps', 0)}")
        print(f"Success rate: {result['stats'].get('success_rate', 0):.1f}%")
        
        # Save report
        report_file = f"pipeline_report_{pipeline.pipeline_id}.json"
        with open(report_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\nReport saved to: {report_file}")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Pipeline failed: {e}")


def configure_custom_pipeline(magento: MagentoConnector, medusa: MedusaConnector):
    print("\nCustom pipeline configuration")
    print("-" * 30)
    
    steps = {
        'test_connections': True,
        'sync_categories': False,
        'sync_products': False,
        'sync_customers': False,
        'process_dlq': False
    }
    
    print("\nSelect steps to include:")
    for i, (step, default) in enumerate(steps.items(), 1):
        step_name = step.replace('_', ' ').title()
        choice = get_choice(f"  {i}. {step_name}? (yes/no) [{ 'yes' if default else 'no'}]: ").lower()
        if choice in ['yes', 'y', '']:
            steps[step] = True
        else:
            steps[step] = False
    
    batch_size = get_batch_size()
    dry_run = get_choice("\nPerform dry run? (yes/no): ").lower() == 'yes'
    
    print(f"\nConfiguration:")
    for step, enabled in steps.items():
        print(f"  {step}: {'✅' if enabled else '❌'}")
    print(f"  Batch size: {batch_size}")
    print(f"  Dry run: {'✅' if dry_run else '❌'}")
    
    confirm = get_choice("\nRun pipeline with these settings? (yes/no): ").lower()
    if confirm != 'yes':
        print("Pipeline cancelled.")
        return
    
    # Note: Full custom pipeline configuration would require more complex setup
    # For now, we'll run a simplified version
    print("\nNote: Full custom pipeline requires CLI mode.")
    print("For advanced configuration, use:")
    print("  python connectors/cli.py pipeline run --config your_config.yaml")
    
    # Run basic pipeline instead
    run_full_pipeline(magento, medusa, dry_run)


def view_statistics():
    """View sync statistics"""
    print("\n" + "=" * 50)
    print("SYNC STATISTICS")
    print("=" * 50)
    
    # Look for result files
    result_patterns = [
        'pipeline_report_*.json',
        'product_sync_results_*.json',
        'category_mapping_*.json'
    ]
    
    found_files = False
    
    for pattern in result_patterns:
        files = list(Path('.').glob(pattern))
        if files:
            found_files = True
            latest_file = max(files, key=lambda p: p.stat().st_mtime)
            file_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
            
            print(f"\n{pattern.replace('_*.json', '').replace('_', ' ').title()}:")
            print(f"  Latest: {latest_file.name}")
            print(f"  Date: {file_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Try to load and show basic stats
            try:
                with open(latest_file, 'r') as f:
                    data = json.load(f)
                    
                if 'stats' in data:
                    stats = data['stats']
                    if 'successful' in stats and 'total_processed' in stats:
                        success_rate = (stats['successful'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0
                        print(f"  Success rate: {success_rate:.1f}%")
                
                if 'mapping' in data:
                    print(f"  Items mapped: {len(data['mapping'])}")
                    
            except Exception:
                pass
    
    if not found_files:
        print("\nNo sync results found.")
        print("Run a sync first to generate statistics.")
    
    # DLQ stats
    print("\n" + "-" * 30)
    print("DLQ Statistics:")
    
    entities = ['products', 'categories', 'customers']
    for entity in entities:
        dlq = DLQHandler(entity)
        count = dlq.get_count()
        if count > 0:
            print(f"  {entity.capitalize()}: {count} failed items")


def launch_cli_mode():
    """Launch advanced CLI mode"""
    print("\nLaunching advanced CLI mode...")
    print("Type 'exit' to return to menu.")
    print("-" * 50)
    
    import subprocess
    import os
    
    # Run CLI with --help first
    subprocess.run([sys.executable, "connectors/cli.py", "--help"])
    
    print("\nEnter CLI commands (or 'exit' to return):")
    
    while True:
        try:
            command = input("\ncli> ").strip()
            
            if command.lower() in ['exit', 'quit', 'q']:
                print("Returning to menu...")
                break
            
            if command:
                # Split command into arguments
                args = command.split()
                
                # Run CLI command
                result = subprocess.run(
                    [sys.executable, "connectors/cli.py"] + args,
                    capture_output=True,
                    text=True
                )
                
                print(result.stdout)
                if result.stderr:
                    print("Errors:", result.stderr)
                    
        except KeyboardInterrupt:
            print("\nReturning to menu...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main menu interface"""
    print_header()
    
    # Initialize connectors
    magento = None
    medusa = None
    
    try:
        while True:
            print_menu()
            choice = get_choice()
            
            if choice == '0':
                print("\nThank you for using Magento to Medusa Sync Tool!")
                print("Goodbye! 👋")
                break
            
            elif choice == '1':
                # Test connections
                if magento is None or medusa is None:
                    print("\nInitializing connectors...")
                    magento = MagentoConnector()
                    medusa = MedusaConnector()
                test_connections(magento, medusa)
            
            elif choice == '2':
                # Sync categories
                if magento is None or medusa is None:
                    print("\nInitializing connectors...")
                    magento = MagentoConnector()
                    medusa = MedusaConnector()
                if test_connections(magento, medusa):
                    sync_categories_interactive(magento, medusa)
            
            elif choice == '3':
                # Sync products
                if magento is None or medusa is None:
                    print("\nInitializing connectors...")
                    magento = MagentoConnector()
                    medusa = MedusaConnector()
                if test_connections(magento, medusa):
                    sync_products_interactive(magento, medusa)
            
            elif choice == '4':
                # Sync customers
                print("\nCustomer sync requires CLI mode.")
                print("Use: python connectors/cli.py sync customers")
                input("\nPress Enter to continue...")
            
            elif choice == '5':
                # Run pipeline
                if magento is None or medusa is None:
                    print("\nInitializing connectors...")
                    magento = MagentoConnector()
                    medusa = MedusaConnector()
                if test_connections(magento, medusa):
                    run_pipeline_interactive(magento, medusa)
            
            elif choice == '6':
                # View DLQ
                view_dlq()
            
            elif choice == '7':
                # Export DLQ
                export_dlq_to_csv_interactive()
            
            elif choice == '8':
                # View statistics
                view_statistics()
            
            elif choice == '9':
                # Advanced CLI mode
                launch_cli_mode()
            
            else:
                print("\nInvalid choice. Please try again.")
            
            input("\nPress Enter to continue...")
            
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        logger.error(f"Main menu error: {e}")


if __name__ == "__main__":
    main()