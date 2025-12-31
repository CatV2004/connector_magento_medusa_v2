"""
Customer Sync Service - Orchestrates customer and address synchronization.
Uses AddressSyncService for address operations to avoid code duplication.
"""
from typing import Dict, List, Optional, Set
from datetime import datetime
import time
from dataclasses import dataclass, field
from utils.logger import logger
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from mappers.customer_mapper import CustomerMapper, CustomerMappingResult
from mappers.address_mapper import AddressMapper, AddressMappingResult
from core.mapping.mapping_factory import MappingFactory
from pathlib import Path
from services.address_sync_service import AddressSyncService


@dataclass  
class SyncStats:
    """Statistics for sync operations"""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    new_customers: int = 0
    updated_customers: int = 0
    addresses_processed: int = 0
    addresses_created: int = 0
    addresses_updated: int = 0
    addresses_failed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        """Convert stats to dictionary"""
        duration = (datetime.now() - self.start_time).total_seconds()
        total_processed = max(self.total_processed, 1)
        total_addresses = max(self.addresses_processed, 1)
        
        return {
            'total_processed': self.total_processed,
            'successful': self.successful,
            'failed': self.failed,
            'skipped': self.skipped,
            'new_customers': self.new_customers,
            'updated_customers': self.updated_customers,
            'addresses_processed': self.addresses_processed,
            'addresses_created': self.addresses_created,
            'addresses_updated': self.addresses_updated,
            'addresses_failed': self.addresses_failed,
            'duration_seconds': round(duration, 2),
            'customer_success_rate': f"{self.successful / total_processed * 100:.1f}%",
            'address_success_rate': f"{(self.addresses_created + self.addresses_updated) / total_addresses * 100:.1f}%"
        }


class CustomerSyncService:
    """Service for orchestrating customer sync from Magento to Medusa"""
    
    def __init__(
        self, 
        magento: MagentoConnector, 
        medusa: MedusaConnector,
        config_path: Path = Path("config/mapping"),
    ):
        self.magento = magento
        self.medusa = medusa
        
        # Initialize mappers
        mapping_factory = MappingFactory(config_path)
        
        self.customer_mapper = CustomerMapper(
            mapping_config=mapping_factory.get("customer"),
            source_connector=magento,
            target_connector=medusa
        )
        
        # Initialize AddressSyncService for address operations
        self.address_sync_service = AddressSyncService(
            magento=magento,
            medusa=medusa,
            config_path=config_path
        )
        
        # Initialize handlers
        self.dlq = DLQHandler('customers')
        
        # Cache and state
        self.existing_customers: Dict[str, str] = {}  # Email -> Medusa ID
        self.sync_stats = SyncStats()
        self.address_cache: Dict[str, Set[str]] = {}  # customer_id -> set of magento_address_ids
        
    
    def sync_all(self, batch_size: int = 100, max_pages: Optional[int] = None) -> Dict:
        """
        Sync all customers from Magento to Medusa.
        Addresses are synced as part of customer sync.
        """
        logger.info("=" * 60)
        logger.info("Starting customer sync from Magento to Medusa")
        logger.info(f"Batch size: {batch_size}, Max pages: {max_pages or 'unlimited'}")
        logger.info("=" * 60)
        
        try:
            # Pre-sync setup
            self._pre_sync_setup()
            
            # Sync customers in batches
            self._sync_customers_in_batches(batch_size, max_pages)
            
            # Post-sync cleanup
            self._post_sync_cleanup()
            
            # Process any DLQ items
            self._process_dlq_items()
            
            # Log summary
            return self._generate_sync_summary()
            
        except Exception as e:
            logger.error(f"Customer sync failed: {e}")
            self.sync_stats.failed += 1
            raise
    
    def sync_single_customer(self, magento_customer_id: int) -> Dict:
        logger.info(f"Syncing single customer: {magento_customer_id}")
        
        try:
            # Fetch customer from Magento with addresses
            magento_customer = self.magento.get_customer(
                magento_customer_id, 
            )
            if not magento_customer:
                raise ValueError(f"Customer {magento_customer_id} not found in Magento")
            
            # Process the customer
            result = self._process_single_customer(
                magento_customer, 
                batch_number=0,
            )
            
            logger.info(f"✓ Single customer sync completed: {result['email']}")
            return result
            
        except Exception as e:
            logger.error(f"Single customer sync failed: {e}")
            raise
    
    def _pre_sync_setup(self):
        """Setup before sync begins"""
        logger.info("Pre-sync setup...")
        
        # Load existing customers from Medusa
        self._load_existing_customers()
        
        # Clear previous stats
        self.sync_stats = SyncStats()
        
        # Ensure DLQ is ready
        # self.dlq.clear()
        
        # Clear address cache
        self.address_cache.clear()
        
        logger.info("Pre-sync setup completed")
    
    def _load_existing_customers(self):
        """Load existing customers from Medusa"""
        try:
            logger.info("Loading existing customers from Medusa...")
            
            existing = self.medusa.get_customers(page_size=10000)
            
            self.existing_customers = {
                customer.get('email', '').lower(): customer.get('id')
                for customer in existing if customer.get('email')
            }
            
            logger.info(f"Loaded {len(self.existing_customers)} existing customers from Medusa")
            
        except Exception as e:
            logger.warning(f"Could not load existing customers: {e}")
            self.existing_customers = {}
    
    def _sync_customers_in_batches(self, batch_size: int, max_pages: Optional[int]):
        """Sync customers in paginated batches"""
        page = 1
        has_more = True
        
        while has_more and (max_pages is None or page <= max_pages):
            logger.info(f"Processing customer page {page}...")
            
            # Fetch customers from Magento
            magento_customers = self.magento.get_customers(
                page=page,
                page_size=batch_size,
            )
            
            if not magento_customers:
                logger.info("No more customers to sync")
                has_more = False
                break
            
            # Process batch
            self._process_batch(magento_customers, page)
            
            # Increment page
            page += 1
            
            # Rate limiting delay
            time.sleep(0.3)
    
    def _process_batch(self, magento_customers: List[Dict], batch_number: int):
        """Process a batch of customers"""
        logger.info(f"Processing batch {batch_number} with {len(magento_customers)} customers")
        
        for customer in magento_customers:
            try:
                result = self._process_single_customer(customer, batch_number)
                
                if result['status'] == 'success':
                    logger.info(f"✓ {result['email']} ({result['action']})")
                elif result['status'] == 'skipped':
                    logger.info(f"- {customer.get('email', 'unknown')} (skipped)")
                else:
                    logger.error(f"✗ {customer.get('email', 'unknown')} (failed)")
                    
            except Exception as e:
                logger.error(f"Unexpected error processing customer: {e}")
        
        # Flush DLQ batch if needed
        self.dlq._flush_batch()
    
    def _process_single_customer(
        self, 
        magento_customer: Dict, 
        batch_number: int,
    ) -> Dict:
        customer_id = magento_customer.get('id')
        email = magento_customer.get('email', '').lower()
        
        # Update global stats
        self.sync_stats.total_processed += 1
        
        # Validate email
        if not email or '@' not in email:
            logger.warning(f"Customer {customer_id} has invalid email, skipping")
            self.sync_stats.skipped += 1
            return {
                'status': 'skipped',
                'reason': 'invalid_email',
                'customer_id': customer_id,
                'email': email
            }
        
        try:
            # Step 1: Map customer data
            customer_result = self._map_customer_data(magento_customer, batch_number)
            
            # Check if mapping failed
            if customer_result.validation_errors:
                self._handle_mapping_errors(
                    magento_customer, 
                    customer_result.validation_errors, 
                    batch_number
                )
                return {
                    'status': 'failed', 
                    'reason': 'mapping_errors',
                    'email': email
                }
            
            # Step 2: Check if customer exists
            existing_customer_id = self.existing_customers.get(email)
            
            if existing_customer_id:
                # Update existing customer
                customer_sync_result = self._update_existing_customer(
                    existing_customer_id, 
                    customer_result.customer_data, 
                    email
                )
                action = 'updated'
                self.sync_stats.updated_customers += 1
                medusa_customer_id = existing_customer_id
            else:
                # Create new customer
                customer_sync_result = self._create_new_customer(
                    customer_result.customer_data, 
                    email
                )
                action = 'created'
                self.sync_stats.new_customers += 1
                medusa_customer_id = customer_sync_result['customer_id']
            
            # Step 3: Sync addresses if enabled
            address_results = self._sync_customer_addresses(
                magento_customer, 
                medusa_customer_id
            )
            
            # Update address stats
            self.sync_stats.addresses_processed += address_results.get('total', 0)
            self.sync_stats.addresses_created += address_results.get('created', 0)
            self.sync_stats.addresses_updated += address_results.get('updated', 0)
            self.sync_stats.addresses_failed += address_results.get('failed', 0)
        
            # Step 4: Update cache
            if action == 'created':
                self.existing_customers[email] = medusa_customer_id
            
            # Step 5: Mark successful
            self.sync_stats.successful += 1
            
            result = {
                'status': 'success',
                'action': action,
                'customer_id': medusa_customer_id,
                'email': email,
            }
            
            # Add address results if available
            if address_results:
                result.update({
                    'addresses_total': address_results.get('total', 0),
                    'addresses_created': address_results.get('created', 0),
                    'addresses_updated': address_results.get('updated', 0),
                    'addresses_failed': address_results.get('failed', 0)
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to process customer {email}: {e}")
            self.sync_stats.failed += 1
            
            # Add to DLQ
            self.dlq.add_item({
                'source_data': magento_customer,
                'error': str(e),
                'operation': 'sync',
                'batch': batch_number,
                'timestamp': datetime.now().isoformat()
            })
            
            return {
                'status': 'failed',
                'reason': 'processing_error',
                'error': str(e),
                'email': email
            }
    
    def _map_customer_data(self, magento_customer: Dict, batch_number: int) -> CustomerMappingResult:
        context = {
            'batch_id': f'batch_{batch_number}',
            'sync_timestamp': datetime.now().isoformat(),
            'is_batch_sync': True
        }
        
        return self.customer_mapper.map(magento_customer, context)
    
    def _handle_mapping_errors(self, magento_customer: Dict, errors: Dict, batch_number: int):
        logger.warning(f"Customer mapping errors: {errors}")
        
        self.dlq.add_item({
            'source_data': magento_customer,
            'errors': errors,
            'operation': 'mapping',
            'batch': batch_number,
            'timestamp': datetime.now().isoformat()
        })
    
    def _create_new_customer(self, customer_data: Dict, email: str) -> Dict:
        logger.debug(f"Creating new customer: {email}")
        
        try:
            response = self.medusa.create_customer(customer_data)
            
            if 'customer' in response:
                new_customer = response['customer']
                customer_id = new_customer['id']
                
                logger.debug(f"Created customer {email} with ID: {customer_id}")
                
                return {
                    'customer_id': customer_id,
                    'customer': new_customer
                }
            else:
                raise ValueError(f"Unexpected response format: {response}")
                
        except Exception as e:
            logger.error(f"Failed to create customer {email}: {e}")
            raise
    
    def _update_existing_customer(
        self,
        customer_id: str,
        customer_data: Dict,
        email: str
    ) -> Dict:
        """Update existing customer in Medusa"""
        logger.debug(f"Updating existing customer: {email}")
        
        # Prepare update payload
        allowed_fields = {
            "email",
            "company_name",
            "first_name",
            "last_name",
            "phone",
            "metadata",
            "additional_data",
        }
        
        payload = {
            k: v
            for k, v in customer_data.items()
            if k in allowed_fields and v is not None
        }
        
        if not payload:
            logger.debug(f"No changes detected for customer {email}, skip update")
            return {
                "customer_id": customer_id,
                "updated": False,
                "reason": "no_changes"
            }
        
        try:
            resp = self.medusa.update_customer(customer_id, payload)
            
            logger.debug(f"Customer updated successfully: {email}")
            
            return {
                "customer_id": customer_id,
                "updated": True,
                "updated_fields": list(payload.keys()),
                "customer": resp.get("customer")
            }
            
        except Exception as e:
            logger.error(f"Failed to update customer {email}: {e}")
            raise
    
    def _sync_customer_addresses(self, magento_customer: Dict, medusa_customer_id: str) -> Dict:
        """
        Sync addresses for a customer using AddressSyncService.
        
        Returns:
            Address sync results
        """
        addresses = magento_customer.get('addresses', [])
        
        if not addresses:
            logger.debug(f"No addresses for customer {medusa_customer_id}")
            return {
                'created': 0,
                'updated': 0,
                'failed': 0,
                'total': 0
            }
        
        logger.debug(f"Syncing {len(addresses)} addresses for customer {medusa_customer_id}")
        
        # Process addresses using AddressSyncService
        results = self.address_sync_service._process_addresses(addresses, medusa_customer_id)
        
        # Cache processed addresses
        if medusa_customer_id not in self.address_cache:
            self.address_cache[medusa_customer_id] = set()
        
        # Add magento address IDs to cache
        for address in addresses:
            magento_address_id = address.get('id')
            if magento_address_id:
                self.address_cache[medusa_customer_id].add(str(magento_address_id))
        
        return results
    
    def _post_sync_cleanup(self):
        """Cleanup after sync completes"""
        logger.info("Post-sync cleanup...")
        
        # Clear caches to free memory
        self.existing_customers.clear()
        self.address_cache.clear()
        
        logger.info("Post-sync cleanup completed")
    
    def _process_dlq_items(self):
        """Process items in DLQ (Dead Letter Queue)"""
        dlq_count = self.dlq.get_count()
        
        if dlq_count > 0:
            logger.warning(f"Found {dlq_count} failed customers in DLQ")
            # Optionally implement retry logic here
        else:
            logger.info("DLQ is empty - no failed items to retry")
    
    def _generate_sync_summary(self) -> Dict:
        """Generate and log sync summary"""
        logger.info("=" * 60)
        logger.info("CUSTOMER SYNC COMPLETE - SUMMARY")
        logger.info("=" * 60)
        
        stats_dict = self.sync_stats.to_dict()
        
        # Customer stats
        logger.info("--- CUSTOMER STATISTICS ---")
        logger.info(f"Total Processed: {stats_dict['total_processed']}")
        logger.info(f"Successful: {stats_dict['successful']}")
        logger.info(f"Failed: {stats_dict['failed']}")
        logger.info(f"Skipped: {stats_dict['skipped']}")
        logger.info(f"New Customers: {stats_dict['new_customers']}")
        logger.info(f"Updated Customers: {stats_dict['updated_customers']}")
        logger.info(f"Success Rate: {stats_dict['customer_success_rate']}")
        
        # Address stats (if enabled)
        logger.info("--- ADDRESS STATISTICS ---")
        logger.info(f"Addresses Processed: {stats_dict['addresses_processed']}")
        logger.info(f"Addresses Created: {stats_dict['addresses_created']}")
        logger.info(f"Addresses Updated: {stats_dict['addresses_updated']}")
        logger.info(f"Addresses Failed: {stats_dict['addresses_failed']}")
        logger.info(f"Address Success Rate: {stats_dict['address_success_rate']}")
        
        # System stats
        logger.info("--- SYSTEM STATISTICS ---")
        logger.info(f"Duration: {stats_dict['duration_seconds']} seconds")
        logger.info(f"DLQ Items: {self.dlq.get_count()}")
        
        logger.info("=" * 60)
        
        return {
            'stats': stats_dict,
            'dlq_count': self.dlq.get_count(),
            'timestamp': datetime.now().isoformat()
        }