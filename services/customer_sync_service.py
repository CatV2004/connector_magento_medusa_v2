from typing import Dict, List, Optional
from datetime import datetime
import time
from utils.logger import logger
from mappers.customer_mapper import CustomerMapper
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector


class CustomerSyncService:
    """Service for syncing customers from Magento to Medusa"""
    
    def __init__(self, magento: MagentoConnector, medusa: MedusaConnector):
        self.magento = magento
        self.medusa = medusa
        self.mapper = CustomerMapper(magento, medusa)
        self.dlq = DLQHandler('customers')
        
        self.sync_stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'with_orders': 0,
            'new_customers': 0,
            'updated_customers': 0
        }
        
        # Cache for existing customers
        self.existing_customers: Dict[str, str] = {}  # Email -> Medusa ID
    
    def sync_all(self, batch_size: int = 100, max_pages: Optional[int] = None) -> Dict:
        """
        Sync all customers from Magento to Medusa
        
        Args:
            batch_size: Number of customers per batch
            max_pages: Maximum number of pages to process
            
        Returns:
            Dict: Sync statistics
        """
        logger.info("Starting customer sync from Magento to Medusa...")
        
        try:
            # Load existing customers from Medusa
            self._load_existing_customers()
            
            # Sync customers in batches
            page = 1
            has_more = True
            
            while has_more and (max_pages is None or page <= max_pages):
                logger.info(f"Processing customer page {page}...")
                
                # Fetch customers from Magento
                magento_customers = self.magento.get_customers(
                    page=page,
                    page_size=batch_size
                )
                
                if not magento_customers:
                    logger.info("No more customers to sync")
                    has_more = False
                    break
                
                # Process batch
                self._process_batch(magento_customers, page)
                
                # Increment page
                page += 1
                
                # Small delay
                time.sleep(0.3)
            
            # Process DLQ items
            self._process_dlq_items()
            
            # Log summary
            self._log_sync_summary()
            
            return {
                'stats': self.sync_stats,
                'dlq_count': self.dlq.get_count()
            }
            
        except Exception as e:
            logger.error(f"Customer sync failed: {e}")
            raise
    
    def _load_existing_customers(self):
        """Load existing customers from Medusa"""
        try:
            # Note: Medusa Admin API might not have bulk customer list endpoint
            # This would need to be implemented based on your Medusa setup
            logger.info("Customer list loading depends on Medusa API availability")
            self.existing_customers = {}
            
        except Exception as e:
            logger.warning(f"Failed to load existing customers: {e}")
            self.existing_customers = {}
    
    def _process_batch(self, magento_customers: List[Dict], batch_number: int):
        """Process a batch of customers"""
        logger.info(f"Processing batch {batch_number} with {len(magento_customers)} customers")
        
        for customer in magento_customers:
            self._process_single_customer(customer, batch_number)
        
        # Flush DLQ batch if needed
        self.dlq._flush_batch()
    
    def _process_single_customer(self, magento_customer: Dict, batch_number: int):
        """Process a single customer"""
        customer_id = magento_customer.get('id')
        email = magento_customer.get('email', '').lower()
        
        if not email:
            logger.warning(f"Customer {customer_id} has no email, skipping")
            self.sync_stats['skipped'] += 1
            return
        
        # Update stats
        self.sync_stats['total_processed'] += 1
        
        try:
            # Validate customer data
            validation_errors = self.mapper.validate_customer(magento_customer)
            if validation_errors['missing_required']:
                error_msg = f"Missing required fields: {validation_errors['missing_required']}"
                logger.warning(f"Customer {email} validation failed: {error_msg}")
                
                self.dlq.add_item({
                    'source_data': magento_customer,
                    'errors': validation_errors,
                    'operation': 'sync',
                    'batch': batch_number,
                    'timestamp': datetime.now().isoformat()
                })
                self.sync_stats['failed'] += 1
                return
            
            # Prepare context
            context = {
                'batch_id': f'batch_{batch_number}',
                'sync_timestamp': datetime.now().isoformat()
            }
            
            # Map customer data
            medusa_data = self.mapper.map(magento_customer, context)
            
            # Check if customer exists
            existing_id = self.existing_customers.get(email)
            
            if existing_id:
                # Update existing customer
                logger.info(f"Updating customer: {email}")
                self._update_customer(existing_id, medusa_data, email)
                self.sync_stats['updated_customers'] += 1
            else:
                # Create new customer
                logger.info(f"Creating new customer: {email}")
                self._create_customer(medusa_data, email)
                self.sync_stats['new_customers'] += 1
            
            # Check if customer has orders
            orders_count = self._get_customer_orders_count(customer_id)
            if orders_count > 0:
                self.sync_stats['with_orders'] += 1
            
            self.sync_stats['successful'] += 1
            logger.info(f"✓ {email}")
            
        except Exception as e:
            logger.error(f"Failed to process customer {email}: {e}")
            self.sync_stats['failed'] += 1
            
            self.dlq.add_item({
                'source_data': magento_customer,
                'error': str(e),
                'operation': 'sync',
                'batch': batch_number,
                'timestamp': datetime.now().isoformat()
            })
    
    def _get_customer_orders_count(self, customer_id: int) -> int:
        """Get count of orders for a customer"""
        try:
            # This would require a Magento API call to get orders by customer ID
            # For now, return 0
            return 0
        except Exception:
            return 0
    
    def _create_customer(self, customer_data: Dict, email: str):
        """Create customer in Medusa"""
        try:
            response = self.medusa.create_customer(customer_data)
            
            if 'customer' in response:
                new_customer = response['customer']
                customer_id = new_customer['id']
                
                # Update cache
                self.existing_customers[email] = customer_id
                
                logger.debug(f"Created customer {email} with ID: {customer_id}")
            else:
                raise Exception(f"Unexpected response format: {response}")
                
        except Exception as e:
            logger.error(f"Failed to create customer {email}: {e}")
            raise
    
    def _update_customer(self, customer_id: str, customer_data: Dict, email: str):
        """Update existing customer in Medusa"""
        try:
            # Note: Medusa Admin API might not have customer update endpoint
            # This would need to be implemented based on your Medusa setup
            logger.warning(f"Customer update not implemented for {email}")
            
            # For now, skip update
            pass
            
        except Exception as e:
            logger.error(f"Failed to update customer {email}: {e}")
            raise
    
    def _process_dlq_items(self):
        """Process items in DLQ"""
        dlq_count = self.dlq.get_count()
        if dlq_count > 0:
            logger.warning(f"Found {dlq_count} failed customers in DLQ")
    
    def _log_sync_summary(self):
        """Log sync summary"""
        logger.info("=" * 60)
        logger.info("CUSTOMER SYNC SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total processed: {self.sync_stats['total_processed']}")
        logger.info(f"Successful: {self.sync_stats['successful']}")
        logger.info(f"Failed: {self.sync_stats['failed']}")
        logger.info(f"Skipped: {self.sync_stats['skipped']}")
        logger.info(f"New customers: {self.sync_stats['new_customers']}")
        logger.info(f"Updated customers: {self.sync_stats['updated_customers']}")
        logger.info(f"Customers with orders: {self.sync_stats['with_orders']}")
        logger.info(f"DLQ items: {self.dlq.get_count()}")
        logger.info("=" * 60)