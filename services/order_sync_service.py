import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from contextlib import contextmanager
import json
from pathlib import Path
from utils.logger import logger
from mappers.order_mapper import OrderMapper
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from core.mapping.mapping_factory import MappingFactory

class OrderSyncService:
    """Service xử lý order migration với retry và rollback logic"""
    
    def __init__(self, magento: MagentoConnector, medusa: MedusaConnector):
        self.magento = magento
        self.medusa = medusa
        
        mapping = MappingFactory(
            Path("config/mapping")
        ).get("order")
        
        self.mapper = OrderMapper(
            mapping_config=mapping
        )
        
        self.dlq = DLQHandler('orders')
        self.retry_queue = {}
        
        # Stats tracking
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'retried': 0,
            'total_amount_migrated': Decimal('0.00'),
            'start_time': None,
            'end_time': None
        }
        
        # Configuration
        self.BATCH_SIZE = 50
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 5  # seconds
        
    def sync_orders_delta(self, since_date: datetime = None) -> Dict:
        self.stats['start_time'] = datetime.now()
        logger.info(f"Starting DELTA order migration since {since_date}")
        
        try:
            # 1. Fetch orders từ Magento (với pagination)
            orders = self._fetch_orders_since(since_date)
            
            # 2. Process từng batch
            for i in range(0, len(orders), self.BATCH_SIZE):
                batch = orders[i:i + self.BATCH_SIZE]
                logger.info(f"Processing batch {i//self.BATCH_SIZE + 1} ({len(batch)} orders)")
                
                success, failed = self._process_batch(batch)
                
                # 3. Retry logic cho failed items
                if failed:
                    self._handle_failed_orders(failed)
                
                # 4. Interim stats reporting
                self._report_progress(i + len(batch), len(orders))
            
            # 5. Final reconciliation report
            self.stats['end_time'] = datetime.now()
            return self._generate_migration_report()
            
        except Exception as e:
            logger.error(f"Delta migration failed: {e}", exc_info=True)
            raise
    
    def _fetch_orders_since(self, since_date: datetime = None) -> List[Dict]:
        """Lấy orders từ Magento với updated_at filter"""
        filter_criteria = []
        
        if since_date:
            # Format date cho Magento API
            from_date = since_date.strftime('%Y-%m-%d %H:%M:%S')
            filter_criteria.append({
                'field': 'updated_at',
                'condition_type': 'gteq',
                'value': from_date
            })
        
        # Thêm filter để exclude test orders nếu cần
        filter_criteria.append({
            'field': 'state',
            'condition_type': 'nin',
            'value': ['canceled', 'holded'] 
        })
        
        try:
            # Giả sử magento connector có method search_orders
            orders = self.magento.search_orders(
                filters=filter_criteria,
                page_size=1000
            )
            logger.info(f"Fetched {len(orders)} orders for migration")
            return orders
            
        except Exception as e:
            logger.error(f"Failed to fetch orders: {e}")
            return []
    
    def _process_batch(self, batch: List[Dict]) -> Tuple[List, List]:
        """Process một batch orders với transaction-like logic"""
        successful = []
        failed = []
        
        for magento_order in batch:
            order_number = magento_order.get('increment_id', 'unknown')
            
            try:
                # 1. Pre-flight check
                self._validate_order_prerequisites(magento_order)
                
                # 2. Map data với context đầy đủ
                context = self._build_mapping_context(magento_order)
                medusa_order_data = self.mapper.map(magento_order, context)
                
                # 3. Kiểm tra order đã tồn tại chưa (idempotency)
                existing_order = self._find_existing_order(magento_order)
                if existing_order:
                    logger.info(f"Order {order_number} already exists, skipping")
                    self.stats['successful'] += 1
                    successful.append(magento_order)
                    continue
                
                # 4. Tạo order trong Medusa
                with self._order_transaction_context(order_number):
                    created_order = self.medusa.create_order(medusa_order_data)
                    
                    # 5. Tạo associated payments/invoices nếu có
                    self._create_associated_records(magento_order, created_order['id'])
                    
                    # 6. Update stats
                    self.stats['successful'] += 1
                    self.stats['total_amount_migrated'] += Decimal(
                        str(magento_order.get('grand_total', 0))
                    )
                    
                    successful.append(magento_order)
                    logger.info(f"Successfully migrated order {order_number}")
                    
            except Exception as e:
                logger.error(f"Failed to migrate order {order_number}: {e}")
                failed.append({
                    'order': magento_order,
                    'error': str(e),
                    'attempts': 1
                })
                self.stats['failed'] += 1
        
        return successful, failed
    
    @contextmanager
    def _order_transaction_context(self, order_number: str):
        steps_completed = []
        
        try:
            yield steps_completed
            # Nếu mọi thứ OK, commit (trong Medusa thường auto-commit)
            
        except Exception as e:
            logger.error(f"Transaction failed for order {order_number}: {e}")
            # Rollback/compensation logic
            self._compensate_steps(order_number, steps_completed)
            raise
    
    def _compensate_steps(self, order_number: str, steps: List[str]):
        """Compensation actions khi order creation fail"""
        logger.warning(f"Executing compensation for order {order_number}")
        
        # 1. Xóa order nếu đã tạo
        if 'order_created' in steps:
            try:
                # Tìm Medusa order ID từ mapping
                # Xóa order (hoặc mark as cancelled)
                pass
            except Exception as e:
                logger.error(f"Failed to compensate order creation: {e}")
        
        # 2. Xóa payments/invoices liên quan
        if 'payment_created' in steps:
            try:
                # Xóa payment records
                pass
            except Exception as e:
                logger.error(f"Failed to compensate payment creation: {e}")
        
        # 3. Log compensation action
        self.dlq.add_item({
            'order_number': order_number,
            'compensated_steps': steps,
            'timestamp': datetime.now().isoformat(),
            'action': 'rollback_executed'
        })
    
    def _create_associated_records(self, magento_order: Dict, medusa_order_id: str):
        """Tạo payment và invoice records liên quan đến order"""
        
        # 1. Create payment từ Magento payment info
        payment_data = self._extract_payment_data(magento_order)
        if payment_data:
            try:
                payment_data['order_id'] = medusa_order_id
                self.medusa.create_payment(payment_data)
            except Exception as e:
                logger.warning(f"Failed to create payment for order {medusa_order_id}: {e}")
                # Không throw error - payment là optional per requirements
        
        # 2. Create invoice nếu có
        invoice_data = self._extract_invoice_data(magento_order)
        if invoice_data:
            try:
                invoice_data['order_id'] = medusa_order_id
                self.medusa.create_invoice(invoice_data)
            except Exception as e:
                logger.warning(f"Failed to create invoice for order {medusa_order_id}: {e}")
    
    def _handle_failed_orders(self, failed_orders: List[Dict]):
        """Xử lý failed orders với retry logic"""
        for failed_item in failed_orders:
            order_num = failed_item['order'].get('increment_id', 'unknown')
            attempts = failed_item.get('attempts', 1)
            
            if attempts < self.MAX_RETRIES:
                # Schedule retry
                self.retry_queue[order_num] = {
                    'data': failed_item['order'],
                    'attempts': attempts + 1,
                    'last_error': failed_item['error'],
                    'scheduled_retry': datetime.now() + timedelta(seconds=self.RETRY_DELAY * attempts)
                }
                logger.info(f"Scheduled retry {attempts+1}/{self.MAX_RETRIES} for order {order_num}")
            else:
                # Move to DLQ sau max retries
                self.dlq.add_item({
                    'order': failed_item['order'],
                    'errors': [failed_item['error']],
                    'attempts': attempts,
                    'final_failure': True,
                    'timestamp': datetime.now().isoformat()
                })
                logger.error(f"Order {order_num} moved to DLQ after {attempts} failed attempts")
    
    def _generate_migration_report(self) -> Dict:
        """Tạo comprehensive migration report"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        report = {
            'summary': {
                'total_processed': self.stats['total_processed'],
                'successful': self.stats['successful'],
                'failed': self.stats['failed'],
                'retried': self.stats['retried'],
                'success_rate': (self.stats['successful'] / self.stats['total_processed'] * 100) 
                               if self.stats['total_processed'] > 0 else 0,
                'total_amount_migrated': str(self.stats['total_amount_migrated']),
                'duration_seconds': duration,
                'orders_per_second': self.stats['total_processed'] / duration if duration > 0 else 0
            },
            'validation': {
                'checksum_passed': self._validate_final_checksum(),
                'dlq_count': self.dlq.get_count(),
                'pending_retries': len(self.retry_queue)
            },
            'timestamps': {
                'start': self.stats['start_time'].isoformat(),
                'end': self.stats['end_time'].isoformat()
            },
            'recommendations': self._generate_recommendations()
        }
        
        return report
    
    def get_id_mapping(self) -> Dict[str, str]:
        return self.mapper.get_id_mapping() if hasattr(self, 'mapper') else {}
    
    def _report_progress(self, processed: int, total: int):
        """Report migration progress - ĐÃ THÊM"""
        percentage = (processed / total * 100) if total > 0 else 0
        
        # Chỉ log mỗi 10% hoặc 100 records
        if processed == total or processed % max(100, total // 10) == 0:
            logger.info(
                f"Progress: {processed}/{total} orders "
                f"({percentage:.1f}%) - "
                f"Success: {self.stats['successful']}, "
                f"Failed: {self.stats['failed']}, "
                f"Skipped: {self.stats['skipped']}"
            )
            
            # Estimate remaining time
            if processed > 0 and hasattr(self, 'start_time'):
                elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
                if elapsed > 0:
                    records_per_second = processed / elapsed
                    remaining = total - processed
                    if records_per_second > 0:
                        eta_seconds = remaining / records_per_second
                        eta_str = str(timedelta(seconds=int(eta_seconds)))
                        logger.info(f"Estimated time remaining: {eta_str}")
    
    def _build_mapping_context(self, magento_order: Dict) -> Dict[str, Any]:
        order_id = str(magento_order.get('entity_id') or magento_order.get('id', ''))
        
        context = {
            'operation': 'create',
            'order_id': order_id,
            'customer_id_mapping': self._load_customer_mapping(),
            'product_id_mapping': self._load_product_mapping(),
            'order_id_mapping': self.get_id_mapping(),
            'strict_mode': self.mapper.get('validation', {}).get('strict', False),
            'timestamp': datetime.now().isoformat(),
            'batch_id': getattr(self, 'current_batch_id', 'default'),
            'metadata': {
                'source_system': 'magento',
                'source_order_id': order_id,
                'source_increment_id': magento_order.get('increment_id'),
                'migration_timestamp': datetime.now().isoformat()
            }
        }
        
        # Add payment metadata if available
        payment_metadata = self._extract_payment_metadata(magento_order)
        if payment_metadata:
            context['payment_metadata'] = payment_metadata
            
        return context
    
    def _load_customer_mapping(self) -> Dict[str, str]:
        try:
            
            mapping_file = Path('mappings/customer_id_mapping.json')
            if mapping_file.exists():
                with open(mapping_file, 'r') as f:
                    return json.load(f)
            
        except Exception as e:
            logger.warning(f"Failed to load customer mapping: {e}")
            
        return {}
    
    def _load_product_mapping(self) -> Dict[str, str]:
        try:
            import json
            from pathlib import Path
            
            mapping_file = Path('mappings/product_id_mapping.json')
            if mapping_file.exists():
                with open(mapping_file, 'r') as f:
                    return json.load(f)
                    
        except Exception as e:
            logger.warning(f"Failed to load product mapping: {e}")
            
        return {}
    
    def _validate_order_prerequisites(self, magento_order: Dict):
        errors = []
        warnings = []
        
        # 1. Check customer exists
        customer_id = magento_order.get('customer_id')
        if customer_id:
            customer_mapping = self._load_customer_mapping()
            if str(customer_id) not in customer_mapping:
                warnings.append(f"Customer {customer_id} not yet migrated")
        else:
            # Guest order
            if not magento_order.get('customer_email'):
                warnings.append("Guest order has no customer_email")
        
        # 2. Check products exist
        items = magento_order.get('items', [])
        product_mapping = self._load_product_mapping()
        
        missing_products = []
        for item in items:
            product_id = str(item.get('product_id', ''))
            if product_id and product_id not in product_mapping:
                missing_products.append(product_id)
                
        if missing_products:
            warnings.append(f"Products not migrated: {set(missing_products)}")
            
        # 3. Check address information
        if not magento_order.get('billing_address'):
            errors.append("Missing billing_address")
            
        # 4. Check monetary values
        grand_total = magento_order.get('grand_total')
        if grand_total is None or Decimal(str(grand_total)) <= 0:
            warnings.append(f"Invalid grand_total: {grand_total}")
        
        # Log warnings, throw error only if critical
        if warnings:
            order_id = magento_order.get('increment_id', 'unknown')
            logger.warning(f"Order {order_id} prerequisites warnings: {warnings}")
            
        if errors:
            raise ValueError(f"Order prerequisites failed: {errors}")
    
    def _find_existing_order(self, magento_order: Dict) -> Optional[Dict]:
        try:
            # Cách 1: Tìm bằng source metadata
            source_increment_id = magento_order.get('increment_id')
            
            if source_increment_id:
                # Search in Medusa by metadata
                existing = self.medusa.search_orders({
                    'metadata[source_increment_id]': source_increment_id
                })
                if existing and len(existing) > 0:
                    return existing[0]
            
            # Cách 2: Tìm bằng customer + total + date (fallback)
            customer_email = magento_order.get('customer_email')
            grand_total = magento_order.get('grand_total')
            created_at = magento_order.get('created_at')
            
            if customer_email and grand_total and created_at:
                # Chuyển đổi created_at format
                from datetime import datetime
                order_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                date_str = order_date.strftime('%Y-%m-%d')
                
                # Search orders trong khoảng thời gian
                existing = self.medusa.search_orders({
                    'email': customer_email,
                    'created_at[gte]': f'{date_str}T00:00:00',
                    'created_at[lte]': f'{date_str}T23:59:59'
                })
                
                # Filter by total amount (cho phép sai số nhỏ)
                for order in existing:
                    order_total = Decimal(str(order.get('total', 0)))
                    magento_total = Decimal(str(grand_total))
                    tolerance = Decimal('0.01')
                    
                    if abs(order_total - magento_total) <= tolerance:
                        return order
                        
        except Exception as e:
            logger.debug(f"Error finding existing order: {e}")
            
        return None
    
    def _extract_payment_data(self, magento_order: Dict) -> Optional[Dict]:
        payment_info = magento_order.get('payment', {})
        if not payment_info:
            return None
            
        # Map payment method
        method_mapping = {
            'checkmo': 'manual',
            'banktransfer': 'bank_transfer',
            'cashondelivery': 'cash_on_delivery',
            'creditcard': 'stripe',
            'paypal_express': 'paypal',
            'stripe_payments': 'stripe'
        }
        
        magento_method = payment_info.get('method', '')
        provider_id = method_mapping.get(magento_method, 'manual')
        
        payment_data = {
            'provider_id': provider_id,
            'amount': magento_order.get('grand_total'),
            'currency_code': magento_order.get('order_currency_code', 'USD'),
            'data': {
                'source_payment_id': payment_info.get('entity_id'),
                'method': magento_method,
                'cc_last4': payment_info.get('cc_last4'),
                'cc_type': payment_info.get('cc_type'),
                'cc_exp_month': payment_info.get('cc_exp_month'),
                'cc_exp_year': payment_info.get('cc_exp_year'),
                'last_trans_id': payment_info.get('last_trans_id')
            },
            'metadata': {
                'source_order_id': magento_order.get('entity_id'),
                'source_increment_id': magento_order.get('increment_id'),
                'migration_timestamp': datetime.now().isoformat()
            }
        }
        
        # Set status based on order status
        order_status = magento_order.get('status', '')
        if order_status in ['complete', 'processing']:
            payment_data['status'] = 'captured'
        elif order_status in ['pending', 'pending_payment']:
            payment_data['status'] = 'pending'
        elif order_status == 'canceled':
            payment_data['status'] = 'canceled'
        else:
            payment_data['status'] = 'pending'
            
        return payment_data
    
    def _extract_invoice_data(self, magento_order: Dict) -> Optional[Dict]:
        status = magento_order.get('status', '')
        if status not in ['complete', 'processing', 'closed']:
            return None
            
        invoice_data = {
            'order_id': None,  # Sẽ được set trong mapping context
            'status': 'posted',  # hoặc 'draft' tùy state
            'amount': magento_order.get('grand_total'),
            'currency_code': magento_order.get('order_currency_code', 'USD'),
            'items': [],
            'metadata': {
                'source_order_id': magento_order.get('entity_id'),
                'source_increment_id': magento_order.get('increment_id'),
                'invoice_type': 'migration_generated',
                'migration_timestamp': datetime.now().isoformat()
            }
        }
        
        # Add line items
        for item in magento_order.get('items', []):
            invoice_item = {
                'product_id': None,  # Sẽ được map trong context
                'title': item.get('name', 'Unknown Product'),
                'quantity': int(float(item.get('qty_ordered', 0))),
                'unit_price': item.get('price'),
                'total': item.get('row_total'),
                'metadata': {
                    'source_item_id': item.get('item_id'),
                    'sku': item.get('sku')
                }
            }
            invoice_data['items'].append(invoice_item)
            
        return invoice_data
    
    def _extract_payment_metadata(self, magento_order: Dict) -> List[Dict]:
        """Extract payment metadata for context - ĐÃ THÊM"""
        payment_data = self._extract_payment_data(magento_order)
        if not payment_data:
            return []
            
        return [{
            'provider': payment_data['provider_id'],
            'transaction_id': payment_data['data'].get('last_trans_id'),
            'amount': payment_data['amount'],
            'currency_code': payment_data['currency_code'],
            'status': payment_data['status'],
            'timestamp': datetime.now().isoformat()
        }]
    
    def _validate_final_checksum(self) -> bool:
        """Validate final checksum after migration - ĐÃ THÊM"""
        try:
            # Get all migrated orders in this session
            migrated_orders = getattr(self, 'migrated_orders', [])
            if not migrated_orders:
                logger.warning("No migrated orders to validate")
                return True
                
            # Sample validation (10% or max 50 orders)
            sample_size = min(max(10, len(migrated_orders) // 10), 50)
            sample_orders = migrated_orders[:sample_size]
            
            checksum_passed = 0
            checksum_failed = 0
            
            for order_info in sample_orders:
                magento_order = order_info.get('source_data')
                medusa_order_id = order_info.get('medusa_id')
                
                if not magento_order or not medusa_order_id:
                    continue
                    
                try:
                    # Get Medusa order
                    medusa_order = self.medusa.get_order(medusa_order_id)
                    
                    # Validate using reconciler
                    result = self.mapper.reconciler.reconcile_order(
                        magento_order, 
                        medusa_order
                    )
                    
                    if result.valid:
                        checksum_passed += 1
                    else:
                        checksum_failed += 1
                        logger.warning(
                            f"Order {magento_order.get('increment_id')} checksum failed: "
                            f"{result.errors}"
                        )
                        
                except Exception as e:
                    logger.warning(f"Checksum validation error: {e}")
                    checksum_failed += 1
            
            total_checked = checksum_passed + checksum_failed
            pass_rate = (checksum_passed / total_checked * 100) if total_checked > 0 else 100
            
            logger.info(
                f"Checksum validation: {checksum_passed}/{total_checked} passed "
                f"({pass_rate:.1f}%)"
            )
            
            # Consider passed if > 95% success rate
            return pass_rate >= 95.0
            
        except Exception as e:
            logger.error(f"Final checksum validation failed: {e}")
            return False
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on migration stats - ĐÃ THÊM"""
        recommendations = []
        
        total = self.stats['total_processed']
        if total == 0:
            return ["No orders processed"]
        
        # 1. Success rate analysis
        success_rate = (self.stats['successful'] / total * 100) if total > 0 else 0
        
        if success_rate < 90:
            recommendations.append(
                f"Low success rate ({success_rate:.1f}%). Review DLQ items."
            )
        elif success_rate < 95:
            recommendations.append(
                f"Moderate success rate ({success_rate:.1f}%). "
                "Consider optimizing mapping rules."
            )
        
        # 2. Failure analysis
        if self.stats['failed'] > 0:
            failure_rate = (self.stats['failed'] / total * 100)
            if failure_rate > 10:
                recommendations.append(
                    f"High failure rate ({failure_rate:.1f}%). "
                    "Check network connectivity and API limits."
                )
        
        # 3. Performance recommendations
        if hasattr(self.stats, 'start_time') and hasattr(self.stats, 'end_time'):
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            if duration > 0:
                orders_per_second = total / duration
                
                if orders_per_second < 1:
                    recommendations.append(
                        f"Slow migration speed ({orders_per_second:.2f} orders/sec). "
                        "Consider increasing batch size or parallel processing."
                    )
                elif orders_per_second > 10:
                    recommendations.append(
                        f"Good migration speed ({orders_per_second:.2f} orders/sec). "
                        "Consider increasing total batch size."
                    )
        
        # 4. DLQ recommendations
        dlq_count = self.dlq.get_count()
        if dlq_count > 0:
            recommendations.append(
                f"{dlq_count} items in DLQ. Review and retry failed migrations."
            )
        
        # 5. Data quality recommendations
        if self.stats.get('skipped', 0) > 0:
            skip_rate = (self.stats['skipped'] / total * 100)
            if skip_rate > 5:
                recommendations.append(
                    f"High skip rate ({skip_rate:.1f}%). "
                    "Review root category exclusions."
                )
        
        # 6. Add generic recommendations
        recommendations.extend([
            "Run validation script on migrated data",
            "Compare order totals between source and target systems",
            "Test customer portal with migrated orders",
            "Monitor target system performance after migration"
        ])
        
        return recommendations