from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from utils.logger import logger
from services.order_sync_service import OrderSyncService
from services.payment_sync_service import PaymentSyncService

class UnifiedMigrationService:
    """Service thống nhất để migrate orders, invoices, payments"""
    
    def __init__(self, magento, medusa):
        self.order_service = OrderSyncService(magento, medusa)
        self.payment_service = PaymentSyncService(magento, medusa)
        
        self.migration_tracking = {}
        
    def migrate_full_batch(self, since_date: datetime = None) -> Dict:
        """
        full migration workflow:
        1. Migrate orders
        2. Migrate invoices cho orders đã migrated
        3. Migrate payments
        4. Validate consistency
        """
        logger.info("=" * 60)
        logger.info("STARTING UNIFIED MIGRATION")
        logger.info("=" * 60)
        
        overall_stats = {
            'start_time': datetime.now(),
            'phases': {},
            'success': False
        }
        
        try:
            # PHASE 1: ORDER MIGRATION
            logger.info("\n[PHASE 1] Order Migration")
            logger.info("-" * 40)
            
            order_result = self.order_service.sync_orders_delta(since_date)
            overall_stats['phases']['orders'] = order_result
            
            # Get order_id mapping từ order service
            order_mapping = self.order_service.get_id_mapping()
            
            if not order_mapping:
                logger.error("No orders migrated, stopping migration")
                return overall_stats
            
            # PHASE 2: INVOICE MIGRATION
            logger.info("\n[PHASE 2] Invoice & Payment Migration")
            logger.info("-" * 40)
            
            payment_result = self.payment_service.sync_invoices_for_orders(order_mapping)
            overall_stats['phases']['invoices_payments'] = payment_result
            
            # PHASE 3: VALIDATION
            logger.info("\n[PHASE 3] Validation & Reconciliation")
            logger.info("-" * 40)
            
            validation_result = self._validate_migration(order_mapping)
            overall_stats['phases']['validation'] = validation_result
            
            # FINAL REPORT
            overall_stats['end_time'] = datetime.now()
            overall_stats['success'] = True
            overall_stats['total_duration'] = (
                overall_stats['end_time'] - overall_stats['start_time']
            ).total_seconds()
            
            self._generate_final_report(overall_stats)
            
            return overall_stats
            
        except Exception as e:
            logger.error(f"Unified migration failed: {e}")
            overall_stats['error'] = str(e)
            overall_stats['end_time'] = datetime.now()
            return overall_stats
    
    def _validate_migration(self, order_mapping: Dict) -> Dict:
        """Validate consistency sau migration"""
        
        validation_results = {
            'order_count': len(order_mapping),
            'validated_orders': 0,
            'invoices_found': 0,
            'payments_found': 0,
            'issues': []
        }
        
        # Sample validation: check 10% of orders
        sample_size = max(1, len(order_mapping) // 10)
        sample_orders = list(order_mapping.items())[:sample_size]
        
        for magento_order_id, medusa_order_id in sample_orders:
            try:
                # 1. Get order từ cả 2 systems
                magento_order = self.order_service.magento.get_order(magento_order_id)
                medusa_order = self.order_service.medusa.get_order(medusa_order_id)
                
                # 2. Validate order totals
                total_valid = self._validate_order_totals(magento_order, medusa_order)
                
                # 3. Check invoices
                invoice_count = self._check_invoice_count(magento_order_id, medusa_order_id)
                
                # 4. Check payments
                payment_count = self._check_payment_count(magento_order_id, medusa_order_id)
                
                validation_results['validated_orders'] += 1
                validation_results['invoices_found'] += invoice_count
                validation_results['payments_found'] += payment_count
                
                if not total_valid:
                    validation_results['issues'].append({
                        'order_id': medusa_order_id,
                        'issue': 'total_mismatch'
                    })
                    
            except Exception as e:
                logger.warning(f"Validation failed for order {magento_order_id}: {e}")
                validation_results['issues'].append({
                    'order_id': medusa_order_id,
                    'issue': 'validation_error',
                    'error': str(e)
                })
        
        return validation_results
    
    def _validate_order_totals(self, magento_order: Dict, medusa_order: Dict) -> bool:
        """Validate order total consistency"""
        from decimal import Decimal
        
        magento_total = Decimal(str(magento_order.get('grand_total', 0)))
        medusa_total = Decimal(str(medusa_order.get('total', 0)))
        
        tolerance = Decimal('0.01')
        discrepancy = abs(magento_total - medusa_total)
        
        if discrepancy > tolerance:
            logger.warning(
                f"Order total discrepancy: "
                f"Magento {magento_total} vs Medusa {medusa_total}"
            )
            return False
            
        return True
    
    def _check_invoice_count(self, magento_order_id: str, medusa_order_id: str) -> int:
        """Check số lượng invoices"""
        try:
            magento_invoices = self.payment_service.magento.get_order_invoices(magento_order_id)
            medusa_invoices = self.payment_service.medusa.get_order_invoices(medusa_order_id)
            
            if len(magento_invoices) != len(medusa_invoices):
                logger.warning(
                    f"Invoice count mismatch for order {magento_order_id}: "
                    f"Magento {len(magento_invoices)} vs Medusa {len(medusa_invoices)}"
                )
                
            return len(medusa_invoices)
            
        except Exception as e:
            logger.debug(f"Error checking invoices: {e}")
            return 0
    
    def _check_payment_count(self, magento_order_id: str, medusa_order_id: str) -> int:
        """Check số lượng payments"""
        try:
            # magento_payments = self.payment_service.magento.get_order_payments(magento_order_id)
            medusa_payments = self.payment_service.medusa.get_order_payments(medusa_order_id)
            
            return len(medusa_payments)
            
        except Exception as e:
            logger.debug(f"Error checking payments: {e}")
            return 0
    
    def _generate_final_report(self, stats: Dict):
        """Generate final migration report"""
        
        logger.info("\n" + "=" * 60)
        logger.info("MIGRATION COMPLETE - FINAL REPORT")
        logger.info("=" * 60)
        
        # Order stats
        order_stats = stats['phases'].get('orders', {}).get('summary', {})
        logger.info(f"\n📦 ORDERS:")
        logger.info(f"  • Processed: {order_stats.get('total_processed', 0)}")
        logger.info(f"  • Successful: {order_stats.get('successful', 0)}")
        logger.info(f"  • Failed: {order_stats.get('failed', 0)}")
        logger.info(f"  • Success Rate: {order_stats.get('success_rate', 0):.1f}%")
        
        # Invoice & Payment stats
        payment_stats = stats['phases'].get('invoices_payments', {}).get('stats', {})
        logger.info(f"\n🧾 INVOICES & PAYMENTS:")
        logger.info(f"  • Invoices Created: {payment_stats.get('invoices_created', 0)}")
        logger.info(f"  • Payments Processed: {payment_stats.get('payments_processed', 0)}")
        logger.info(f"  • Payments as Metadata: {payment_stats.get('payments_as_metadata', 0)}")
        
        # Validation stats
        validation_stats = stats['phases'].get('validation', {})
        logger.info(f"\n✅ VALIDATION:")
        logger.info(f"  • Orders Validated: {validation_stats.get('validated_orders', 0)}")
        logger.info(f"  • Issues Found: {len(validation_stats.get('issues', []))}")
        
        # Timing
        duration = stats.get('total_duration', 0)
        logger.info(f"\n⏱️ TIMING:")
        logger.info(f"  • Start Time: {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  • End Time: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  • Total Duration: {duration:.2f} seconds")
        
        if duration > 0:
            orders_per_sec = order_stats.get('total_processed', 0) / duration
            logger.info(f"  • Orders per Second: {orders_per_sec:.2f}")
        
        # Recommendations
        logger.info(f"\n📋 RECOMMENDATIONS:")
        
        if order_stats.get('failed', 0) > 0:
            logger.info(f"  • Check DLQ for {order_stats['failed']} failed orders")
            
        if validation_stats.get('issues'):
            logger.info(f"  • Review {len(validation_stats['issues'])} validation issues")
            
        logger.info("\n" + "=" * 60)