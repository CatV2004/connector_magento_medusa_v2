from typing import Dict, List, Optional
from datetime import datetime
from utils.logger import logger
from mappers.invoice_mapper import InvoiceMapper
from mappers.payment_mapper import PaymentMapper
from core.dlq_handler import DLQHandler
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from core.mapping.mapping_factory import MappingFactory

class PaymentSyncService:
    """Service xử lý invoice và payment migration"""
    
    def __init__(self, magento: MagentoConnector, medusa: MedusaConnector):
        self.magento = magento
        self.medusa = medusa
        
        # Load mappers
        self.invoice_mapper = InvoiceMapper(
            MappingFactory().get("invoice")
        )
        self.payment_mapper = PaymentMapper(
            MappingFactory().get("payment")
        )
        
        self.dlq = DLQHandler('payments_invoices')
        self.stats = {
            'invoices_processed': 0,
            'invoices_created': 0,
            'payments_processed': 0,
            'payments_as_metadata': 0,
            'failed': 0
        }
    
    def sync_invoices_for_orders(self, order_mapping: Dict[str, str]) -> Dict:
        logger.info(f"Starting invoice sync for {len(order_mapping)} orders")
        
        results = []
        
        for magento_order_id, medusa_order_id in order_mapping.items():
            try:
                # 1. Lấy invoices từ Magento cho order này
                magento_invoices = self.magento.get_order_invoices(magento_order_id)
                
                if not magento_invoices:
                    continue
                    
                # 2. Process từng invoice
                for magento_invoice in magento_invoices:
                    invoice_result = self._sync_single_invoice(
                        magento_invoice, 
                        medusa_order_id,
                        order_mapping
                    )
                    results.append(invoice_result)
                    
            except Exception as e:
                logger.error(f"Failed to sync invoices for order {magento_order_id}: {e}")
                self.stats['failed'] += 1
                
        logger.info(f"Invoice sync completed: {self.stats}")
        return {
            'stats': self.stats.copy(),
            'results': results
        }
    
    def _sync_single_invoice(self, magento_invoice: Dict, 
                            medusa_order_id: str,
                            order_mapping: Dict) -> Dict:
        
        invoice_number = magento_invoice.get('increment_id', 'unknown')
        logger.info(f"Syncing invoice {invoice_number}")
        
        try:
            # 1. Prepare context
            context = {
                'order_id_mapping': order_mapping,
                'product_id_mapping': self._load_product_mapping(),
                'operation': 'create_invoice'
            }
            
            # 2. Map invoice data
            medusa_invoice_data = self.invoice_mapper.map(magento_invoice, context)
            
            # 3. Ensure order_id is set
            medusa_invoice_data['order_id'] = medusa_order_id
            
            # 4. Check if invoice already exists
            existing_invoice = self._find_existing_invoice(
                invoice_number, 
                medusa_order_id
            )
            
            if existing_invoice:
                logger.info(f"Invoice {invoice_number} already exists, skipping")
                return {'status': 'skipped', 'invoice_id': existing_invoice['id']}
            
            # 5. Create invoice in Medusa
            created_invoice = self.medusa.create_invoice(medusa_invoice_data)
            
            # 6. Sync payments liên quan đến invoice này
            self._sync_payments_for_invoice(magento_invoice, medusa_order_id, context)
            
            self.stats['invoices_processed'] += 1
            self.stats['invoices_created'] += 1
            
            return {
                'status': 'success',
                'invoice_id': created_invoice.get('id'),
                'invoice_number': invoice_number,
                'amount': medusa_invoice_data.get('amount')
            }
            
        except Exception as e:
            logger.error(f"Failed to sync invoice {invoice_number}: {e}")
            self.stats['failed'] += 1
            
            # Add to DLQ
            self.dlq.add_item({
                'entity_type': 'invoice',
                'source_data': magento_invoice,
                'error': str(e),
                'order_id': medusa_order_id,
                'timestamp': datetime.now().isoformat()
            })
            
            return {'status': 'failed', 'error': str(e)}
    
    def _sync_payments_for_invoice(self, magento_invoice: Dict, 
                                  medusa_order_id: str,
                                  context: Dict):
        
        invoice_id = magento_invoice.get('entity_id')
        if not invoice_id:
            return
            
        try:
            # 1. Lấy payments từ Magento cho invoice này
            magento_payments = self.magento.get_invoice_payments(invoice_id)
            
            if not magento_payments:
                return
                
            # 2. Process từng payment
            for magento_payment in magento_payments:
                self._process_payment(
                    magento_payment, 
                    medusa_order_id,
                    context
                )
                
        except Exception as e:
            logger.warning(f"Failed to sync payments for invoice {invoice_id}: {e}")
    
    def _process_payment(self, magento_payment: Dict, 
                        medusa_order_id: str,
                        context: Dict):
        
        payment_id = magento_payment.get('entity_id', 'unknown')
        logger.debug(f"Processing payment {payment_id}")
        
        try:
            # 1. Map payment data
            medusa_payment_data = self.payment_mapper.map(magento_payment, context)
            
            # 2. Check config - chỉ lưu metadata hay tạo payment record
            if self.payment_mapper.metadata_only:
                # Chỉ lấy metadata, đã được add vào context
                self.stats['payments_as_metadata'] += 1
                return
                
            # 3. Tạo payment record trong Medusa nếu cần
            if medusa_payment_data and 'provider_id' in medusa_payment_data:
                medusa_payment_data['order_id'] = medusa_order_id
                
                # Check if payment exists
                existing_payment = self._find_existing_payment(
                    magento_payment.get('last_trans_id'),
                    medusa_order_id
                )
                
                if not existing_payment:
                    self.medusa.create_payment(medusa_payment_data)
                    
            self.stats['payments_processed'] += 1
            
        except Exception as e:
            logger.warning(f"Failed to process payment {payment_id}: {e}")
    
    def _find_existing_invoice(self, invoice_number: str, order_id: str) -> Optional[Dict]:
        try:
            # Giả sử medusa có method get_order_invoices
            existing_invoices = self.medusa.get_order_invoices(order_id)
            
            for invoice in existing_invoices:
                if invoice.get('metadata', {}).get('source_increment_id') == invoice_number:
                    return invoice
                if invoice.get('invoice_number', '').endswith(invoice_number):
                    return invoice
                    
        except Exception as e:
            logger.debug(f"Error checking existing invoices: {e}")
            
        return None
    
    def _find_existing_payment(self, transaction_id: str, order_id: str) -> Optional[Dict]:
        if not transaction_id:
            return None
            
        try:
            # Giả sử medusa có method get_order_payments
            existing_payments = self.medusa.get_order_payments(order_id)
            
            for payment in existing_payments:
                if payment.get('transaction_id') == transaction_id:
                    return payment
                if payment.get('data', {}).get('source_transaction_id') == transaction_id:
                    return payment
                    
        except Exception as e:
            logger.debug(f"Error checking existing payments: {e}")
            
        return None
    
    def _load_product_mapping(self) -> Dict:
        """Load product_id mapping từ file hoặc DB"""
        # Implement based on your project's mapping storage
        return {}