from decimal import Decimal
from typing import Dict, Any, List, Optional
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from utils.logger import logger

class InvoiceMapper(BaseMapper):
    """Mapper cho Magento Invoice → Medusa Invoice"""
    
    def __init__(self, mapping_config: Dict[str, Any], **kwargs):
        super().__init__(mapping_config, **kwargs)
        self.STATE_MAP = mapping_config.get('state_mapping', {})

    def _entity_specific_pre_validate(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        errors = []
        warnings = []
        
        # 1. Check order reference
        if not source_data.get('order_id'):
            errors.append("Invoice has no order_id")
            
        # 2. Check invoice state
        state = source_data.get('state')
        valid_states = ['pending', 'paid', 'canceled', 'refunded']
        if state and state not in valid_states:
            warnings.append(f"Unknown invoice state: {state}")
            
        # 3. Check monetary consistency
        grand_total = source_data.get('grand_total')
        if grand_total:
            try:
                total = Decimal(str(grand_total))
                if total <= 0:
                    warnings.append(f"Invoice grand_total is zero or negative: {total}")
            except:
                errors.append(f"Invalid grand_total: {grand_total}")
                
        # 4. Check transaction_id for paid invoices
        if state == 'paid' and not source_data.get('transaction_id'):
            warnings.append("Paid invoice has no transaction_id")
            
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        # Validate required fields
        required_validation = self._validate_required_fields(source_data)
        if not required_validation['valid']:
            missing = required_validation['missing_required']
            raise ValueError(f"Missing required fields: {missing}")
        
        # Base mapping
        result = super().map(source_data, context)
        
        # Apply invoice-specific transformations
        result = self._apply_invoice_transformations(result, source_data, context)
        
        # Validate amount consistency
        self._validate_amount_consistency(result, source_data)
        
        return result
    
    def _apply_invoice_transformations(self, result: Dict, source: Dict, context: Dict) -> Dict:
        """Áp dụng transformations cho invoice"""
        
        # 1. Map invoice state
        if 'status' in result:
            magento_state = source.get('state', '')
            result['status'] = self.STATE_MAP.get(magento_state, 'draft')
            
        # 2. Map order_id từ context
        if 'order_id' in result:
            magento_order_id = source.get('order_id')
            if magento_order_id:
                order_mapping = context.get('order_id_mapping', {})
                medusa_order_id = order_mapping.get(str(magento_order_id))
                if medusa_order_id:
                    result['order_id'] = medusa_order_id
                else:
                    logger.warning(f"Order ID {magento_order_id} not found in mapping")
                    
        # 3. Map invoice items
        if 'items' in source and 'items' not in result:
            result['items'] = self._map_invoice_items(source['items'], context)
            
        # 4. Add metadata
        result['metadata'] = result.get('metadata', {})
        result['metadata'].update({
            'source_system': 'magento',
            'source_invoice_id': source.get('entity_id'),
            'source_increment_id': source.get('increment_id'),
            'source_order_id': source.get('order_id'),
            'invoice_date': source.get('created_at'),
            'transaction_id': source.get('transaction_id')
        })
        
        # 5. Set invoice number
        if 'invoice_number' not in result and 'increment_id' in source:
            result['invoice_number'] = f"INV-{source['increment_id']}"
            
        return result
    
    def _map_invoice_items(self, magento_items: List[Dict], context: Dict) -> List[Dict]:
        """Map invoice line items"""
        mapped_items = []
        
        for item in magento_items:
            # Map product_id
            magento_product_id = str(item.get('product_id', ''))
            product_mapping = context.get('product_id_mapping', {})
            medusa_product_id = product_mapping.get(magento_product_id)
            
            mapped_item = {
                'product_id': medusa_product_id,
                'title': item.get('name', 'Unknown Item'),
                'quantity': int(float(item.get('qty', 0))),
                'unit_price': self._parse_decimal(item.get('price', 0)),
                'total': self._parse_decimal(item.get('row_total', 0)),
                'metadata': {
                    'source_item_id': item.get('entity_id'),
                    'sku': item.get('sku'),
                    'product_type': item.get('product_type')
                }
            }
            
            mapped_items.append(mapped_item)
            
        return mapped_items
    
    def _validate_amount_consistency(self, mapped_data: Dict, source_data: Dict):
        """Validate invoice amount consistency"""
        mapped_amount = self._parse_decimal(mapped_data.get('amount', 0))
        source_amount = self._parse_decimal(source_data.get('grand_total', 0))
        
        tolerance = Decimal('0.01')
        discrepancy = abs(mapped_amount - source_amount)
        
        if discrepancy > tolerance:
            logger.warning(
                f"Invoice {source_data.get('increment_id')} amount discrepancy: "
                f"source {source_amount} vs mapped {mapped_amount}"
            )
            
            # Auto-correct nếu discrepancy nhỏ
            if discrepancy <= Decimal('1.00'):  # Nếu sai số ≤ $1
                mapped_data['amount'] = source_amount
                mapped_data['metadata']['amount_corrected'] = True
                mapped_data['metadata']['original_amount'] = str(mapped_amount)
                logger.info(f"Auto-corrected invoice amount to {source_amount}")
    
    def _parse_decimal(self, value) -> Decimal:
        """Safe decimal parsing"""
        try:
            return Decimal(str(value)).quantize(Decimal('0.01'))
        except:
            return Decimal('0.00')