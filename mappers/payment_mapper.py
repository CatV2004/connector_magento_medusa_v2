from typing import Dict, Any, Optional
from mappers.base_mapper import BaseMapper
from utils.logger import logger

class PaymentMapper(BaseMapper):
    """Mapper cho Magento Payment → Medusa Payment (metadata only)"""
    
    def __init__(self, mapping_config: Dict[str, Any], **kwargs):
        super().__init__(mapping_config, **kwargs)
        self.METHOD_MAP = mapping_config.get('method_mapping', {})
        self.metadata_only = mapping_config.get('metadata_only', True)
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        # Skip nếu chỉ cần metadata
        if self.metadata_only:
            return self._map_as_metadata(source_data, context)
            
        # Base mapping cho payment creation
        result = super().map(source_data, context)
        
        # Apply payment transformations
        result = self._apply_payment_transformations(result, source_data, context)
        
        return result
    
    def _map_as_metadata(self, source_data: Dict, context: Dict) -> Dict:
        """Map payment chỉ như metadata để lưu trong order"""
        
        # 1. Map order_id
        magento_order_id = source_data.get('order_id')
        order_mapping = context.get('order_id_mapping', {})
        medusa_order_id = order_mapping.get(str(magento_order_id)) if magento_order_id else None
        
        # 2. Map payment method
        magento_method = source_data.get('method', '')
        payment_method = self.METHOD_MAP.get(magento_method, 'manual')
        
        # 3. Tạo metadata object
        metadata = {
            'provider': payment_method,
            'transaction_id': source_data.get('last_trans_id'),
            'amount': source_data.get('amount_ordered'),
            'currency_code': source_data.get('currency_code'),
            'data': {
                'source_payment_id': source_data.get('entity_id'),
                'method': magento_method,
                'cc_last4': source_data.get('cc_last4'),
                'cc_type': source_data.get('cc_type'),
                'parent_id': source_data.get('parent_id')
            },
            'timestamps': {
                'created_at': source_data.get('created_at'),
                'updated_at': source_data.get('updated_at')
            }
        }
        
        # 4. Add vào context để order sử dụng
        if 'payment_metadata' not in context:
            context['payment_metadata'] = []
        context['payment_metadata'].append(metadata)
        
        # Return empty dict vì không tạo payment record
        return {}
    
    def _apply_payment_transformations(self, result: Dict, source: Dict, context: Dict) -> Dict:
        """Transformations cho payment creation (nếu cần tạo payment record)"""
        
        # 1. Map payment method
        if 'provider_id' in result:
            magento_method = source.get('method', '')
            result['provider_id'] = self.METHOD_MAP.get(magento_method, 'manual')
            
        # 2. Map order_id
        if 'order_id' in result:
            magento_order_id = source.get('order_id')
            if magento_order_id:
                order_mapping = context.get('order_id_mapping', {})
                medusa_order_id = order_mapping.get(str(magento_order_id))
                if medusa_order_id:
                    result['order_id'] = medusa_order_id
                    
        # 3. Set payment status
        if 'status' not in result:
            amount_ordered = self._parse_decimal(source.get('amount_ordered', 0))
            amount_paid = self._parse_decimal(source.get('amount_paid', 0))
            
            if amount_paid >= amount_ordered:
                result['status'] = 'captured'
            elif amount_paid > 0:
                result['status'] = 'partially_captured'
            else:
                result['status'] = 'pending'
                
        # 4. Add metadata
        result['data'] = result.get('data', {})
        result['data'].update({
            'source_payment_id': source.get('entity_id'),
            'method': source.get('method'),
            'cc_last4': source.get('cc_last4'),
            'cc_type': source.get('cc_type'),
            'parent_id': source.get('parent_id')
        })
        
        return result
    
    def _parse_decimal(self, value):
        from decimal import Decimal
        try:
            return Decimal(str(value))
        except:
            return Decimal('0.00')