from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional
from mappers.base_mapper import BaseMapper
from core.transformer import Transformer
from utils.logger import logger
from utils.reconciliation import MonetaryReconciler

class OrderMapper(BaseMapper):
    
    def __init__(self, mapping_config: Dict[str, Any], **kwargs):
        super().__init__(mapping_config, **kwargs)
        self.reconciler = MonetaryReconciler()
        self.STATUS_MAP = mapping_config.get('status_mapping', {})

    def _entity_specific_pre_validate(self, source_data: Dict[str, Any]) -> Dict[str, Any]:
        errors = []
        warnings = []
        
        # 1. Check monetary fields
        monetary_fields = ['grand_total', 'subtotal', 'tax_amount', 'shipping_amount']
        for field in monetary_fields:
            if field in source_data:
                try:
                    value = source_data[field]
                    if value is not None:
                        Decimal(str(value))
                except:
                    errors.append(f"Invalid decimal value for {field}: {source_data[field]}")
        
        # 2. Check order status
        status = source_data.get('status')
        if status and status not in self.STATUS_MAP:
            warnings.append(f"Unknown order status: {status}")
            
        # 3. Check customer reference
        customer_id = source_data.get('customer_id')
        customer_email = source_data.get('customer_email')
        if not customer_id and not customer_email:
            warnings.append("Order has no customer_id or customer_email")
            
        # 4. Check line items
        items = source_data.get('items', [])
        if not items:
            errors.append("Order has no line items")
        else:
            for i, item in enumerate(items):
                if not item.get('product_id'):
                    warnings.append(f"Item {i} has no product_id")
                if not item.get('sku'):
                    warnings.append(f"Item {i} has no sku")
                    
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }
        
    def map(self, source_data: Dict[str, Any], context: Optional[Dict] = None) -> Dict[str, Any]:
        # Pre-validation
        pre_validation = self._pre_validate(source_data)

        if not pre_validation['valid']:
            raise ValueError(f"Pre-validation failed: {pre_validation['errors']}")
            
        if pre_validation['warnings']:
            logger.warning(f"Order {source_data.get('increment_id')} warnings: {pre_validation['warnings']}")
        
        # Base mapping
        result = super().map(source_data, context)
        
        # Apply order-specific transformations
        result = self._apply_order_transformations(result, source_data, context)
        
        # Post-mapping validation (checksum)
        validation_result = self._validate_monetary_consistency(result, source_data)
        if not validation_result['valid']:
            logger.error(f"Order {source_data.get('increment_id')} failed checksum: {validation_result['errors']}")
            raise ValueError(f"Monetary inconsistency: {validation_result['errors']}")
            
        return result
    
    def _apply_order_transformations(self, result: Dict, source: Dict, context: Dict) -> Dict:
        """Áp dụng transformations phức tạp cho order"""
        
        # 1. Map order status
        if 'status' in result:
            magento_status = source.get('status', '')
            result['status'] = self.STATUS_MAP.get(magento_status, 'pending')
            
        # 2. Map line items với product_id resolution
        if 'items' in source and 'items' in result:
            result['items'] = self._map_order_items(source['items'], context)
            
        # 3. Tính toán total nếu cần (backup calculation)
        if 'total' not in result or not result['total']:
            result['total'] = self._calculate_total(source)
            
        # 4. Thêm metadata cho tracking
        result['metadata'] = result.get('metadata', {})
        result['metadata'].update({
            'source_system': 'magento',
            'source_order_id': source.get('entity_id'),
            'source_increment_id': source.get('increment_id'),
            'migration_batch_id': context.get('batch_id', ''),
            'checksum_validated': True
        })
        
        return result
    
    def _map_order_items(self, magento_items: List[Dict], context: Dict) -> List[Dict]:
        """Map Magento line items sang Medusa format"""
        mapped_items = []
        
        for item in magento_items:
            # Resolve product_id mapping
            magento_product_id = str(item.get('product_id'))
            medusa_product_id = context.get('product_id_mapping', {}).get(magento_product_id)
            
            if not medusa_product_id:
                logger.warning(f"Product ID {magento_product_id} not mapped. Using variant_id placeholder.")
                # Có thể tạo placeholder product hoặc skip item
                continue
                
            mapped_item = {
                'variant_id': medusa_product_id,
                'title': item.get('name', 'Unknown Product'),
                'quantity': int(float(item.get('qty_ordered', 0))),
                'unit_price': self._parse_decimal(item.get('price', 0)),
                'metadata': {
                    'source_item_id': item.get('item_id'),
                    'sku': item.get('sku'),
                    'product_type': item.get('product_type')
                }
            }
            
            # Tính item total
            mapped_item['total'] = mapped_item['unit_price'] * mapped_item['quantity']
            
            mapped_items.append(mapped_item)
            
        return mapped_items
    
    def _calculate_total(self, source: Dict) -> Decimal:
        """Tính toán total từ các components - dùng cho verification"""
        subtotal = self._parse_decimal(source.get('subtotal', 0))
        tax = self._parse_decimal(source.get('tax_amount', 0))
        shipping = self._parse_decimal(source.get('shipping_amount', 0))
        discount = self._parse_decimal(source.get('discount_amount', 0))
        
        return (subtotal + tax + shipping - discount).quantize(Decimal('0.01'), ROUND_HALF_UP)
    
    def _validate_monetary_consistency(self, mapped_data: Dict, source_data: Dict) -> Dict:
        """CHECKSUM validation: sum(line_items) + tax + shipping = total"""
        
        # Lấy các giá trị từ mapped data
        items_total = sum(item.get('total', 0) for item in mapped_data.get('items', []))
        tax_total = self._parse_decimal(mapped_data.get('tax_total', 0))
        shipping_total = self._parse_decimal(mapped_data.get('shipping_total', 0))
        discount_total = self._parse_decimal(mapped_data.get('discount_total', 0))
        
        calculated_total = items_total + tax_total + shipping_total - discount_total
        declared_total = self._parse_decimal(mapped_data.get('total', 0))
        
        # Cho phép sai số nhỏ do rounding
        tolerance = Decimal('0.01')
        discrepancy = abs(calculated_total - declared_total)
        
        errors = []
        if discrepancy > tolerance:
            errors.append(
                f"Total mismatch: calculated {calculated_total} vs declared {declared_total}"
            )
            
        # Cross-check với source data
        source_total = self._parse_decimal(source_data.get('grand_total', 0))
        if abs(declared_total - source_total) > tolerance:
            errors.append(
                f"Source total mismatch: source {source_total} vs mapped {declared_total}"
            )
            
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'calculated_total': float(calculated_total),
            'declared_total': float(declared_total),
            'discrepancy': float(discrepancy)
        }
    
    def _parse_decimal(self, value) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value)).quantize(Decimal('0.01'), ROUND_HALF_UP)
        except:
            return Decimal('0.00')

    def get_id_mapping(self) -> Dict[str, str]:
        id_mapping = {}
        mappings = self.mapping_config.get('id_mapping', [])
        
        for entry in mappings:
            magento_id = str(entry.get('magento_order_id'))
            medusa_id = str(entry.get('medusa_order_id'))
            if magento_id and medusa_id:
                id_mapping[magento_id] = medusa_id
                
        return id_mapping