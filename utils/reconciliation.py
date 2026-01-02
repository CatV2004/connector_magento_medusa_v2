from decimal import Decimal
from typing import Dict, List, Tuple
from dataclasses import dataclass

@dataclass
class ReconciliationResult:
    valid: bool
    errors: List[str]
    discrepancies: Dict[str, Decimal]
    source_total: Decimal
    target_total: Decimal

class MonetaryReconciler:
    """Utility cho monetary consistency checks"""
    
    def reconcile_order(self, magento_order: Dict, medusa_order: Dict) -> ReconciliationResult:
        """Reconcile giữa Magento và Medusa order totals"""
        
        errors = []
        discrepancies = {}
        
        # 1. Compare grand totals
        magento_total = self._safe_decimal(magento_order.get('grand_total'))
        medusa_total = self._safe_decimal(medusa_order.get('total'))
        
        if abs(magento_total - medusa_total) > Decimal('0.01'):
            discrepancies['grand_total'] = medusa_total - magento_total
            errors.append(
                f"Grand total mismatch: Magento {magento_total} vs Medusa {medusa_total}"
            )
        
        # 2. Compare tax
        magento_tax = self._safe_decimal(magento_order.get('tax_amount'))
        medusa_tax = self._safe_decimal(medusa_order.get('tax_total'))
        
        if abs(magento_tax - medusa_tax) > Decimal('0.01'):
            discrepancies['tax'] = medusa_tax - magento_tax
            errors.append(f"Tax mismatch: Magento {magento_tax} vs Medusa {medusa_tax}")
        
        # 3. Item count validation
        magento_items = len(magento_order.get('items', []))
        medusa_items = len(medusa_order.get('items', []))
        
        if magento_items != medusa_items:
            errors.append(
                f"Item count mismatch: Magento {magento_items} vs Medusa {medusa_items}"
            )
        
        return ReconciliationResult(
            valid=len(errors) == 0,
            errors=errors,
            discrepancies=discrepancies,
            source_total=magento_total,
            target_total=medusa_total
        )
    
    def batch_reconciliation(self, orders_mapping: List[Tuple[Dict, Dict]]) -> Dict:
        """Bulk reconciliation cho batch orders"""
        
        results = {
            'total_orders': len(orders_mapping),
            'passed': 0,
            'failed': 0,
            'total_discrepancy': Decimal('0.00'),
            'detailed_results': []
        }
        
        for magento_order, medusa_order in orders_mapping:
            recon_result = self.reconcile_order(magento_order, medusa_order)
            
            if recon_result.valid:
                results['passed'] += 1
            else:
                results['failed'] += 1
                # Sum absolute discrepancies
                for disc in recon_result.discrepancies.values():
                    results['total_discrepancy'] += abs(disc)
            
            results['detailed_results'].append({
                'order_number': magento_order.get('increment_id'),
                'valid': recon_result.valid,
                'errors': recon_result.errors,
                'discrepancies': {
                    k: str(v) for k, v in recon_result.discrepancies.items()
                }
            })
        
        return results
    
    def _safe_decimal(self, value) -> Decimal:
        """Safe decimal conversion"""
        try:
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value)).quantize(Decimal('0.01'))
        except:
            return Decimal('0.00')