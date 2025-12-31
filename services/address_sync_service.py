"""
Address Sync Service - Handles address-only synchronization.
Useful for syncing addresses independently of customers.
"""
from typing import Dict, List, Optional
from utils.logger import logger
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from mappers.address_mapper import AddressMapper
from core.mapping.mapping_factory import MappingFactory
from pathlib import Path


class AddressSyncService:
    """Service for syncing addresses independently"""
    
    def __init__(
        self,
        magento: MagentoConnector,
        medusa: MedusaConnector,
        config_path: Path = Path("config/mapping")
    ):
        self.magento = magento
        self.medusa = medusa
        
        mapping_factory = MappingFactory(config_path)
        
        self.address_mapper = AddressMapper(
            mapping_config=mapping_factory.get("address"),
            source_connector=magento,
            target_connector=medusa
        )
        
        logger.info("AddressSyncService initialized")
    
    # def sync_customer_addresses(self, customer_id: str, magento_customer_id: int) -> Dict:
    #     logger.info(f"Syncing addresses for customer: Medusa={customer_id}, Magento={magento_customer_id}")
        
    #     try:
    #         addresses = self.magento.get_customer_addresses(magento_customer_id)
            
    #         if not addresses:
    #             logger.info(f"No addresses found for customer {magento_customer_id}")
    #             return {
    #                 'customer_id': customer_id,
    #                 'addresses_processed': 0,
    #                 'status': 'no_addresses'
    #             }
            
    #         # Process each address
    #         results = self._process_addresses(addresses, customer_id)
            
    #         logger.info(
    #             f"Address sync completed: {results['created']} created, "
    #             f"{results['updated']} updated, {results['failed']} failed"
    #         )
            
    #         return {
    #             'customer_id': customer_id,
    #             'addresses_processed': len(addresses),
    #             'results': results,
    #             'status': 'completed'
    #         }
            
    #     except Exception as e:
    #         logger.error(f"Failed to sync addresses for customer {customer_id}: {e}")
    #         raise
    
    def sync_single_address(self, customer_id: str, magento_address: Dict) -> Dict:
        logger.info(f"Syncing single address for customer {customer_id}")
        
        try:
            # Map address
            address_result = self.address_mapper.map(
                magento_address,
                context={'customer_id': customer_id}
            )
            
            if address_result.validation_errors:
                raise ValueError(f"Address validation failed: {address_result.validation_errors}")
            
            # Create/update in Medusa
            response = self.medusa.add_address(customer_id, address_result.address_data)
            
            return {
                'customer_id': customer_id,
                'address_id': response.get('id'),
                'address_type': address_result.address_type.value,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Failed to sync address: {e}")
            raise
    
    def _process_addresses(self, addresses: List[Dict], customer_id: str) -> Dict:
        results = {
            'created': 0,
            'updated': 0,
            'failed': 0,
            'total': len(addresses)
        }
        
        for address in addresses:
            try:
                existing_address_id = self._find_existing_address(customer_id, address)
                
                if existing_address_id:
                    self._update_address(customer_id, existing_address_id, address)
                    results['updated'] += 1
                    logger.debug(f"  ↻ Updated address {existing_address_id}")
                else:
                    self._create_address(customer_id, address)
                    results['created'] += 1
                    logger.debug(f"  ✓ Created new address")
                    
            except Exception as e:
                logger.error(f"Failed to process address: {e}")
                results['failed'] += 1
        
        return results
    
    def _find_existing_address(
        self,
        customer_id: str,
        magento_address: Dict
    ) -> Optional[str]:
        magento_address_id = str(magento_address.get("id"))
        if not magento_address_id:
            return None

        customer = self.medusa.get_customer(customer_id)
        addresses = customer.get("customer", {}).get("addresses", [])

        for address in addresses:
            metadata = address.get("metadata") or {}
            if str(metadata.get("magento_address_id")) == magento_address_id:
                return address["id"]

        def normalize(val: Optional[str]) -> str:
            return (val or "").strip().lower()

        magento_street = normalize(" ".join(magento_address.get("street", [])))
        magento_city = normalize(magento_address.get("city"))
        magento_postcode = normalize(magento_address.get("postcode"))
        magento_country = normalize(magento_address.get("country_id"))

        for address in addresses:
            street = normalize(
                f"{address.get('address_1', '')} {address.get('address_2', '')}"
            )
            city = normalize(address.get("city"))
            postcode = normalize(address.get("postal_code"))
            country = normalize(address.get("country_code"))

            if (
                street == magento_street
                and city == magento_city
                and postcode == magento_postcode
                and country == magento_country
            ):
                return address["id"]

        return None
    
    def _create_address(self, customer_id: str, magento_address: Dict):
        address_result = self.address_mapper.map(
            magento_address,
            context={"customer_id": customer_id}
        )

        if address_result.validation_errors:
            raise ValueError(
                f"Cannot create invalid address: {address_result.validation_errors}"
            )

        return self.medusa.add_address(
            customer_id,
            address_result.address_data
        )
    
    def _update_address(self, customer_id: str, address_id: str, magento_address: Dict):
        address_result = self.address_mapper.map(
            magento_address,
            context={"customer_id": customer_id}
        )

        if address_result.validation_errors:
            raise ValueError(
                f"Cannot update to invalid address {address_id}: {address_result.validation_errors}"
            )

        payload = {
            "address_name": address_result.address_data.get("address_name"),
            "company": address_result.address_data.get("company"),
            "first_name": address_result.address_data.get("first_name"),
            "last_name": address_result.address_data.get("last_name"),
            "address_1": address_result.address_data.get("address_1"),
            "address_2": address_result.address_data.get("address_2"),
            "city": address_result.address_data.get("city"),
            "country_code": address_result.address_data.get("country_code"),
            "province": address_result.address_data.get("province"),
            "postal_code": address_result.address_data.get("postal_code"),
            "phone": address_result.address_data.get("phone"),
            "metadata": address_result.address_data.get("metadata"),
            "is_default_shipping": address_result.address_data.get("is_default_shipping"),
            "is_default_billing": address_result.address_data.get("is_default_billing"),
        }

        payload = {k: v for k, v in payload.items() if v is not None}

        if not payload:
            logger.info(f"No address changes detected for address {address_id}")
            return None

        return self.medusa.update_address(
            customer_id=customer_id,
            address_id=address_id,
            address_data=payload
        )