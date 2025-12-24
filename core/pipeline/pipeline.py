from typing import Dict, Any
from connectors.magento.magento_connector import MagentoConnector
from connectors.medusa.medusa_connector import MedusaConnector
from core.pipeline.sync_pipeline import SyncPipeline
from core.pipeline.async_sync_pipeline import AsyncSyncPipeline

def create_pipeline(
    magento: MagentoConnector,
    medusa: MedusaConnector,
    pipeline_type: str = "default",
    config: Dict[str, Any] = None
) -> SyncPipeline:
    """
    Create a sync pipeline
    
    Args:
        magento: Magento connector
        medusa: Medusa connector
        pipeline_type: Type of pipeline ("default", "async", "custom")
        config: Pipeline configuration
        
    Returns:
        SyncPipeline instance
    """
    if pipeline_type == "async":
        pipeline = AsyncSyncPipeline(magento, medusa, config=config)
    else:
        pipeline = SyncPipeline(magento, medusa, config=config)
    
    if pipeline_type == "default":
        pipeline.build_default_pipeline()
    
    return pipeline