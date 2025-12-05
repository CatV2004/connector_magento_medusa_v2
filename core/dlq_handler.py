import json
import csv
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
from utils.logger import logger


class DLQHandler:
    """Dead Letter Queue handler for failed sync items"""
    
    def __init__(self, entity_type: str, dlq_dir: str = "dlq"):
        """
        Args:
            entity_type: Type of entity (products, categories, customers)
            dlq_dir: Directory for DLQ files
        """
        self.entity_type = entity_type
        self.dlq_dir = Path(dlq_dir)
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        
        # Current batch
        self.current_batch: List[Dict] = []
        self.batch_size = 100
        
    def add_item(self, item: Dict[str, Any]):
        """
        Add item to DLQ
        
        Args:
            item: Item data including source data and error info
        """
        # Add metadata
        item_with_meta = {
            **item,
            'dlq_timestamp': datetime.now().isoformat(),
            'entity_type': self.entity_type,
            'batch_id': datetime.now().strftime('%Y%m%d_%H%M%S')
        }
        
        self.current_batch.append(item_with_meta)
        
        # Write to file if batch size reached
        if len(self.current_batch) >= self.batch_size:
            self._flush_batch()
            
    def _flush_batch(self):
        """Write current batch to file"""
        if not self.current_batch:
            return
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.entity_type}_{timestamp}.json"
        filepath = self.dlq_dir / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.current_batch, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Written {len(self.current_batch)} items to DLQ: {filepath}")
            self.current_batch = []
            
        except Exception as e:
            logger.error(f"Failed to write DLQ file: {e}")
            
    def get_count(self) -> int:
        """Get count of items in DLQ for this entity type"""
        count = len(self.current_batch)
        
        # Count items in existing files
        pattern = f"{self.entity_type}_*.json"
        for filepath in self.dlq_dir.glob(pattern):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    count += len(data)
            except Exception as e:
                logger.warning(f"Failed to read DLQ file {filepath}: {e}")
                
        return count
        
    def export_to_csv(self, output_file: str = None):
        """Export DLQ items to CSV for manual review"""
        if not output_file:
            output_file = f"{self.entity_type}_dlq_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
        output_path = self.dlq_dir / output_file
        
        # Collect all items
        all_items = self.current_batch.copy()
        pattern = f"{self.entity_type}_*.json"
        
        for filepath in self.dlq_dir.glob(pattern):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_items.extend(data)
            except Exception as e:
                logger.warning(f"Failed to read DLQ file {filepath}: {e}")
                
        if not all_items:
            logger.info("No items in DLQ to export")
            return
            
        # Flatten nested structures for CSV
        flattened_items = []
        for item in all_items:
            flat_item = {
                'entity_type': item.get('entity_type'),
                'dlq_timestamp': item.get('dlq_timestamp'),
                'operation': item.get('operation'),
                'error': item.get('error', ''),
            }
            
            # Add source data fields
            source_data = item.get('source_data', {})
            for key, value in source_data.items():
                if isinstance(value, (str, int, float, bool)):
                    flat_item[f'src_{key}'] = value
                else:
                    flat_item[f'src_{key}'] = str(value)
                    
            flattened_items.append(flat_item)
            
        # Write CSV
        try:
            if flattened_items:
                fieldnames = flattened_items[0].keys()
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(flattened_items)
                    
                logger.info(f"Exported {len(flattened_items)} DLQ items to {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export DLQ to CSV: {e}")
            
    def retry_failed_items(self, retry_callback):
        """
        Retry failed items from DLQ
        
        Args:
            retry_callback: Function to call for retrying each item
        """
        pattern = f"{self.entity_type}_*.json"
        retried_count = 0
        successful_count = 0
        
        for filepath in self.dlq_dir.glob(pattern):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    items = json.load(f)
                    
                for item in items:
                    retried_count += 1
                    try:
                        retry_callback(item)
                        successful_count += 1
                    except Exception as e:
                        logger.error(f"Retry failed for item: {e}")
                        
                # Archive processed file
                archive_path = self.dlq_dir / 'archive' / filepath.name
                archive_path.parent.mkdir(exist_ok=True)
                filepath.rename(archive_path)
                
            except Exception as e:
                logger.error(f"Failed to process DLQ file {filepath}: {e}")
                
        logger.info(f"Retried {retried_count} items, {successful_count} successful")
        return successful_count