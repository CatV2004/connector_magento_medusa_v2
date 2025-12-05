import json
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from utils.logger import logger


def load_config(config_file: str) -> Dict[str, Any]:
    """Load configuration from YAML or JSON file"""
    path = Path(config_file)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")
    
    with open(path, 'r', encoding='utf-8') as f:
        if path.suffix.lower() in ['.yaml', '.yml']:
            return yaml.safe_load(f)
        elif path.suffix.lower() == '.json':
            return json.load(f)
        else:
            # Try to determine format from content
            content = f.read()
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                try:
                    return yaml.safe_load(content)
                except yaml.YAMLError:
                    raise ValueError(f"Unsupported config file format: {config_file}")


def save_config(config: Dict[str, Any], config_file: str):
    """Save configuration to file"""
    path = Path(config_file)
    
    with open(path, 'w', encoding='utf-8') as f:
        if path.suffix.lower() in ['.yaml', '.yml']:
            yaml.dump(config, f, default_flow_style=False)
        elif path.suffix.lower() == '.json':
            json.dump(config, f, indent=2)
        else:
            # Default to JSON
            json.dump(config, f, indent=2)
    
    logger.info(f"Configuration saved to {config_file}")


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def get_pipeline_files() -> Dict[str, List[str]]:
    """Get all pipeline-related files"""
    pipeline_files = {
        'state_files': [],
        'report_files': [],
        'result_files': []
    }
    
    # Find state files
    for file in Path('.').glob('pipeline_state_*.json'):
        pipeline_files['state_files'].append(str(file))
    
    # Find report files
    for file in Path('.').glob('pipeline_report_*.json'):
        pipeline_files['report_files'].append(str(file))
    
    # Find result files
    for file in Path('.').glob('pipeline_results_*.json'):
        pipeline_files['result_files'].append(str(file))
    
    return pipeline_files


def cleanup_old_files(days: int = 30):
    """Clean up old pipeline files"""
    cutoff_time = datetime.now().timestamp() - (days * 24 * 3600)
    
    files_deleted = 0
    for pattern in ['pipeline_*.json', 'sync_report_*.json', '*_dlq_*.json']:
        for file in Path('.').glob(pattern):
            if file.stat().st_mtime < cutoff_time:
                try:
                    file.unlink()
                    logger.debug(f"Deleted old file: {file}")
                    files_deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete {file}: {e}")
    
    logger.info(f"Cleaned up {files_deleted} old files")


def validate_pipeline_state(state_file: str) -> bool:
    """Validate pipeline state file"""
    try:
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        required_fields = ['pipeline_id', 'status', 'timestamp']
        for field in required_fields:
            if field not in state:
                logger.error(f"Missing required field in state: {field}")
                return False
        
        valid_statuses = ['pending', 'running', 'paused', 'completed', 'failed', 'cancelled']
        if state['status'] not in valid_statuses:
            logger.error(f"Invalid status in state: {state['status']}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to validate state file: {e}")
        return False


def create_backup(file_path: str, backup_dir: str = "backups"):
    """Create backup of a file"""
    path = Path(file_path)
    if not path.exists():
        return None
    
    backup_path = Path(backup_dir) / f"{path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{path.suffix}"
    backup_path.parent.mkdir(exist_ok=True)
    
    try:
        import shutil
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup: {backup_path}")
        return str(backup_path)
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return None