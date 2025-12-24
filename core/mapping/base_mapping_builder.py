from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path
import yaml
from utils.logger import logger


class MappingBuilder(ABC):
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    @abstractmethod
    def build(self) -> Dict[str, Any]:
        pass

    def _load(self, filename: str) -> Dict[str, Any]:
        path = self.base_dir / filename
        logger.info(f"Loading mapping file: {path.resolve()}")
        
        if not path.exists():
            logger.error(f"Mapping file NOT FOUND: {path.resolve()}")
            return {}
            
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            logger.info(f"Loaded data from {filename}: {data}")
            return data