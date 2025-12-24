from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from dataclasses import dataclass, field
from core.pipeline.pipeline_status import PipelineStatus

@dataclass
class PipelineStep:
    """Pipeline step configuration"""
    name: str
    executor: Callable
    depends_on: List[str] = field(default_factory=list)
    timeout: int = 300  # 5 minutes default
    retries: int = 3
    enabled: bool = True
    params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self.id = f"step_{self.name.lower().replace(' ', '_')}"
        self.status = PipelineStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.error: Optional[str] = None
        self.attempts: int = 0