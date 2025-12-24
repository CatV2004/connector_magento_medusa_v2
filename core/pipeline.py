# import asyncio
# from typing import Dict, List, Optional, Any, Callable
# from datetime import datetime
# import time
# from dataclasses import dataclass, field
# from enum import Enum
# from utils.logger import logger
# from core.dlq_handler import DLQHandler
# from core.validator import Validator
# from core.transformer import Transformer
# from connectors.magento.magento_connector import MagentoConnector
# from connectors.medusa.medusa_connector import MedusaConnector


# class PipelineStatus(Enum):
#     """Pipeline status enumeration"""
#     PENDING = "pending"
#     RUNNING = "running"
#     PAUSED = "paused"
#     COMPLETED = "completed"
#     FAILED = "failed"
#     CANCELLED = "cancelled"


# @dataclass
# class PipelineStep:
#     """Pipeline step configuration"""
#     name: str
#     executor: Callable
#     depends_on: List[str] = field(default_factory=list)
#     timeout: int = 300  # 5 minutes default
#     retries: int = 3
#     enabled: bool = True
#     params: Dict[str, Any] = field(default_factory=dict)
    
#     def __post_init__(self):
#         self.id = f"step_{self.name.lower().replace(' ', '_')}"
#         self.status = PipelineStatus.PENDING
#         self.start_time: Optional[datetime] = None
#         self.end_time: Optional[datetime] = None
#         self.error: Optional[str] = None
#         self.attempts: int = 0


# @dataclass
# class PipelineStats:
#     """Pipeline statistics"""
#     total_steps: int = 0
#     completed_steps: int = 0
#     failed_steps: int = 0
#     skipped_steps: int = 0
#     total_items_processed: int = 0
#     successful_items: int = 0
#     failed_items: int = 0
#     start_time: Optional[datetime] = None
#     end_time: Optional[datetime] = None
    
#     @property
#     def duration(self) -> Optional[float]:
#         """Get pipeline duration in seconds"""
#         if self.start_time and self.end_time:
#             return (self.end_time - self.start_time).total_seconds()
#         elif self.start_time:
#             return (datetime.now() - self.start_time).total_seconds()
#         return None
    
#     @property
#     def success_rate(self) -> float:
#         """Calculate success rate"""
#         if self.total_items_processed == 0:
#             return 0.0
#         return (self.successful_items / self.total_items_processed) * 100
    
#     def to_dict(self) -> Dict[str, Any]:
#         """Convert to dictionary"""
#         return {
#             'total_steps': self.total_steps,
#             'completed_steps': self.completed_steps,
#             'failed_steps': self.failed_steps,
#             'skipped_steps': self.skipped_steps,
#             'total_items_processed': self.total_items_processed,
#             'successful_items': self.successful_items,
#             'failed_items': self.failed_items,
#             'start_time': self.start_time.isoformat() if self.start_time else None,
#             'end_time': self.end_time.isoformat() if self.end_time else None,
#             'duration': self.duration,
#             'success_rate': self.success_rate
#         }


# class SyncPipeline:
#     """
#     Main sync pipeline that orchestrates data synchronization
#     from Magento to Medusa
#     """
    
#     def __init__(
#         self,
#         magento: MagentoConnector,
#         medusa: MedusaConnector,
#         pipeline_id: str = None,
#         config: Dict[str, Any] = None
#     ):
#         """
#         Initialize sync pipeline
        
#         Args:
#             magento: Magento connector instance
#             medusa: Medusa connector instance
#             pipeline_id: Unique pipeline identifier
#             config: Pipeline configuration
#         """
#         self.magento = magento
#         self.medusa = medusa
#         self.pipeline_id = pipeline_id or f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#         self.config = config or {}
        
#         # Core components
#         self.validator = Validator()
#         self.transformer = Transformer()
#         self.dlq = DLQHandler('pipeline')
        
#         # Pipeline state
#         self.steps: Dict[str, PipelineStep] = {}
#         self.status = PipelineStatus.PENDING
#         self.stats = PipelineStats()
#         self.results: Dict[str, Any] = {}
#         self.errors: List[Dict[str, Any]] = []
        
#         # Dependencies
#         self.dependency_graph: Dict[str, List[str]] = {}
        
#         # Callbacks
#         self.on_step_start: Optional[Callable] = None
#         self.on_step_complete: Optional[Callable] = None
#         self.on_step_failed: Optional[Callable] = None
#         self.on_pipeline_complete: Optional[Callable] = None
#         self.on_pipeline_failed: Optional[Callable] = None
        
#         logger.info(f"Initialized pipeline: {self.pipeline_id}")
    
#     def add_step(self, step: PipelineStep):
#         """
#         Add a step to the pipeline
        
#         Args:
#             step: Pipeline step configuration
#         """
#         if step.id in self.steps:
#             raise ValueError(f"Step {step.id} already exists")
        
#         self.steps[step.id] = step
#         self.dependency_graph[step.id] = step.depends_on
        
#         logger.debug(f"Added step: {step.name} (ID: {step.id})")
    
#     def add_steps(self, steps: List[PipelineStep]):
#         """Add multiple steps to the pipeline"""
#         for step in steps:
#             self.add_step(step)
    
#     def build_default_pipeline(self):
#         """Build default sync pipeline"""
#         default_steps = [
#             PipelineStep(
#                 name="Test Connections",
#                 executor=self._test_connections,
#                 timeout=30
#             ),
#             PipelineStep(
#                 name="Sync Categories",
#                 executor=self._sync_categories,
#                 depends_on=["step_test_connections"],
#                 timeout=600,
#                 params={'batch_size': 100}
#             ),
#             PipelineStep(
#                 name="Sync Products",
#                 executor=self._sync_products,
#                 depends_on=["step_sync_categories"],
#                 timeout=3600,
#                 params={'batch_size': 50, 'max_pages': None}
#             ),
#             PipelineStep(
#                 name="Sync Customers",
#                 executor=self._sync_customers,
#                 depends_on=["step_test_connections"],
#                 timeout=1800,
#                 params={'batch_size': 100}
#             ),
#             PipelineStep(
#                 name="Process DLQ",
#                 executor=self._process_dlq,
#                 depends_on=[
#                     "step_sync_categories",
#                     "step_sync_products",
#                     "step_sync_customers"
#                 ],
#                 timeout=300
#             ),
#             PipelineStep(
#                 name="Generate Report",
#                 executor=self._generate_report,
#                 depends_on=["step_process_dlq"],
#                 timeout=60
#             )
#         ]
        
#         self.add_steps(default_steps)
#         logger.info(f"Built default pipeline with {len(default_steps)} steps")
    
#     def run(self, dry_run: bool = False) -> Dict[str, Any]:
#         """
#         Run the pipeline
        
#         Args:
#             dry_run: If True, don't make actual changes
            
#         Returns:
#             Pipeline results
#         """
#         if self.status == PipelineStatus.RUNNING:
#             raise RuntimeError("Pipeline is already running")
        
#         logger.info(f"Starting pipeline: {self.pipeline_id}")
#         logger.info(f"Dry run mode: {dry_run}")
        
#         self.status = PipelineStatus.RUNNING
#         self.stats.start_time = datetime.now()
#         self.stats.total_steps = len(self.steps)
        
#         try:
#             # Validate pipeline configuration
#             self._validate_pipeline()
            
#             # Execute steps in dependency order
#             executed_steps = set()
            
#             while len(executed_steps) < len(self.steps):
#                 # Find ready steps (dependencies satisfied)
#                 ready_steps = self._get_ready_steps(executed_steps)
                
#                 if not ready_steps:
#                     # Check for circular dependencies or unmet dependencies
#                     unresolved = self._find_unresolved_dependencies(executed_steps)
#                     if unresolved:
#                         raise RuntimeError(f"Unresolved dependencies: {unresolved}")
#                     break
                
#                 # Execute ready steps
#                 for step_id in ready_steps:
#                     step = self.steps[step_id]
                    
#                     if not step.enabled:
#                         logger.info(f"Skipping disabled step: {step.name}")
#                         step.status = PipelineStatus.PENDING
#                         self.stats.skipped_steps += 1
#                         executed_steps.add(step_id)
#                         continue
                    
#                     # Execute step
#                     success = self._execute_step(step, dry_run)
                    
#                     if success:
#                         executed_steps.add(step_id)
#                         self.stats.completed_steps += 1
#                     else:
#                         self.stats.failed_steps += 1
                        
#                         # Check if step should be retried
#                         if step.attempts < step.retries:
#                             logger.info(f"Retrying step {step.name} (attempt {step.attempts + 1}/{step.retries})")
#                             time.sleep(2 ** step.attempts)  # Exponential backoff
#                             success = self._execute_step(step, dry_run)
                            
#                             if success:
#                                 executed_steps.add(step_id)
#                                 self.stats.completed_steps += 1
#                                 self.stats.failed_steps -= 1
#                             else:
#                                 # Step failed after retries
#                                 self._handle_step_failure(step)
#                                 break
#                         else:
#                             # Step failed after max retries
#                             self._handle_step_failure(step)
#                             break
                
#                 # If any step failed, stop pipeline
#                 if any(s.status == PipelineStatus.FAILED for s in self.steps.values()):
#                     break
            
#             # Update pipeline status
#             if all(s.status in [PipelineStatus.COMPLETED, PipelineStatus.PENDING, PipelineStatus.SKIPPED] 
#                    for s in self.steps.values()):
#                 self.status = PipelineStatus.COMPLETED
#                 logger.info("Pipeline completed successfully")
                
#                 if self.on_pipeline_complete:
#                     self.on_pipeline_complete(self)
#             else:
#                 self.status = PipelineStatus.FAILED
#                 logger.error("Pipeline failed")
                
#                 if self.on_pipeline_failed:
#                     self.on_pipeline_failed(self)
            
#         except KeyboardInterrupt:
#             logger.warning("Pipeline interrupted by user")
#             self.status = PipelineStatus.CANCELLED
#             self._handle_interruption()
            
#         except Exception as e:
#             logger.error(f"Pipeline failed with error: {e}")
#             self.status = PipelineStatus.FAILED
#             self.errors.append({
#                 'timestamp': datetime.now().isoformat(),
#                 'error': str(e),
#                 'type': type(e).__name__
#             })
            
#             if self.on_pipeline_failed:
#                 self.on_pipeline_failed(self)
                
#         finally:
#             self.stats.end_time = datetime.now()
#             self._log_pipeline_summary()
        
#         return {
#             'pipeline_id': self.pipeline_id,
#             'status': self.status.value,
#             'stats': self.stats.to_dict(),
#             'results': self.results,
#             'errors': self.errors,
#             'dry_run': dry_run
#         }
    
#     def _execute_step(self, step: PipelineStep, dry_run: bool) -> bool:
#         """Execute a single pipeline step"""
#         step.attempts += 1
#         step.start_time = datetime.now()
#         step.status = PipelineStatus.RUNNING
        
#         logger.info(f"Executing step: {step.name}")
        
#         if self.on_step_start:
#             self.on_step_start(step)
        
#         try:
#             # Prepare parameters
#             params = step.params.copy()
#             params['dry_run'] = dry_run
            
#             # Execute step function
#             result = step.executor(**params)
            
#             # Store result
#             self.results[step.id] = result
            
#             # Update step status
#             step.status = PipelineStatus.COMPLETED
#             step.end_time = datetime.now()
            
#             # Update statistics
#             if isinstance(result, dict) and 'stats' in result:
#                 stats = result['stats']
#                 if 'total_processed' in stats:
#                     self.stats.total_items_processed += stats['total_processed']
#                 if 'successful' in stats:
#                     self.stats.successful_items += stats['successful']
#                 if 'failed' in stats:
#                     self.stats.failed_items += stats['failed']
            
#             duration = (step.end_time - step.start_time).total_seconds()
#             logger.info(f"Step completed: {step.name} ({duration:.2f}s)")
            
#             if self.on_step_complete:
#                 self.on_step_complete(step, result)
            
#             return True
            
#         except Exception as e:
#             step.status = PipelineStatus.FAILED
#             step.end_time = datetime.now()
#             step.error = str(e)
            
#             duration = (step.end_time - step.start_time).total_seconds()
#             logger.error(f"Step failed: {step.name} ({duration:.2f}s) - {e}")
            
#             # Add to errors list
#             self.errors.append({
#                 'step_id': step.id,
#                 'step_name': step.name,
#                 'timestamp': datetime.now().isoformat(),
#                 'error': str(e),
#                 'attempt': step.attempts,
#                 'duration': duration
#             })
            
#             # Add to DLQ
#             self.dlq.add_item({
#                 'pipeline_id': self.pipeline_id,
#                 'step_id': step.id,
#                 'step_name': step.name,
#                 'error': str(e),
#                 'timestamp': datetime.now().isoformat(),
#                 'attempt': step.attempts
#             })
            
#             if self.on_step_failed:
#                 self.on_step_failed(step, e)
            
#             return False
    
#     def _get_ready_steps(self, executed_steps: set) -> List[str]:
#         """Get steps that are ready to execute (dependencies satisfied)"""
#         ready_steps = []
        
#         for step_id, step in self.steps.items():
#             if step_id in executed_steps:
#                 continue
            
#             # Check if all dependencies are satisfied
#             dependencies = self.dependency_graph.get(step_id, [])
#             if all(dep in executed_steps for dep in dependencies):
#                 ready_steps.append(step_id)
        
#         # Sort by dependency depth (optional)
#         ready_steps.sort(key=lambda x: len(self.dependency_graph.get(x, [])))
        
#         return ready_steps
    
#     def _find_unresolved_dependencies(self, executed_steps: set) -> List[str]:
#         """Find steps with unresolved dependencies"""
#         unresolved = []
        
#         for step_id, step in self.steps.items():
#             if step_id in executed_steps:
#                 continue
            
#             dependencies = self.dependency_graph.get(step_id, [])
#             missing = [dep for dep in dependencies if dep not in executed_steps]
            
#             if missing:
#                 unresolved.append(f"{step_id} -> {missing}")
        
#         return unresolved
    
#     def _validate_pipeline(self):
#         """Validate pipeline configuration"""
#         # Check for circular dependencies
#         if self._has_circular_dependencies():
#             raise ValueError("Pipeline has circular dependencies")
        
#         # Check that all step IDs referenced in dependencies exist
#         for step_id, dependencies in self.dependency_graph.items():
#             for dep in dependencies:
#                 if dep not in self.steps:
#                     raise ValueError(f"Dependency {dep} referenced by {step_id} does not exist")
        
#         logger.info("Pipeline validation passed")
    
#     def _has_circular_dependencies(self) -> bool:
#         """Check for circular dependencies using DFS"""
#         visited = set()
#         recursion_stack = set()
        
#         def dfs(node):
#             visited.add(node)
#             recursion_stack.add(node)
            
#             for neighbor in self.dependency_graph.get(node, []):
#                 if neighbor not in visited:
#                     if dfs(neighbor):
#                         return True
#                 elif neighbor in recursion_stack:
#                     return True
            
#             recursion_stack.remove(node)
#             return False
        
#         for step_id in self.steps:
#             if step_id not in visited:
#                 if dfs(step_id):
#                     return True
        
#         return False
    
#     def _handle_step_failure(self, step: PipelineStep):
#         """Handle step failure"""
#         logger.error(f"Step {step.name} failed after {step.retries} retries")
        
#         # Determine if pipeline should continue
#         critical_steps = ['step_test_connections', 'step_sync_categories']
#         if step.id in critical_steps:
#             logger.error(f"Critical step failed, stopping pipeline")
#             self.status = PipelineStatus.FAILED
#         else:
#             logger.warning(f"Non-critical step failed, continuing pipeline")
    
#     def _handle_interruption(self):
#         """Handle pipeline interruption"""
#         logger.warning("Cleaning up after interruption...")
        
#         # Save current state for resume
#         state = {
#             'pipeline_id': self.pipeline_id,
#             'status': self.status.value,
#             'step_states': {
#                 step_id: {
#                     'status': step.status.value,
#                     'attempts': step.attempts,
#                     'error': step.error
#                 }
#                 for step_id, step in self.steps.items()
#             },
#             'results': self.results,
#             'stats': self.stats.to_dict(),
#             'timestamp': datetime.now().isoformat()
#         }
        
#         # Save state to file
#         import json
#         state_file = f"pipeline_state_{self.pipeline_id}.json"
#         with open(state_file, 'w') as f:
#             json.dump(state, f, indent=2)
        
#         logger.info(f"Pipeline state saved to {state_file}")
    
#     def _log_pipeline_summary(self):
#         """Log pipeline summary"""
#         duration = self.stats.duration
#         if duration:
#             duration_str = f"{duration:.2f}s"
#         else:
#             duration_str = "N/A"
        
#         logger.info("=" * 70)
#         logger.info("PIPELINE SUMMARY")
#         logger.info("=" * 70)
#         logger.info(f"Pipeline ID: {self.pipeline_id}")
#         logger.info(f"Status: {self.status.value}")
#         logger.info(f"Duration: {duration_str}")
#         logger.info(f"Steps: {self.stats.completed_steps}/{self.stats.total_steps} completed")
#         logger.info(f"Items: {self.stats.successful_items} successful, "
#                    f"{self.stats.failed_items} failed")
#         logger.info(f"Success Rate: {self.stats.success_rate:.2f}%")
#         logger.info("=" * 70)
        
#         # Log step details
#         for step_id, step in self.steps.items():
#             if step.start_time and step.end_time:
#                 step_duration = (step.end_time - step.start_time).total_seconds()
#                 logger.info(f"  {step.name}: {step.status.value} ({step_duration:.2f}s)")
#             else:
#                 logger.info(f"  {step.name}: {step.status.value}")
    
#     # Step execution methods
#     def _test_connections(self, **kwargs) -> Dict[str, Any]:
#         """Test connections to Magento and Medusa"""
#         dry_run = kwargs.get('dry_run', False)
        
#         logger.info("Testing connections...")
        
#         results = {}
        
#         # Test Magento connection
#         try:
#             magento_result = self.magento.test_connection()
#             results['magento'] = {
#                 'success': True,
#                 'data': magento_result
#             }
#             logger.info("✓ Magento connection successful")
#         except Exception as e:
#             results['magento'] = {
#                 'success': False,
#                 'error': str(e)
#             }
#             logger.error(f"✗ Magento connection failed: {e}")
#             if not dry_run:
#                 raise
        
#         # Test Medusa connection
#         try:
#             medusa_result = self.medusa.test_connection()
#             results['medusa'] = {
#                 'success': True,
#                 'data': medusa_result
#             }
#             logger.info("✓ Medusa connection successful")
#         except Exception as e:
#             results['medusa'] = {
#                 'success': False,
#                 'error': str(e)
#             }
#             logger.error(f"✗ Medusa connection failed: {e}")
#             if not dry_run:
#                 raise
        
#         return results
    
#     def _sync_categories(self, **kwargs) -> Dict[str, Any]:
#         """Sync categories"""
#         from services.category_sync_service import CategorySyncService
        
#         batch_size = kwargs.get('batch_size', 100)
#         dry_run = kwargs.get('dry_run', False)
        
#         logger.info(f"Syncing categories (batch_size={batch_size}, dry_run={dry_run})")
        
#         service = CategorySyncService(self.magento, self.medusa)
        
#         if dry_run:
#             logger.info("Dry run - would sync categories")
#             return {'dry_run': True, 'would_sync': True}
        
#         result = service.sync_all()
        
#         # Store category mapping for product sync
#         self.results['category_mapping'] = result.get('mapping', {})
        
#         return result
    
#     def _sync_products(self, **kwargs) -> Dict[str, Any]:
#         """Sync products"""
#         from services.product_sync_service import ProductSyncService
        
#         batch_size = kwargs.get('batch_size', 50)
#         max_pages = kwargs.get('max_pages')
#         dry_run = kwargs.get('dry_run', False)
        
#         # Get category mapping from previous step
#         category_mapping = self.results.get('category_mapping', {})
        
#         logger.info(f"Syncing products (batch_size={batch_size}, dry_run={dry_run})")
        
#         service = ProductSyncService(self.magento, self.medusa, category_mapping)
        
#         if dry_run:
#             logger.info("Dry run - would sync products")
#             return {'dry_run': True, 'would_sync': True}
        
#         return service.sync_all(batch_size=batch_size, max_pages=max_pages)
    
#     def _sync_customers(self, **kwargs) -> Dict[str, Any]:
#         """Sync customers"""
#         from services.customer_sync_service import CustomerSyncService
        
#         batch_size = kwargs.get('batch_size', 100)
#         max_pages = kwargs.get('max_pages')
#         dry_run = kwargs.get('dry_run', False)
        
#         logger.info(f"Syncing customers (batch_size={batch_size}, dry_run={dry_run})")
        
#         service = CustomerSyncService(self.magento, self.medusa)
        
#         if dry_run:
#             logger.info("Dry run - would sync customers")
#             return {'dry_run': True, 'would_sync': True}
        
#         return service.sync_all(batch_size=batch_size, max_pages=max_pages)
    
#     def _process_dlq(self, **kwargs) -> Dict[str, Any]:
#         """Process items in Dead Letter Queue"""
#         dry_run = kwargs.get('dry_run', False)
        
#         logger.info("Processing DLQ items...")
        
#         # Get DLQ counts
#         dlq_counts = {}
#         for entity in ['products', 'categories', 'customers', 'pipeline']:
#             dlq = DLQHandler(entity)
#             count = dlq.get_count()
#             dlq_counts[entity] = count
        
#         total_dlq = sum(dlq_counts.values())
        
#         if total_dlq == 0:
#             logger.info("No items in DLQ")
#             return {'dlq_counts': dlq_counts, 'processed': 0}
        
#         logger.warning(f"Found {total_dlq} items in DLQ:")
#         for entity, count in dlq_counts.items():
#             if count > 0:
#                 logger.warning(f"  {entity}: {count} items")
        
#         if dry_run:
#             logger.info("Dry run - would process DLQ items")
#             return {'dry_run': True, 'dlq_counts': dlq_counts}
        
#         # In production, you'd implement retry logic here
#         logger.info("DLQ processing would be implemented here")
        
#         return {'dlq_counts': dlq_counts, 'processed': 0}
    
#     def _generate_report(self, **kwargs) -> Dict[str, Any]:
#         """Generate sync report"""
#         dry_run = kwargs.get('dry_run', False)
        
#         logger.info("Generating sync report...")
        
#         report = {
#             'pipeline_id': self.pipeline_id,
#             'status': self.status.value,
#             'timestamp': datetime.now().isoformat(),
#             'stats': self.stats.to_dict(),
#             'step_results': {},
#             'errors': self.errors
#         }
        
#         # Add step results
#         for step_id, step in self.steps.items():
#             step_result = self.results.get(step_id, {})
            
#             # Extract relevant stats
#             if isinstance(step_result, dict):
#                 step_stats = step_result.get('stats', {})
#             else:
#                 step_stats = {}
            
#             report['step_results'][step_id] = {
#                 'name': step.name,
#                 'status': step.status.value,
#                 'start_time': step.start_time.isoformat() if step.start_time else None,
#                 'end_time': step.end_time.isoformat() if step.end_time else None,
#                 'attempts': step.attempts,
#                 'error': step.error,
#                 'stats': step_stats
#             }
        
#         # Save report to file
#         if not dry_run:
#             import json
#             report_file = f"sync_report_{self.pipeline_id}.json"
#             with open(report_file, 'w') as f:
#                 json.dump(report, f, indent=2)
            
#             logger.info(f"Report saved to {report_file}")
        
#         return report
    
#     def resume(self, state_file: str) -> Dict[str, Any]:
#         """
#         Resume pipeline from saved state
        
#         Args:
#             state_file: Path to saved state file
            
#         Returns:
#             Pipeline results
#         """
#         import json
        
#         logger.info(f"Resuming pipeline from {state_file}")
        
#         with open(state_file, 'r') as f:
#             state = json.load(f)
        
#         # Restore pipeline state
#         self.pipeline_id = state['pipeline_id']
        
#         # Restore step states
#         for step_id, step_state in state.get('step_states', {}).items():
#             if step_id in self.steps:
#                 step = self.steps[step_id]
#                 step.status = PipelineStatus(step_state['status'])
#                 step.attempts = step_state.get('attempts', 0)
#                 step.error = step_state.get('error')
        
#         # Restore results
#         self.results = state.get('results', {})
        
#         # Restore stats
#         stats_data = state.get('stats', {})
#         self.stats = PipelineStats()
#         for key, value in stats_data.items():
#             if hasattr(self.stats, key):
#                 setattr(self.stats, key, value)
        
#         logger.info(f"Pipeline state restored, resuming from step {len(self.results)}")
        
#         # Continue pipeline execution
#         return self.run()
    
#     def get_status_report(self) -> Dict[str, Any]:
#         """Get current pipeline status report"""
#         return {
#             'pipeline_id': self.pipeline_id,
#             'status': self.status.value,
#             'stats': self.stats.to_dict(),
#             'steps': {
#                 step_id: {
#                     'name': step.name,
#                     'status': step.status.value,
#                     'attempts': step.attempts,
#                     'start_time': step.start_time.isoformat() if step.start_time else None,
#                     'end_time': step.end_time.isoformat() if step.end_time else None,
#                     'error': step.error,
#                     'enabled': step.enabled
#                 }
#                 for step_id, step in self.steps.items()
#             },
#             'errors': self.errors,
#             'timestamp': datetime.now().isoformat()
#         }
    
#     def cancel(self):
#         """Cancel pipeline execution"""
#         if self.status == PipelineStatus.RUNNING:
#             logger.warning("Cancelling pipeline...")
#             self.status = PipelineStatus.CANCELLED
#             self._handle_interruption()
#         else:
#             logger.info(f"Pipeline is not running (status: {self.status.value})")


# class AsyncSyncPipeline(SyncPipeline):
#     """
#     Async version of sync pipeline for concurrent execution
#     """
    
#     async def run_async(self, dry_run: bool = False) -> Dict[str, Any]:
#         """Run pipeline asynchronously"""
#         if self.status == PipelineStatus.RUNNING:
#             raise RuntimeError("Pipeline is already running")
        
#         logger.info(f"Starting async pipeline: {self.pipeline_id}")
        
#         self.status = PipelineStatus.RUNNING
#         self.stats.start_time = datetime.now()
#         self.stats.total_steps = len(self.steps)
        
#         try:
#             # Validate pipeline
#             self._validate_pipeline()
            
#             # Execute steps with concurrency
#             await self._execute_steps_concurrently(dry_run)
            
#             # Update pipeline status
#             if all(s.status in [PipelineStatus.COMPLETED, PipelineStatus.PENDING, PipelineStatus.SKIPPED] 
#                    for s in self.steps.values()):
#                 self.status = PipelineStatus.COMPLETED
#                 logger.info("Async pipeline completed successfully")
#             else:
#                 self.status = PipelineStatus.FAILED
#                 logger.error("Async pipeline failed")
                
#         except Exception as e:
#             logger.error(f"Async pipeline failed with error: {e}")
#             self.status = PipelineStatus.FAILED
#             self.errors.append({
#                 'timestamp': datetime.now().isoformat(),
#                 'error': str(e),
#                 'type': type(e).__name__
#             })
            
#         finally:
#             self.stats.end_time = datetime.now()
#             self._log_pipeline_summary()
        
#         return {
#             'pipeline_id': self.pipeline_id,
#             'status': self.status.value,
#             'stats': self.stats.to_dict(),
#             'results': self.results,
#             'errors': self.errors,
#             'dry_run': dry_run
#         }
    
#     async def _execute_steps_concurrently(self, dry_run: bool):
#         """Execute steps concurrently based on dependencies"""
#         import asyncio
        
#         # Create task for each step
#         tasks = {}
#         for step_id, step in self.steps.items():
#             if step.enabled:
#                 task = asyncio.create_task(
#                     self._execute_step_async(step, dry_run),
#                     name=step_id
#                 )
#                 tasks[step_id] = task
        
#         # Wait for tasks to complete with dependency awareness
#         completed = set()
        
#         while tasks:
#             # Check which tasks can run (dependencies satisfied)
#             ready_tasks = {}
#             for step_id, task in tasks.items():
#                 step = self.steps[step_id]
#                 dependencies = self.dependency_graph.get(step_id, [])
                
#                 if all(dep in completed for dep in dependencies):
#                     ready_tasks[step_id] = task
            
#             if not ready_tasks:
#                 # No tasks ready, check for deadlock
#                 logger.warning("No tasks ready, possible deadlock")
#                 break
            
#             # Execute ready tasks concurrently
#             logger.info(f"Executing {len(ready_tasks)} tasks concurrently")
            
#             done, pending = await asyncio.wait(
#                 ready_tasks.values(),
#                 return_when=asyncio.FIRST_COMPLETED
#             )
            
#             # Process completed tasks
#             for task in done:
#                 step_id = task.get_name()
#                 step = self.steps[step_id]
                
#                 try:
#                     result = await task
#                     self.results[step_id] = result
#                     step.status = PipelineStatus.COMPLETED
#                     completed.add(step_id)
                    
#                     # Update statistics
#                     if isinstance(result, dict) and 'stats' in result:
#                         stats = result['stats']
#                         self._update_stats_from_result(stats)
                    
#                     logger.info(f"Task completed: {step.name}")
                    
#                 except Exception as e:
#                     step.status = PipelineStatus.FAILED
#                     step.error = str(e)
#                     self.stats.failed_steps += 1
                    
#                     logger.error(f"Task failed: {step.name} - {e}")
            
#             # Remove completed tasks from pending
#             for step_id in list(tasks.keys()):
#                 if step_id in completed:
#                     del tasks[step_id]
        
#         # Update step completion counts
#         self.stats.completed_steps = len([s for s in self.steps.values() 
#                                          if s.status == PipelineStatus.COMPLETED])
    
#     async def _execute_step_async(self, step: PipelineStep, dry_run: bool):
#         """Execute a step asynchronously"""
#         step.attempts += 1
#         step.start_time = datetime.now()
#         step.status = PipelineStatus.RUNNING
        
#         logger.info(f"Executing async step: {step.name}")
        
#         try:
#             # Prepare parameters
#             params = step.params.copy()
#             params['dry_run'] = dry_run
            
#             # Execute step function (convert sync to async if needed)
#             if asyncio.iscoroutinefunction(step.executor):
#                 result = await step.executor(**params)
#             else:
#                 # Run sync function in thread pool
#                 loop = asyncio.get_event_loop()
#                 result = await loop.run_in_executor(
#                     None, 
#                     lambda: step.executor(**params)
#                 )
            
#             step.end_time = datetime.now()
            
#             duration = (step.end_time - step.start_time).total_seconds()
#             logger.info(f"Async step completed: {step.name} ({duration:.2f}s)")
            
#             return result
            
#         except Exception as e:
#             step.status = PipelineStatus.FAILED
#             step.end_time = datetime.now()
#             step.error = str(e)
            
#             duration = (step.end_time - step.start_time).total_seconds()
#             logger.error(f"Async step failed: {step.name} ({duration:.2f}s) - {e}")
            
#             # Add to DLQ
#             self.dlq.add_item({
#                 'pipeline_id': self.pipeline_id,
#                 'step_id': step.id,
#                 'step_name': step.name,
#                 'error': str(e),
#                 'timestamp': datetime.now().isoformat(),
#                 'attempt': step.attempts
#             })
            
#             raise
    
#     def _update_stats_from_result(self, result_stats: Dict[str, Any]):
#         """Update pipeline stats from step result"""
#         if 'total_processed' in result_stats:
#             self.stats.total_items_processed += result_stats['total_processed']
#         if 'successful' in result_stats:
#             self.stats.successful_items += result_stats['successful']
#         if 'failed' in result_stats:
#             self.stats.failed_items += result_stats['failed']


# # Factory function for creating pipelines
# def create_pipeline(
#     magento: MagentoConnector,
#     medusa: MedusaConnector,
#     pipeline_type: str = "default",
#     config: Dict[str, Any] = None
# ) -> SyncPipeline:
#     """
#     Create a sync pipeline
    
#     Args:
#         magento: Magento connector
#         medusa: Medusa connector
#         pipeline_type: Type of pipeline ("default", "async", "custom")
#         config: Pipeline configuration
        
#     Returns:
#         SyncPipeline instance
#     """
#     if pipeline_type == "async":
#         pipeline = AsyncSyncPipeline(magento, medusa, config=config)
#     else:
#         pipeline = SyncPipeline(magento, medusa, config=config)
    
#     if pipeline_type == "default":
#         pipeline.build_default_pipeline()
    
#     return pipeline