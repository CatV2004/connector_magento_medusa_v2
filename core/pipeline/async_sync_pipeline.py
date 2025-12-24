import asyncio
from typing import Dict, Any
from datetime import datetime
from utils.logger import logger
from core.pipeline.pipeline_status import PipelineStatus
from core.pipeline.pipeline_step import PipelineStep
from core.pipeline.pipeline import SyncPipeline


class AsyncSyncPipeline(SyncPipeline):
    """
    Async version of sync pipeline for concurrent execution
    """
    
    async def run_async(self, dry_run: bool = False) -> Dict[str, Any]:
        """Run pipeline asynchronously"""
        if self.status == PipelineStatus.RUNNING:
            raise RuntimeError("Pipeline is already running")
        
        logger.info(f"Starting async pipeline: {self.pipeline_id}")
        
        self.status = PipelineStatus.RUNNING
        self.stats.start_time = datetime.now()
        self.stats.total_steps = len(self.steps)
        
        try:
            # Validate pipeline
            self._validate_pipeline()
            
            # Execute steps with concurrency
            await self._execute_steps_concurrently(dry_run)
            
            # Update pipeline status
            if all(s.status in [PipelineStatus.COMPLETED, PipelineStatus.PENDING, PipelineStatus.SKIPPED] 
                   for s in self.steps.values()):
                self.status = PipelineStatus.COMPLETED
                logger.info("Async pipeline completed successfully")
            else:
                self.status = PipelineStatus.FAILED
                logger.error("Async pipeline failed")
                
        except Exception as e:
            logger.error(f"Async pipeline failed with error: {e}")
            self.status = PipelineStatus.FAILED
            self.errors.append({
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'type': type(e).__name__
            })
            
        finally:
            self.stats.end_time = datetime.now()
            self._log_pipeline_summary()
        
        return {
            'pipeline_id': self.pipeline_id,
            'status': self.status.value,
            'stats': self.stats.to_dict(),
            'results': self.results,
            'errors': self.errors,
            'dry_run': dry_run
        }
    
    async def _execute_steps_concurrently(self, dry_run: bool):
        """Execute steps concurrently based on dependencies"""
        import asyncio
        
        # Create task for each step
        tasks = {}
        for step_id, step in self.steps.items():
            if step.enabled:
                task = asyncio.create_task(
                    self._execute_step_async(step, dry_run),
                    name=step_id
                )
                tasks[step_id] = task
        
        # Wait for tasks to complete with dependency awareness
        completed = set()
        
        while tasks:
            # Check which tasks can run (dependencies satisfied)
            ready_tasks = {}
            for step_id, task in tasks.items():
                step = self.steps[step_id]
                dependencies = self.dependency_graph.get(step_id, [])
                
                if all(dep in completed for dep in dependencies):
                    ready_tasks[step_id] = task
            
            if not ready_tasks:
                # No tasks ready, check for deadlock
                logger.warning("No tasks ready, possible deadlock")
                break
            
            # Execute ready tasks concurrently
            logger.info(f"Executing {len(ready_tasks)} tasks concurrently")
            
            done, pending = await asyncio.wait(
                ready_tasks.values(),
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Process completed tasks
            for task in done:
                step_id = task.get_name()
                step = self.steps[step_id]
                
                try:
                    result = await task
                    self.results[step_id] = result
                    step.status = PipelineStatus.COMPLETED
                    completed.add(step_id)
                    
                    # Update statistics
                    if isinstance(result, dict) and 'stats' in result:
                        stats = result['stats']
                        self._update_stats_from_result(stats)
                    
                    logger.info(f"Task completed: {step.name}")
                    
                except Exception as e:
                    step.status = PipelineStatus.FAILED
                    step.error = str(e)
                    self.stats.failed_steps += 1
                    
                    logger.error(f"Task failed: {step.name} - {e}")
            
            # Remove completed tasks from pending
            for step_id in list(tasks.keys()):
                if step_id in completed:
                    del tasks[step_id]
        
        # Update step completion counts
        self.stats.completed_steps = len([s for s in self.steps.values() 
                                         if s.status == PipelineStatus.COMPLETED])
    
    async def _execute_step_async(self, step: PipelineStep, dry_run: bool):
        """Execute a step asynchronously"""
        step.attempts += 1
        step.start_time = datetime.now()
        step.status = PipelineStatus.RUNNING
        
        logger.info(f"Executing async step: {step.name}")
        
        try:
            # Prepare parameters
            params = step.params.copy()
            params['dry_run'] = dry_run
            
            # Execute step function (convert sync to async if needed)
            if asyncio.iscoroutinefunction(step.executor):
                result = await step.executor(**params)
            else:
                # Run sync function in thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: step.executor(**params)
                )
            
            step.end_time = datetime.now()
            
            duration = (step.end_time - step.start_time).total_seconds()
            logger.info(f"Async step completed: {step.name} ({duration:.2f}s)")
            
            return result
            
        except Exception as e:
            step.status = PipelineStatus.FAILED
            step.end_time = datetime.now()
            step.error = str(e)
            
            duration = (step.end_time - step.start_time).total_seconds()
            logger.error(f"Async step failed: {step.name} ({duration:.2f}s) - {e}")
            
            # Add to DLQ
            self.dlq.add_item({
                'pipeline_id': self.pipeline_id,
                'step_id': step.id,
                'step_name': step.name,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'attempt': step.attempts
            })
            
            raise
    
    def _update_stats_from_result(self, result_stats: Dict[str, Any]):
        """Update pipeline stats from step result"""
        if 'total_processed' in result_stats:
            self.stats.total_items_processed += result_stats['total_processed']
        if 'successful' in result_stats:
            self.stats.successful_items += result_stats['successful']
        if 'failed' in result_stats:
            self.stats.failed_items += result_stats['failed']