"""
Process-Separated PHI De-identification Pipeline with Producer-Consumer Pattern
Orchestrates the complete PHI classification process with true parallel processing using separate processes
"""

import logging
import sys
import traceback
import threading
import multiprocessing
from multiprocessing import Process, Queue as MPQueue, Event as MPEvent, TimeoutError as MPTimeoutError
from queue import Queue, Empty
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import os
import json
from nd_api.models import Clients, ClientDataDump
from .database_manager import DatabaseManager
from .llm_agent_mac import LLMAgent, PHIClassificationResult
from .phi_validation_tools import PHIValidationToolsManager, build_mapping_cache, init_worker_mapping_cache
from .output_manager import OutputManager

# Per-process singleton for validation manager and DB managers in worker processes
_WORKER_VALIDATION_MANAGER = None
_WORKER_DB_MANAGER = None
_WORKER_MAPPING_DB_MANAGER = None

# Global cache for main process - built once and reused
_MAIN_PROCESS_MAPPING_CACHE = None
_CACHE_BUILD_LOCK = threading.Lock()


def safe_queue_size(queue):
    """Safe wrapper for queue.qsize() that handles NotImplementedError on macOS"""
    try:
        return queue.qsize()
    except NotImplementedError:
        return "N/A (qsize not supported on this platform)"
    except Exception:
        return "Unknown"

def check_queue_has_space(queue, max_size):
    """
    Check if queue has space by attempting a test put/get operation
    This works on all platforms including macOS where qsize() is not supported
    """
    try:
        # Try to put a test item
        test_item = "test_space_check"
        queue.put(test_item, timeout=1)
        # If successful, remove the test item
        queue.get_nowait()
        return True
    except:
        return False

def estimate_queue_size(queue, max_size):
    """
    Estimate queue size by attempting multiple test puts
    This is more accurate than single test put for macOS
    """
    try:
        test_items = []
        # Try to put multiple test items to estimate current size
        for i in range(max_size + 1):
            try:
                test_item = f"test_size_check_{i}"
                queue.put(test_item, timeout=0.1)
                test_items.append(test_item)
            except:
                break
        
        # Remove all test items
        for test_item in test_items:
            try:
                queue.get_nowait()
            except:
                break
                
        return len(test_items)
    except:
        return 0


class TableProcessingResult:
    """Data class for table processing results - designed to be pickleable for multiprocessing"""
    def __init__(self, table_name: str, columns_data: List[Dict[str, Any]], 
                 llm_results: List[PHIClassificationResult], 
                 validation_results: List[Dict[str, Any]] = None):
        self.table_name = table_name
        self.columns_data = columns_data
        self.llm_results = llm_results
        self.validation_results = validation_results or []
        self.completed = False
        self.timestamp = datetime.now()
    
    def __getstate__(self):
        """Custom pickling to ensure all attributes are serializable"""
        state = self.__dict__.copy()
        # Convert datetime to string for pickling
        state['timestamp'] = self.timestamp.isoformat()
        return state
    
    def __setstate__(self, state):
        """Custom unpickling to restore datetime object"""
        self.__dict__.update(state)
        # Convert timestamp back to datetime
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)


def validate_single_column_worker(config: dict, db_config: dict, mapping_config: dict, task: Dict[str, Any]) -> Tuple[bool, str, str]:
    """
    Standalone worker function for validating a single column (runs in separate process)
    This function is defined at module level to be pickleable.
    """
    try:
        # Ensure logging is initialized in child process and routed to consumer log
        logging.basicConfig(level=logging.INFO, force=True)
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
        consumer_file = logging.FileHandler('phi_pipeline_consumer.log')
        consumer_file.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(consumer_file)
        root_logger.addHandler(stream_handler)
        # Reuse per-process singletons to avoid re-initializing and reloading caches in the same worker
        global _WORKER_VALIDATION_MANAGER, _WORKER_DB_MANAGER, _WORKER_MAPPING_DB_MANAGER
        if _WORKER_DB_MANAGER is None:
            _WORKER_DB_MANAGER = DatabaseManager(db_config)
        if _WORKER_MAPPING_DB_MANAGER is None:
            _WORKER_MAPPING_DB_MANAGER = DatabaseManager(mapping_config)
        if _WORKER_VALIDATION_MANAGER is None:
            _WORKER_VALIDATION_MANAGER = PHIValidationToolsManager(
                config,
                _WORKER_DB_MANAGER,
                _WORKER_MAPPING_DB_MANAGER
            )
        validation_manager = _WORKER_VALIDATION_MANAGER
        db_name = config.get('database', {}).get('database_name', '')
        validation_passed, validator_pipeline_remark, validator_rule_detected = validation_manager.validate_phi_type(
            task['phi_type'], 
            db_name, 
            task['table_name'], 
            task['column_name'], 
            task['llm_remarks']
        )
        # Do not close connections here; reused by this worker process for subsequent tasks
        return validation_passed, validator_pipeline_remark, validator_rule_detected
    except Exception as e:
        return False, f"VALIDATION_ERROR: {str(e)}", None


def producer_worker_process(config_dict: dict, db_config: dict, tables: List[str], 
                          stop_event: MPEvent, producer_done: MPEvent,
                          stats_queue: MPQueue, json_dir: str) -> None:
    """
    Producer worker process that performs LLM classification
    This function runs in a separate process for true parallelism
    """
    try:
        import sys  # Add this import for platform detection
        
        # Initialize logging in producer process with proper file handler
        logging.basicConfig(level=logging.INFO, force=True)
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
        
        # Create producer-specific file handler
        producer_file = logging.FileHandler('phi_pipeline_producer.log')
        producer_file.setFormatter(formatter)
        
        # Create stream handler for console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        # Get root logger and add handlers
        root_logger = logging.getLogger()
        root_logger.handlers = []
        root_logger.addHandler(producer_file)
        root_logger.addHandler(stream_handler)
        
        logger = logging.getLogger(f"Producer-{os.getpid()}")
        logger.info("Producer process started")
        
        # Initialize producer-specific resources
        db_manager = DatabaseManager(db_config)
        llm_agent = LLMAgent(config_dict)
        
        processed_tables = 0
        processed_columns = 0
        errors = 0
        
        for table_idx, table_name in enumerate(tables):
            if stop_event.is_set():
                logger.info("Stop event received, producer exiting")
                break
            
            try:
                logger.info(f"Producer processing table {table_idx + 1}/{len(tables)}: {table_name}")
                
                # Get columns for the table
                columns = db_manager.get_table_columns(table_name)
                if not columns:
                    logger.warning(f"No columns found for table: {table_name}")
                    continue
                
                # Prepare column data for LLM processing
                column_data = []
                for column in columns:
                    sample_size = config_dict.get('pipeline', {}).get('sample_size', 15)
                    sample_values = db_manager.get_sample_values(table_name, column['name'], sample_size)
                    sample_values = [str(val)[:100] for val in sample_values] if sample_values else []
                    
                    column_data.append({
                        'table_name': table_name,
                        'column_name': column['name'],
                        'sample_values': sample_values,
                        'column_info': column
                    })

                # Perform LLM classification for all columns at once
                try:
                    llm_results = _classify_columns_in_process(llm_agent, column_data)
                    processed_columns += len(llm_results)
                except Exception as e:
                    logger.error(f"LLM batch classification failed for table {table_name}: {str(e)}")
                    llm_results = [
                        PHIClassificationResult(is_phi='no', phi_type='', remarks="") 
                        for _ in column_data
                    ]
                    errors += len(column_data)

                
                # Save LLM results to JSON file
                json_path = os.path.join(json_dir, f"{table_name}.json")
                json_data = {
                    "table_name": table_name,
                    "columns": {}
                }
                for col_data, result in zip(column_data, llm_results):
                    json_data["columns"][col_data["column_name"]] = {
                        "is_phi": result.is_phi,
                        "phi_type": result.phi_type,
                        "remarks": result.remarks,
                        "confidence": getattr(result, "confidence", None)
                    }
                with open(json_path, "w") as f:
                    json.dump(json_data, f, indent=2, default=str)
                logger.info(f"Saved LLM results to {json_path}")
                
                processed_tables += 1
                
            except Exception as e:
                logger.error(f"Producer error processing table {table_name}: {str(e)}")
                errors += 1
                continue
        
        # Send final stats
        stats_queue.put({
            'producer_tables': processed_tables,
            'producer_columns': processed_columns,
            'producer_errors': errors
        })
        
        # Signal producer completion
        producer_done.set()
        logger.info(f"Producer process completed - Tables: {processed_tables}, Columns: {processed_columns}, Errors: {errors}")
        
    except Exception as e:
        logger.error(f"Producer process failed: {str(e)}")
        producer_done.set()
    finally:
        try:
            # Cleanup database connections
            if 'db_manager' in locals() and db_manager is not None:
                try:
                    db_manager.close_connection()
                    logger.debug("Producer DB connection closed")
                except Exception as e:
                    logger.error(f"Error closing producer DB connection: {str(e)}")
                    
            logger.info("Producer process cleanup completed")
            
        except Exception as cleanup_error:
            logger.error(f"Error during producer cleanup: {str(cleanup_error)}")
            pass

def consumer_worker_process(config_dict: dict, db_config: dict, mapping_config: dict,
                          stop_event: MPEvent, producer_done: MPEvent,
                          consumer_done: MPEvent, stats_queue: MPQueue, mapping_cache: dict, json_dir: str) -> None:
    """
    Consumer worker process that performs validation and generates outputs
    This function runs in a separate process for true parallelism
    """
    # Replace the consumer process logging initialization (around line 380-390 in your script) with this:

    try:
        # Initialize logging in consumer process with proper configuration
        logging.basicConfig(level=logging.INFO, force=True)
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')
        
        # Create consumer-specific file handler
        consumer_file = logging.FileHandler('phi_pipeline_consumer.log')
        consumer_file.setFormatter(formatter)
        
        # Create stream handler for console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        # Get root logger and add handlers
        root_logger = logging.getLogger()
        root_logger.handlers = []  # Clear existing handlers
        root_logger.addHandler(consumer_file)
        root_logger.addHandler(stream_handler)
        root_logger.setLevel(logging.INFO)
        
        logger = logging.getLogger(f"Consumer-{os.getpid()}")
        logger.info("Consumer process started with enhanced logging")
        logger.info(f"Consumer PID: {os.getpid()}")
        
        # Test logging immediately
        logger.info("Consumer logging test - this should appear in logs")
        
        # Initialize consumer-specific resources
        logger.info("Initializing consumer database connections...")
        db_manager = DatabaseManager(db_config)
        mapping_db_manager = DatabaseManager(mapping_config)
        logger.info("Database connections established")
        
        logger.info("Initializing consumer output manager...")
        output_manager = OutputManager(config_dict)
        logger.info("Output manager initialized")
        
        # Initialize validation pool with mapping cache
        logger.info("Initializing validation cache...")
        init_worker_mapping_cache(mapping_cache)
        logger.info("Validation cache initialized")
        
        logger.info("Creating validation manager...")
        validation_manager = PHIValidationToolsManager(config_dict, db_manager, mapping_db_manager)
        logger.info("Validation manager created")
        
        # Initialize validation worker pool
        validation_workers = config_dict.get('pipeline', {}).get('validation_workers', 1)
        validation_timeout = config_dict.get('pipeline', {}).get('validation_timeout', 3000)
        
        try:
            logger.info(f"Consumer initializing validation pool with {validation_workers} workers (timeout: {validation_timeout}s)...")
            validation_pool = multiprocessing.Pool(
                processes=validation_workers,
                initializer=init_worker_mapping_cache,
                initargs=(mapping_cache,)
            )
            logger.info("Consumer validation pool initialized successfully")
        except Exception as pool_error:
            logger.error(f"Failed to initialize validation pool: {str(pool_error)}")
            logger.error(f"Pool error traceback: {traceback.format_exc()}")
            validation_pool = None
        
        processed_tables = 0
        processed_columns = 0
        phi_columns = 0
        validation_passed = 0
        validation_failed = 0
        errors = 0
        
        logger.info("Consumer entering main processing loop")
        
        pool_restart_count = 0
        max_pool_restarts = 3
        
        completed_file = os.path.join(json_dir, 'completed_tables.json')
        failed_file = os.path.join(json_dir, 'failed_tables.json')
        
        while not stop_event.is_set():
            try:
                # Load completed and failed tables
                completed = set()
                if os.path.exists(completed_file):
                    with open(completed_file, 'r') as f:
                        completed = set(json.load(f))

                failed = []
                if os.path.exists(failed_file):
                    with open(failed_file, 'r') as f:
                        failed = json.load(f)
                
                # Find new tables
                new_tables = []
                for filename in os.listdir(json_dir):
                    if filename.endswith('.json') and filename != 'completed_tables.json' and filename != 'failed_tables.json':
                        table_name = filename[:-5]
                        if table_name not in completed:
                            new_tables.append(table_name)
                
                logger.info(f"Found {len(new_tables)} new tables to process")
                
                for table_name in sorted(new_tables):
                    json_path = os.path.join(json_dir, f"{table_name}.json")
                    if not os.path.exists(json_path):
                        continue
                    
                    try:
                        with open(json_path, 'r') as f:
                            table_data = json.load(f)
                        
                        # Reconstruct table_result
                        columns_data = [{'table_name': table_data['table_name'], 'column_name': col_name} for col_name in table_data['columns']]
                        llm_results = []
                        for col_dict in table_data['columns'].values():
                            llm_results.append(PHIClassificationResult(
                                is_phi=col_dict['is_phi'],
                                phi_type=col_dict['phi_type'],
                                remarks=col_dict['remarks'],
                                confidence=col_dict.get('confidence', None)
                            ))
                        table_result = TableProcessingResult(
                            table_name=table_data['table_name'],
                            columns_data=columns_data,
                            llm_results=llm_results
                        )
                        
                        # Check validation pool health before processing
                        if validation_pool is not None:
                            try:
                                # Quick health check - use a simple function instead of lambda
                                def health_check_func():
                                    return "health_check"
                                
                                test_result = validation_pool.apply_async(health_check_func)
                                test_result.get(timeout=5)  # 5 second timeout for health check
                            except Exception as health_error:
                                logger.warning(f"Validation pool health check failed: {str(health_error)}")
                                
                                if pool_restart_count < max_pool_restarts:
                                    logger.info(f"Restarting validation pool (attempt {pool_restart_count + 1}/{max_pool_restarts})")
                                    try:
                                        # Terminate current pool
                                        validation_pool.terminate()
                                        # Use join() without timeout parameter for compatibility
                                        validation_pool.join()
                                        
                                        # Create new pool
                                        validation_pool = multiprocessing.Pool(
                                            processes=max(2, validation_workers - pool_restart_count),  # Reduce workers on restart
                                            initializer=init_worker_mapping_cache,
                                            initargs=(mapping_cache,)
                                        )
                                        pool_restart_count += 1
                                        logger.info("Validation pool restarted successfully")
                                        
                                    except Exception as restart_error:
                                        logger.error(f"Failed to restart validation pool: {str(restart_error)}")
                                        validation_pool = None
                                else:
                                    logger.error("Max pool restart attempts reached, disabling parallel validation")
                                    validation_pool = None
                        
                        # Add per-table timeout to prevent hanging
                        table_start_time = time.time()
                        table_timeout = 3000  # 10 minutes max per table
                        
                        # Perform validation with timeout wrapper
                        validation_start_time = time.time()
                        try:
                            # Create a separate process for this table's validation if pool is problematic
                            if validation_pool is not None:
                                validation_results = _validate_table_parallel_in_process(
                                    table_result, validation_pool, validation_manager, config_dict, logger, 
                                    min(validation_timeout, table_timeout - (time.time() - table_start_time))
                                )
                            else:
                                logger.warning("Validation pool not available, using sequential validation")
                                validation_results = _validate_table_sequential_in_process(
                                    table_result, validation_manager, config_dict, logger
                                )
                            
                            validation_duration = time.time() - validation_start_time
                            logger.info(f"Validation completed for table {table_result.table_name} in {validation_duration:.2f} seconds")
                            
                        except Exception as validation_error:
                            logger.error(f"Validation failed for table {table_result.table_name}: {str(validation_error)}")
                            # Create fallback results
                            validation_results = []
                            for column_data, llm_result in zip(table_result.columns_data, table_result.llm_results):
                                validation_results.append(_create_validation_result_in_process(
                                    column_data, llm_result, False, f"VALIDATION_TIMEOUT: {str(validation_error)}", None
                                ))
                        
                        # Check if table processing took too long
                        table_duration = time.time() - table_start_time
                        if table_duration > table_timeout:
                            logger.warning(f"Table {table_result.table_name} took {table_duration:.2f}s (over {table_timeout}s limit)")
                        
                        # Process results
                        db_name = config_dict.get('database', {}).get('database_name', '')
                        for validation_result in validation_results:
                            # Add result to output manager
                            output_manager.add_result(
                                db_name=db_name,
                                table_name=validation_result['table_name'],
                                column_name=validation_result['column_name'],
                                is_phi=validation_result['is_phi'],
                                phi_rule=validation_result['phi_rule'],
                                validation_passed=validation_result['validation_passed'],
                                confidence=validation_result['confidence'],
                                pipeline_remark=validation_result['pipeline_remark'],
                                llm_phi_type=validation_result['llm_phi_type']
                            )
                            
                            # Update statistics
                            if validation_result['is_phi'] == 'yes':
                                phi_columns += 1
                            if validation_result['validation_passed'] is True:
                                validation_passed += 1
                            elif validation_result['validation_passed'] is False:
                                validation_failed += 1
                            processed_columns += 1
                        
                        # Generate output for this table
                        table_output_path = f"phi_classification_results_{db_name}.csv"
                        output_manager.generate_csv_output(table_output_path)
                        
                        processed_tables += 1
                        logger.info(f"Consumer completed validation for table: {table_result.table_name} ({processed_tables} total)")
                        
                        # Delete the JSON file on success
                        os.remove(json_path)
                        logger.info(f"Deleted processed JSON: {json_path}")
                        
                        # Update tracking
                        completed.add(table_name)
                        if table_name in failed:
                            failed.remove(table_name)
                        
                    except Exception as table_error:
                        logger.error(f"Failed to process table {table_name}: {str(table_error)}")
                        if table_name not in failed:
                            failed.append(table_name)
                        errors += 1
                        # Do not delete JSON on failure (allows retry)
                
                # Save tracking files
                with open(completed_file, 'w') as f:
                    json.dump(list(completed), f)
                with open(failed_file, 'w') as f:
                    json.dump(failed, f)
                logger.info(f"Saved tracking: {len(completed)} completed, {len(failed)} failed")
                
                # Check if done
                producer_status = "RUNNING" if not producer_done.is_set() else "DONE"
                logger.info(f"Polling complete. Completed tables: {len(completed)}, Failed: {len(failed)}, Producer: {producer_status}")
                
                if producer_done.is_set() and len(new_tables) == 0:
                    logger.info("Producer is done and no more tables to process, consumer exiting")
                    break
                
            except Exception as e:
                logger.error(f"Consumer polling error: {str(e)}")
                errors += 1
                continue
            
            time.sleep(15)
        
        # Generate final output
        try:
            csv_path = output_manager.generate_csv_output()
            logger.info(f"Generated final output: {csv_path}")
        except Exception as e:
            logger.error(f"Failed to generate output: {str(e)}")
        
        # Send final stats
        stats_queue.put({
            'consumer_tables': processed_tables,
            'consumer_columns': processed_columns,
            'consumer_phi_columns': phi_columns,
            'consumer_validation_passed': validation_passed,
            'consumer_validation_failed': validation_failed,
            'consumer_errors': errors
        })
        
        consumer_done.set()
        logger.info(f"Consumer process completed - Tables: {processed_tables}, Columns: {processed_columns}")
        
    except Exception as e:
        logger.error(f"Consumer process failed: {str(e)}")
        consumer_done.set()
    finally:
        try:
            # Cleanup validation pool properly
            if 'validation_pool' in locals() and validation_pool is not None:
                logger.info("Cleaning up validation pool...")
                try:
                    validation_pool.close()
                    validation_pool.join()
                    logger.info("✅ Validation pool cleaned up successfully")
                except Exception as e:
                    logger.error(f"Error cleaning up validation pool: {str(e)}")
            
            # Cleanup database connections
            if 'db_manager' in locals() and db_manager is not None:
                try:
                    db_manager.close_connection()
                    logger.debug("Consumer DB connection closed")
                except Exception as e:
                    logger.error(f"Error closing consumer DB connection: {str(e)}")
                    
            if 'mapping_db_manager' in locals() and mapping_db_manager is not None:
                try:
                    mapping_db_manager.close_connection()
                    logger.debug("Consumer mapping DB connection closed")
                except Exception as e:
                    logger.error(f"Error closing consumer mapping DB connection: {str(e)}")
                    
            logger.info("Consumer process cleanup completed")
            
        except Exception as cleanup_error:
            logger.error(f"Error during consumer cleanup: {str(cleanup_error)}")
            pass


def _classify_columns_in_process(llm_agent: LLMAgent, column_data: List[Dict[str, Any]]) -> List[PHIClassificationResult]:
    """Helper function for batch LLM classification in producer process"""
    try:
        results = llm_agent.classify_columns(column_data)
        return results
    except Exception as e:
        return [PHIClassificationResult(is_phi='no', phi_type='', remarks="") for _ in column_data]



def _validate_table_parallel_in_process(table_result: TableProcessingResult, validation_pool,
                                       validation_manager, config_dict: dict, logger, timeout: int = 3000) -> List[Dict[str, Any]]:
    """Helper function to validate table results in consumer process with better error handling"""
    try:
        validation_tasks = []
        # Prepare validation tasks for PHI columns only
        for i, (column_data, llm_result) in enumerate(zip(table_result.columns_data, table_result.llm_results)):
            if llm_result.is_phi == 'yes' and llm_result.phi_type:
                validation_tasks.append({
                    'index': i,
                    'table_name': column_data['table_name'],
                    'column_name': column_data['column_name'],
                    'phi_type': llm_result.phi_type,
                    'llm_remarks': llm_result.remarks
                })

        validation_results = [None] * len(table_result.columns_data)

        if validation_tasks:
            # Use parallel validation with enhanced error handling
            db_config = {"database": config_dict.get('database', {})}
            mapping_db_config = config_dict.get('database', {}).copy()
            mapping_db_config['database_name'] = config_dict.get('mapping_table', {}).get('database_name', '')
            mapping_config = {"database": mapping_db_config}
            
            args = [
                (config_dict, db_config, mapping_config, task)
                for task in validation_tasks
            ]
            
            try:
                logger.info(f"Starting parallel validation for {len(validation_tasks)} columns with {timeout}s timeout")
                
                # Batch process to avoid overwhelming the pool
                batch_size = min(3, len(validation_tasks))  # Process 3 at a time max
                all_results = []
                
                for i in range(0, len(args), batch_size):
                    batch_args = args[i:i+batch_size]
                    batch_tasks = validation_tasks[i:i+batch_size]
                    
                    try:
                        logger.info(f"Processing validation batch {i//batch_size + 1}: {len(batch_args)} tasks")
                        
                        # Use map_async with shorter timeout for each batch
                        batch_timeout = min(timeout, 3000)  # Max 2 minutes per batch
                        async_result = validation_pool.starmap_async(validate_single_column_worker, batch_args)
                        
                        # Wait for batch results with timeout
                        batch_results = async_result.get(timeout=batch_timeout)
                        all_results.extend(list(zip(batch_tasks, batch_results)))
                        
                        logger.info(f"Batch {i//batch_size + 1} completed successfully")
                        
                    except MPTimeoutError:
                        logger.error(f"Batch {i//batch_size + 1} timed out after {batch_timeout}s, using sequential fallback")
                        # Cancel the async operation
                        try:
                            async_result.cancel()
                        except:
                            pass
                        
                        # Fall back to sequential for this batch
                        for task in batch_tasks:
                            try:
                                validation_passed, validator_pipeline_remark, validator_rule_detected = validation_manager.validate_phi_type(
                                    task['phi_type'],
                                    config_dict.get('database', {}).get('database_name', ''),
                                    task['table_name'],
                                    task['column_name'],
                                    task['llm_remarks']
                                )
                                all_results.append((task, (validation_passed, validator_pipeline_remark, validator_rule_detected)))
                            except Exception as ve:
                                logger.error(f"Sequential validation failed for {task['table_name']}.{task['column_name']}: {str(ve)}")
                                all_results.append((task, (False, f"VALIDATION_ERROR: {str(ve)}", None)))
                    
                    except Exception as batch_error:
                        logger.error(f"Batch {i//batch_size + 1} failed: {str(batch_error)}")
                        # Fall back to sequential for failed batch
                        for task in batch_tasks:
                            try:
                                validation_passed, validator_pipeline_remark, validator_rule_detected = validation_manager.validate_phi_type(
                                    task['phi_type'],
                                    config_dict.get('database', {}).get('database_name', ''),
                                    task['table_name'],
                                    task['column_name'],
                                    task['llm_remarks']
                                )
                                all_results.append((task, (validation_passed, validator_pipeline_remark, validator_rule_detected)))
                            except Exception as ve:
                                logger.error(f"Sequential fallback failed for {task['table_name']}.{task['column_name']}: {str(ve)}")
                                all_results.append((task, (False, f"VALIDATION_ERROR: {str(ve)}", None)))
                
                # Process all results
                for task, (validation_passed, validator_pipeline_remark, validator_rule_detected) in all_results:
                    llm_result = table_result.llm_results[task['index']]
                    column_data = table_result.columns_data[task['index']]
                    validation_result = _create_validation_result_in_process(
                        column_data,
                        llm_result,
                        validation_passed,
                        validator_pipeline_remark,
                        validator_rule_detected
                    )
                    validation_results[task['index']] = validation_result
                        
            except Exception as e:
                logger.error(f"Parallel validation completely failed: {str(e)}")
                # Fallback to sequential validation for all tasks
                logger.info("Falling back to sequential validation for all tasks")
                for task in validation_tasks:
                    try:
                        validation_passed, validator_pipeline_remark, validator_rule_detected = validation_manager.validate_phi_type(
                            task['phi_type'],
                            config_dict.get('database', {}).get('database_name', ''),
                            task['table_name'],
                            task['column_name'],
                            task['llm_remarks']
                        )
                        llm_result = table_result.llm_results[task['index']]
                        column_data = table_result.columns_data[task['index']]
                        validation_result = _create_validation_result_in_process(
                            column_data,
                            llm_result,
                            validation_passed,
                            validator_pipeline_remark,
                            validator_rule_detected
                        )
                        validation_results[task['index']] = validation_result
                    except Exception as ve:
                        logger.error(f"Sequential validation failed for {task['table_name']}.{task['column_name']}: {str(ve)}")
                        llm_result = table_result.llm_results[task['index']]
                        column_data = table_result.columns_data[task['index']]
                        validation_result = _create_validation_result_in_process(
                            column_data,
                            llm_result,
                            False,
                            f"VALIDATION_ERROR: {str(ve)}",
                            None
                        )
                        validation_results[task['index']] = validation_result

        # Fill in non-PHI columns
        for i, (column_data, llm_result) in enumerate(zip(table_result.columns_data, table_result.llm_results)):
            if validation_results[i] is None:
                validation_results[i] = _create_validation_result_in_process(column_data, llm_result, None, None, None)

        return validation_results

    except Exception as e:
        logger.error(f"Validation failed for table {table_result.table_name}: {str(e)}")
        return [_create_validation_result_in_process(column_data, llm_result, None, None, None) 
               for column_data, llm_result in zip(table_result.columns_data, table_result.llm_results)]


def _validate_table_sequential_in_process(table_result: TableProcessingResult, validation_manager,
                                         config_dict: dict, logger) -> List[Dict[str, Any]]:
    """Sequential validation fallback for consumer process"""
    try:
        logger.info(f"Using sequential validation for table: {table_result.table_name}")
        validation_results = []
        
        for column_data, llm_result in zip(table_result.columns_data, table_result.llm_results):
            if llm_result.is_phi == 'yes' and llm_result.phi_type:
                try:
                    validation_passed, validator_pipeline_remark, validator_rule_detected = validation_manager.validate_phi_type(
                        llm_result.phi_type,
                        config_dict.get('database', {}).get('database_name', ''),
                        column_data['table_name'],
                        column_data['column_name'],
                        llm_result.remarks
                    )
                    
                    validation_result = _create_validation_result_in_process(
                        column_data, llm_result, validation_passed, validator_pipeline_remark, validator_rule_detected
                    )
                    validation_results.append(validation_result)
                    
                except Exception as e:
                    logger.error(f"Sequential validation failed for {column_data['table_name']}.{column_data['column_name']}: {str(e)}")
                    validation_result = _create_validation_result_in_process(
                        column_data, llm_result, False, f"VALIDATION_ERROR: {str(e)}", None
                    )
                    validation_results.append(validation_result)
            else:
                # Non-PHI column
                validation_result = _create_validation_result_in_process(column_data, llm_result, None, None, None)
                validation_results.append(validation_result)
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Sequential validation failed for table {table_result.table_name}: {str(e)}")
        return [_create_validation_result_in_process(column_data, llm_result, None, None, None) 
               for column_data, llm_result in zip(table_result.columns_data, table_result.llm_results)]


def _create_validation_result_in_process(column_data: Dict[str, Any], llm_result: PHIClassificationResult,
                                        validation_passed: bool, validator_pipeline_remark: str,
                                        validator_rule_detected: str) -> Dict[str, Any]:
    """Create a validation result dictionary in process"""
    if validation_passed:
        is_phi = "yes"
        phi_type = validator_rule_detected
    else:
        is_phi = "no"
        phi_type = None
    return {
        'table_name': column_data['table_name'],
        'column_name': column_data['column_name'],
        'is_phi': is_phi,
        'phi_rule': phi_type,
        'validation_passed': validation_passed,
        'confidence': llm_result.confidence,
        'pipeline_remark': validator_pipeline_remark if validator_pipeline_remark else llm_result.remarks,
        'llm_phi_type': llm_result.phi_type
    }


class PHIDeidentificationPipelineOptimized:
    """Process-separated pipeline for PHI de-identification analysis with true parallel producer-consumer pattern"""
    
    def __init__(self, client_id: int, dump_id: int, llm_config: dict,json_dir: Optional[str] = None):
        try:
            # Initialize configuration
            self.client_obj = Clients.objects.get(id=client_id)
            self.dump_obj = ClientDataDump.objects.get(id=dump_id)
            self.llm_config = llm_config
            
            # Setup logging
            self.logger = logging.getLogger(__name__)
            
            self.logger.info("Initializing Optimized PHI De-identification Pipeline")
            
            # Initialize components
            self.db_manager = DatabaseManager(self.dump_obj.get_source_db_connection_str())
            self.mapping_db_manager = DatabaseManager(self.dump_obj.get_mapping_db_connection_str())
            self.llm_agent = LLMAgent(self.llm_config)
            self.output_manager = OutputManager(self.llm_config)
            
            # Initialize validation manager once during startup (after cache is built)
            self.validation_manager = None
            
            # JSON directory setup
            if json_dir is None:
                json_dir = './phi_llm_results'
            self.json_dir = json_dir
            os.makedirs(self.json_dir, exist_ok=True)
            self.completed_file = os.path.join(self.json_dir, 'completed_tables.json')
            
            self.logger.info(f"JSON directory set to: {self.json_dir}")
            
            # Build mapping cache once in main process
            self._ensure_mapping_cache_built()
            
            # Pipeline statistics
            self.stats = {
                'start_time': None,
                'end_time': None,
                'total_tables': 0,
                'total_columns': 0,
                'phi_columns': 0,
                'non_phi_columns': 0,
                'validation_passed': 0,
                'validation_failed': 0,
                'errors': 0,
                'parallel_validations': 0,
                'sequential_validations': 0,
                'pool_recoveries': 0
            }

            # Producer-Consumer setup with processes (enhanced for macOS)
            self.validation_workers = 1  # Reduced from 5 for better stability on macOS
            self.validation_timeout = 3000  # Reduced from 1000 to 5 minutes
            self.stats_queue = MPQueue()
            
            # macOS-specific optimizations
            if sys.platform == 'darwin':  # macOS
                self.validation_workers = 1  # Further reduce for macOS
                self.validation_timeout = 3000  # 3 minutes for macOS
                self.logger.info("Applied macOS-specific optimizations")
            
            # Pipeline configuration overrides
            pipeline_config = self.config.get('pipeline', {})
            pipeline_config['validation_timeout'] = self.validation_timeout
            pipeline_config['validation_workers'] = self.validation_workers
            
            # Queue monitoring for platforms where qsize() is not supported (like macOS)
            self.queue_operations = {
                'puts': 0,
                'gets': 0,
                'estimated_size': 0
            }
            
            # Process controls
            self.producer_process = None
            self.consumer_process = None
            self.stop_event = MPEvent()
            self.producer_done = MPEvent()
            self.consumer_done = MPEvent()
            
            # Results storage (not needed for process separation but kept for compatibility)
            self.table_results = {}
            self.results_lock = threading.Lock()
            
            # Process separation removes need for shared validation pool in main process
            # Each process will have its own resources
            self.validation_pool = None
            self.validation_manager = None
            
            self.logger.info("Optimized pipeline initialization completed successfully")
            
        except Exception as e:
            print(f"Failed to initialize optimized pipeline: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
    
    def _ensure_mapping_cache_built(self) -> None:
        """Ensure mapping cache is built once in main process"""
        global _MAIN_PROCESS_MAPPING_CACHE, _CACHE_BUILD_LOCK
        
        if _MAIN_PROCESS_MAPPING_CACHE is not None:
            self.logger.info("Mapping cache already built in main process")
            return
            
        with _CACHE_BUILD_LOCK:
            # Double-check inside lock
            if _MAIN_PROCESS_MAPPING_CACHE is not None:
                self.logger.info("Mapping cache already built in main process (after lock)")
                return
                
            self.logger.info("Building mapping cache in main process - this should happen only once")
            try:
                _MAIN_PROCESS_MAPPING_CACHE = build_mapping_cache(self.config, self.mapping_db_manager)
                
                if _MAIN_PROCESS_MAPPING_CACHE is None:
                    raise Exception("build_mapping_cache returned None")
                
                self.logger.info("✅ Mapping cache successfully built in main process")
                
                # Log cache statistics
                patient_cache = _MAIN_PROCESS_MAPPING_CACHE.get('patient', {})
                encounter_cache = _MAIN_PROCESS_MAPPING_CACHE.get('encounter')
                
                patient_stats = {col: len(values) for col, values in patient_cache.items()}
                encounter_count = len(encounter_cache) if encounter_cache else 0
                
                self.logger.info(f"Cache stats - Patient columns: {patient_stats}, Encounter IDs: {encounter_count}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to build mapping cache: {str(e)}")
                self.logger.error(f"Exception traceback: {traceback.format_exc()}")
                _MAIN_PROCESS_MAPPING_CACHE = {"loaded": True, "patient": {}, "encounter": None}
                self.logger.warning("Using empty fallback cache")
    
    # Validation pool initialization removed - now handled by consumer process
    
    # Validation manager initialization removed - now handled by consumer process
    
    def run_pipeline(self) -> str:
        """
        Run the complete optimized PHI de-identification pipeline
        
        Returns:
            Path to the generated CSV output file
        """
        try:
            self.stats['start_time'] = datetime.now()
            self.logger.info("Starting optimized PHI de-identification pipeline")
            
            # Get all tables to analyze
            tables = self._get_tables_to_analyze()
            self.stats['total_tables'] = len(tables)
            
            self.logger.info(f"Analyzing {len(tables)} tables with producer-consumer pattern")
            
            # Start producer and consumer threads
            self._start_producer_consumer_threads(tables)
            
            # Wait for completion
            self._wait_for_completion()
            
            # Generate final outputs
            output_path = self._generate_outputs()
            
            # Log final statistics
            self._log_final_statistics()
            
            self.logger.info(f"Optimized pipeline completed successfully. Output: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"Optimized pipeline execution failed: {str(e)}")
            traceback.print_exc()
            raise
        finally:
            # Ensure end_time is set for statistics
            if self.stats.get('end_time') is None:
                self.stats['end_time'] = datetime.now()
            # Always perform cleanup
            self._cleanup()
    
    def _get_tables_to_analyze(self) -> List[str]:
        """
        Get list of tables to analyze
        
        Returns:
            List of table names to analyze
        """
        try:
            all_tables = self.db_manager.get_all_tables()
            
            # excluded_tables = self.config_manager.get_excluded_tables()
            
            # Filter out excluded tables
            # tables_to_analyze = [table for table in all_tables if table not in excluded_tables]
            
            self.logger.info(f"Found {len(all_tables)} total tables, analyzing {len(all_tables)} tables")
            return all_tables
            
        except Exception as e:
            self.logger.error(f"Failed to get tables to analyze: {str(e)}")
            raise
    
    def _start_producer_consumer_threads(self, tables: List[str]) -> None:
        """Start producer and consumer processes (renamed for compatibility but now uses processes)"""
        try:
            # Set multiprocessing start method for compatibility
            try:
                if multiprocessing.get_start_method(allow_none=True) is None:
                    multiprocessing.set_start_method('spawn', force=True)
                    self.logger.info("Set multiprocessing start method to 'spawn'")
            except RuntimeError as e:
                self.logger.info(f"Multiprocessing start method already set: {e}")
            
            # Start producer process
            self.logger.info("Starting producer process...")
            self.producer_process = Process(
                target=producer_worker_process,
                args=(
                    self.config,
                    self.config_manager.get_db_config(),
                    tables,
                    self.stop_event,
                    self.producer_done,
                    self.stats_queue,
                    self.json_dir
                ),
                name="ProducerProcess"
            )
            self.producer_process.start()
            
            # Verify producer started
            time.sleep(1)  # Give process time to start
            if self.producer_process.is_alive():
                self.logger.info(f"✅ Producer process started successfully (PID: {self.producer_process.pid})")
            else:
                self.logger.error("❌ Producer process failed to start")
                raise Exception("Producer process failed to start")
            
            # Start consumer process
            self.logger.info("Starting consumer process...")
            self.consumer_process = Process(
                target=consumer_worker_process,
                args=(
                    self.config,
                    self.config_manager.get_db_config(),
                    self.config_manager.get_mapping_config(),
                    self.stop_event,
                    self.producer_done,
                    self.consumer_done,
                    self.stats_queue,
                    _MAIN_PROCESS_MAPPING_CACHE,
                    self.json_dir
                ),
                name="ConsumerProcess"
            )
            self.consumer_process.start()
            
            # Verify consumer started
            time.sleep(1)  # Give process time to start
            if self.consumer_process.is_alive():
                self.logger.info(f"✅ Consumer process started successfully (PID: {self.consumer_process.pid})")
            else:
                self.logger.error("❌ Consumer process failed to start")
                raise Exception("Consumer process failed to start")
            
            self.logger.info(f"Both processes started successfully - Producer PID: {self.producer_process.pid}, Consumer PID: {self.consumer_process.pid}")
            
        except Exception as e:
            self.logger.error(f"Failed to start producer-consumer processes: {str(e)}")
            raise
    
    # Old thread-based producer worker removed - now using producer_worker_process function
    
    # Parallel column classification removed - now handled by producer process
    
    def _classify_single_column(self, table_name: str, column_name: str, sample_values: List[Any]) -> PHIClassificationResult:
        """
        Classify a single column using LLM agent
        """
        try:
            result = self.llm_agent.classify_column(column_name, sample_values, table_name)
            self.logger.debug(f"LLM classification for {table_name}.{column_name}: {result.is_phi}, {result.phi_type}")
            return result
            
        except Exception as e:
            self.logger.error(f"LLM classification failed for {table_name}.{column_name}: {str(e)}")
            return PHIClassificationResult(is_phi='no', phi_type='', remarks="")
    
    # Old thread-based consumer worker removed - now using consumer_worker_process function
    
    # Parallel table validation removed - now handled by consumer process

    # Sequential validation removed - now handled by consumer process

    def _create_validation_result(self, column_data: Dict[str, Any], llm_result: PHIClassificationResult,
                                validation_passed: bool, validator_pipeline_remark: str,
                                validator_rule_detected: str) -> Dict[str, Any]:
        """
        Create a validation result dictionary
        """
        if validation_passed:
            is_phi = "yes"
            phi_type = validator_rule_detected
        else:
            is_phi = "no"
            phi_type = None
        return {
            'table_name': column_data['table_name'],
            'column_name': column_data['column_name'],
            'is_phi': is_phi,
            'phi_rule': phi_type,
            'validation_passed': validation_passed,
            'confidence': llm_result.confidence,
            'pipeline_remark': validator_pipeline_remark if validator_pipeline_remark else llm_result.remarks,
            'llm_phi_type': llm_result.phi_type
        }
    
    # Table results processing removed - now handled by consumer process
    
    # Table output generation removed - now handled by consumer process
    
    def _wait_for_completion(self) -> None:
        """Wait for producer and consumer processes to complete"""
        try:
            self.logger.info("Waiting for processes to complete...")
            
            # Monitor processes with periodic status updates
            check_interval = 30  # Check every 30 seconds
            total_wait_time = 0
            max_wait_time = 3600  # 1 hour maximum
            
            while total_wait_time < max_wait_time:
                # Check producer status
                producer_alive = self.producer_process.is_alive() if self.producer_process else False
                consumer_alive = self.consumer_process.is_alive() if self.consumer_process else False
                
                self.logger.info(f"Process status check (after {total_wait_time}s): Producer={producer_alive}, Consumer={consumer_alive}")
                
                # If producer is done, check if consumer is also done
                if not producer_alive:
                    self.logger.info("Producer process completed")
                    # Give consumer some time to finish processing remaining items
                    if not consumer_alive:
                        self.logger.info("Consumer process also completed")
                        break
                else:
                        self.logger.info("Waiting for consumer to finish processing...")
                
                # Wait before next check
                time.sleep(check_interval)
                total_wait_time += check_interval
            
            # Final join with timeout handling
            if self.producer_process and self.producer_process.is_alive():
                self.logger.info("Joining producer process...")
                try:
                    # Use a simple join without timeout for compatibility
                    self.producer_process.join()
                except Exception as e:
                    self.logger.warning(f"Producer process join failed: {str(e)}, terminating...")
                    self.producer_process.terminate()
            
            if self.consumer_process and self.consumer_process.is_alive():
                self.logger.info("Joining consumer process...")
                try:
                    # Use a simple join without timeout for compatibility
                    self.consumer_process.join()
                except Exception as e:
                    self.logger.warning(f"Consumer process join failed: {str(e)}, terminating...")
                    self.consumer_process.terminate()
            
            # Collect final statistics from processes
            self._collect_final_statistics()
            
            self.logger.info("All processes completed")
            
        except Exception as e:
            self.logger.error(f"Error waiting for completion: {str(e)}")
            traceback.print_exc()
    
    def _collect_final_statistics(self) -> None:
        """Collect final statistics from worker processes"""
        try:
            # Collect stats from processes
            while not self.stats_queue.empty():
                try:
                    stats = self.stats_queue.get_nowait()
                    for key, value in stats.items():
                        if key.startswith('producer_') or key.startswith('consumer_'):
                            base_key = key.split('_', 1)[1]  # Remove prefix
                            if base_key in self.stats:
                                self.stats[base_key] += value
                            else:
                                self.stats[base_key] = value
                except:
                    break
            
        except Exception as e:
            self.logger.error(f"Error collecting final statistics: {str(e)}")
    
    def _cleanup(self) -> None:
        """Cleanup resources and prevent resource leaks"""
        try:
            # Signal stop
            self.stop_event.set()
            
            # Cleanup processes properly to prevent resource leaks
            processes_to_cleanup = []
            
            if self.producer_process:
                processes_to_cleanup.append(("producer", self.producer_process))
            if self.consumer_process:
                processes_to_cleanup.append(("consumer", self.consumer_process))
            
            for process_name, process in processes_to_cleanup:
                if process.is_alive():
                    self.logger.info(f"Terminating {process_name} process...")
                    try:
                        # First try graceful termination
                        process.terminate()
                        process.join()
                        
                        # If still alive, force kill
                        if process.is_alive():
                            self.logger.warning(f"{process_name} process did not terminate gracefully, forcing kill...")
                            process.kill()
                            process.join()
                        
                        if process.is_alive():
                            self.logger.error(f"{process_name} process could not be terminated")
                        else:
                            self.logger.info(f"✅ {process_name} process terminated successfully")
                            
                    except Exception as e:
                        self.logger.error(f"Error terminating {process_name} process: {str(e)}")
            
            # Clean up queues to prevent semaphore leaks
            try:
                # Clean up stats queue
                while True:
                    try:
                        self.stats_queue.get_nowait()
                    except:
                        break
                        
                if hasattr(self.stats_queue, 'close'):
                    self.stats_queue.close()
                if hasattr(self.stats_queue, 'join_thread'):
                    self.stats_queue.join_thread()
                    
            except Exception as e:
                self.logger.debug(f"Stats queue cleanup error (non-critical): {str(e)}")
            
            # Close database connections in main process
            if self.db_manager:
                try:
                    self.db_manager.close_connection()
                except Exception as e:
                    self.logger.debug(f"DB manager cleanup error: {str(e)}")
                    
            if self.mapping_db_manager:
                try:
                    self.mapping_db_manager.close_connection()
                except Exception as e:
                    self.logger.debug(f"Master DB manager cleanup error: {str(e)}")
            
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            # Continue cleanup even if there are errors
            pass
    
    def _generate_outputs(self) -> str:
        """
        Generate final output files (now handled by consumer process)
        
        Returns:
            Path to main CSV output file
        """
        try:
            self.logger.info("Final output generation handled by consumer process")
            
            # Generate output path (consumer handles actual file generation)
            db_name = self.config.get('database', {}).get('database_name', '')
            csv_path = f"phi_classification_results_{db_name}.csv"
            
            self.logger.info(f"Expected output file: {csv_path}")
            
            return csv_path
            
        except Exception as e:
            self.logger.error(f"Failed to determine output path: {str(e)}")
            raise
    
    def _log_final_statistics(self) -> None:
        """Log final pipeline statistics"""
        try:
            # Ensure end_time is set
            if self.stats.get('end_time') is None:
                self.stats['end_time'] = datetime.now()
            
            # Calculate duration safely
            start_time = self.stats.get('start_time')
            end_time = self.stats.get('end_time')
            
            if start_time and end_time:
                duration = (end_time - start_time).total_seconds()
                duration_str = f"{duration:.2f} seconds"
            else:
                duration_str = "Unknown (timing data incomplete)"
            
            self.logger.info("=" * 60)
            self.logger.info("PROCESS-SEPARATED PIPELINE EXECUTION SUMMARY")
            self.logger.info("=" * 60)
            self.logger.info(f"Execution Time: {duration_str}")
            self.logger.info(f"Tables Processed: {self.stats.get('tables', 0)}")
            self.logger.info(f"Columns Analyzed: {self.stats.get('columns', 0)}")
            self.logger.info(f"PHI Columns Found: {self.stats.get('phi_columns', 0)}")
            self.logger.info(f"Validation Passed: {self.stats.get('validation_passed', 0)}")
            self.logger.info(f"Validation Failed: {self.stats.get('validation_failed', 0)}")
            self.logger.info(f"Errors Encountered: {self.stats.get('errors', 0)}")
            self.logger.info("Process Separation: ✅ Producer and Consumer in separate processes")
            self.logger.info("True Parallelism: ✅ No GIL limitations between processes")
            
            total_columns = self.stats.get('columns', 0)
            phi_columns = self.stats.get('phi_columns', 0)
            if total_columns > 0:
                phi_percentage = (phi_columns / total_columns) * 100
                self.logger.info(f"PHI Percentage: {phi_percentage:.1f}%")
            
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"Error logging final statistics: {str(e)}")
            # Log basic info without calculations
            self.logger.info("PIPELINE EXECUTION COMPLETED (statistics calculation failed)")
    
    def process_single_table(self, table_name: str) -> Dict[str, Any]:
        """
        Process a single table and return results (fallback to sequential processing)
        
        Args:
            table_name: Name of the table to process
            
        Returns:
            Dictionary containing processing results
        """
        try:
            self.logger.info(f"Processing single table: {table_name}")
            
            # Clear previous results
            self.output_manager.clear_results()
            
            # Get columns for the table
            columns = self.db_manager.get_table_columns(table_name)
            if not columns:
                self.logger.warning(f"No columns found for table: {table_name}")
                return {'table_name': table_name, 'total_columns': 0, 'phi_columns': 0, 'results': []}
            
            # Process columns sequentially for single table
            for column in columns:
                try:
                    self._process_column_sequential(table_name, column)
                except Exception as e:
                    self.logger.error(f"Error processing column {table_name}.{column['name']}: {str(e)}")
                    self.stats['errors'] += 1
                    continue
            
            # Get results
            results = self.output_manager.get_results_dataframe()
            
            return {
                'table_name': table_name,
                'total_columns': len(results),
                'phi_columns': len(results[results['is_phi'] == 'yes']),
                'results': results.to_dict('records')
            }
            
        except Exception as e:
            self.logger.error(f"Error processing single table {table_name}: {str(e)}")
            raise
    
    def _process_column_sequential(self, table_name: str, column_info: Dict[str, Any]) -> None:
        """
        Process a single column sequentially (for single table processing)
        """
        column_name = column_info['name']
        
        try:
            self.logger.debug(f"Processing column: {table_name}.{column_name}")
            
            # Get sample values
            sample_size = self.config.get('pipeline', {}).get('sample_size', 15)
            sample_values = self.db_manager.get_sample_values(table_name, column_name, sample_size)
            sample_values = [str(val)[:100] for val in sample_values] if sample_values else []
            
            # Classify with LLM
            classification_result = self._classify_single_column(table_name, column_name, sample_values)
            
            # Validate classification if PHI detected
            validation_passed = None
            validator_pipeline_remark = None
            validator_rule_detected = None
            
            if classification_result.is_phi == 'yes' and classification_result.phi_type:
                try:
                    # Use the pre-initialized validation manager
                    if self.validation_manager is None:
                        self.logger.error("Validation manager not initialized during single table processing")
                        # Fallback: create one but this will rebuild cache
                        self.validation_manager = PHIValidationToolsManager(self.config, self.db_manager, self.mapping_db_manager)
                    
                    validation_passed, validator_pipeline_remark, validator_rule_detected = self.validation_manager.validate_phi_type(
                        classification_result.phi_type, 
                        self.config.get('database', {}).get('database_name', ''), 
                        table_name, 
                        column_name, 
                        classification_result.remarks
                    )
                except Exception as e:
                    self.logger.error(f"Validation failed for {table_name}.{column_name}: {str(e)}")
                    validation_passed = False
                    validator_pipeline_remark = f"VALIDATION_ERROR: {str(e)}"
                    validator_rule_detected = None
            
            # Store result
            self.output_manager.add_result(
                db_name=self.config.get('database', {}).get('database_name', ''),
                table_name=table_name,
                column_name=column_name,
                is_phi=classification_result.is_phi,
                phi_rule=(validator_rule_detected if validator_rule_detected else classification_result.phi_type),
                validation_passed=validation_passed,
                confidence=classification_result.confidence,
                pipeline_remark=validator_pipeline_remark if validator_pipeline_remark else classification_result.remarks,
                llm_phi_type=classification_result.phi_type
            )
            
            self.logger.debug(f"Column processed: {table_name}.{column_name} -> {classification_result.is_phi}, {classification_result.phi_type}")
            
        except Exception as e:
            self.logger.error(f"Error processing column {table_name}.{column_name}: {str(e)}")
            raise
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status
        
        Returns:
            Dictionary containing pipeline status information
        """
        return {
            'stats': self.stats.copy(),
            'config_loaded': self.config is not None,
            'db_connected': self.db_manager.engine is not None,
            'results_count': self.output_manager.get_result_count(),
            'producer_done': self.producer_done.is_set(),
            'consumer_done': self.consumer_done.is_set(),
            'tables_completed': len(self.table_results)
        }


# def main():
#     """Main entry point for the process-separated optimized pipeline"""
#     try:
#         # Set multiprocessing start method for cross-platform compatibility
#         try:
#             if multiprocessing.get_start_method(allow_none=True) is None:
#                 multiprocessing.set_start_method('spawn', force=True)
#         except RuntimeError:
#             pass  # Start method already set
        
#         # Initialize and run process-separated optimized pipeline
#         pipeline = PHIDeidentificationPipelineOptimized()
#         output_file = pipeline.run_pipeline()
        
#         print(f"\nProcess-Separated PHI De-identification Pipeline completed successfully!")
#         print(f"Results saved to: {output_file}")
        
#         return 0
        
#     except KeyboardInterrupt:
#         print("\nPipeline interrupted by user")
#         return 1
#     except Exception as e:
#         print(f"\nPipeline failed: {str(e)}")
#         traceback.print_exc()
#         return 1


# if __name__ == "__main__":
#     sys.exit(main())