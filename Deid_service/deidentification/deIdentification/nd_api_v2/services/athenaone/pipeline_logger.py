#!/usr/bin/env python3
"""
Pipeline Logger - Centralized logging utility for all pipeline operations
Provides consistent logging across all scripts with file and console output
"""

import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


class ImmediateStreamHandler(logging.StreamHandler):
    """StreamHandler that flushes after every emit for real-time output"""
    def emit(self, record):
        super().emit(record)
        self.flush()


class PipelineLogger:
    """Centralized logger for pipeline operations"""
    
    _logger = None
    _initialized = False
    
    @classmethod
    def setup(cls, log_name="pipeline", log_dir="logs"):
        """Setup logging with both file and console handlers"""
        
        if cls._initialized:
            return cls._logger
        
        # Create logs directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Create logger
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        logger.handlers = []
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(funcName)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # File handler with rotation (max 10MB, keep 5 backups)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"{log_name}_{timestamp}.log")
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
        
        # Console handler with immediate flushing
        console_handler = ImmediateStreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # Ensure console output is flushed immediately
        sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
        
        # Summary log file (only important messages)
        summary_log_file = os.path.join(log_dir, f"{log_name}_summary_{timestamp}.log")
        summary_handler = RotatingFileHandler(
            summary_log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        summary_handler.setLevel(logging.WARNING)
        summary_handler.setFormatter(detailed_formatter)
        logger.addHandler(summary_handler)
        
        cls._logger = logger
        cls._initialized = True
        
        # Log initialization
        logger.info("="*80)
        logger.info("Pipeline Logger Initialized")
        logger.info(f"Log File: {log_file}")
        logger.info(f"Summary Log: {summary_log_file}")
        logger.info("="*80)
        
        return logger
    
    @classmethod
    def get_logger(cls):
        """Get the logger instance"""
        if not cls._initialized:
            return cls.setup()
        return cls._logger
    
    @classmethod
    def log_step(cls, step_num, total_steps, step_name):
        """Log a pipeline step"""
        logger = cls.get_logger()
        logger.info("")
        logger.info("="*80)
        logger.info(f"STEP {step_num}/{total_steps}: {step_name.upper()}")
        logger.info("="*80)
        sys.stdout.flush()
    
    @classmethod
    def log_success(cls, message="Operation completed successfully"):
        """Log success message"""
        logger = cls.get_logger()
        logger.info(f"✅ {message}")
        sys.stdout.flush()
    
    @classmethod
    def log_error(cls, message):
        """Log error message"""
        logger = cls.get_logger()
        logger.error(f"❌ {message}")
        sys.stdout.flush()
    
    @classmethod
    def log_warning(cls, message):
        """Log warning message"""
        logger = cls.get_logger()
        logger.warning(f"⚠️  {message}")
    
    @classmethod
    def log_info(cls, message):
        """Log info message"""
        logger = cls.get_logger()
        logger.info(message)
        sys.stdout.flush()
    
    @classmethod
    def log_header(cls, title):
        """Log a section header"""
        logger = cls.get_logger()
        logger.info("="*80)
        logger.info(title)
        logger.info("="*80)
        sys.stdout.flush()
    
    @classmethod
    def log_summary(cls, summary_dict):
        """Log a summary dictionary"""
        logger = cls.get_logger()
        logger.info("")
        logger.info("="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        for key, value in summary_dict.items():
            logger.info(f"{key}: {value}")
        logger.info("="*80)


# Convenience functions
def get_logger(name="pipeline", log_dir="logs"):
    """Get or create a logger instance"""
    return PipelineLogger.setup(name, log_dir)


def log_step(step_num, total_steps, step_name):
    """Log a pipeline step"""
    PipelineLogger.log_step(step_num, total_steps, step_name)


def log_success(message="Operation completed successfully"):
    """Log success message"""
    PipelineLogger.log_success(message)


def log_error(message):
    """Log error message"""
    PipelineLogger.log_error(message)


def log_warning(message):
    """Log warning message"""
    PipelineLogger.log_warning(message)


def log_info(message):
    """Log info message"""
    PipelineLogger.log_info(message)


def log_header(title):
    """Log a section header"""
    PipelineLogger.log_header(title)


def log_summary(summary_dict):
    """Log a summary dictionary"""
    PipelineLogger.log_summary(summary_dict)


# Export functions
__all__ = [
    'PipelineLogger',
    'get_logger',
    'log_step',
    'log_success',
    'log_error',
    'log_warning',
    'log_info',
    'log_header',
    'log_summary'
]
