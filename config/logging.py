"""
Enhanced logging configuration with structured logs
"""
import logging
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Create logs directory if it doesn't exist
logs_dir = Path(__file__).resolve().parent.parent / "logs"
logs_dir.mkdir(exist_ok=True)

class StructuredLogFormatter(logging.Formatter):
    """JSON formatter for structured logs"""
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
            }
            
        # Add extra attributes if any
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
            
        return json.dumps(log_data)

class HybridFormatter(logging.Formatter):
    """Formatter that outputs human-readable logs to console and JSON to file"""
    
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)
        self.structured_formatter = StructuredLogFormatter()
        self.standard_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def format(self, record):
        # Check if the handler is a FileHandler
        if any(isinstance(h, logging.FileHandler) for h in logging.root.handlers 
               if hasattr(record, 'handler') and record.handler == h):
            return self.structured_formatter.format(record)
        else:
            return self.standard_formatter.format(record)

def configure_logging():
    """Configure and return a logger with structured output"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Console handler with human-readable format
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console)
    
    # Determine log filename with date
    today = datetime.now().strftime("%Y%m%d")
    log_file = logs_dir / f'telegram_bot_{today}.log'
    
    # File handler with JSON format
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(StructuredLogFormatter())
    logger.addHandler(file_handler)
    
    # Create a named logger for the caller
    caller_logger = logging.getLogger(__name__)
    
    return caller_logger
