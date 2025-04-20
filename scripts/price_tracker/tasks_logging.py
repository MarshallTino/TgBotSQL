import os
import logging
import json
from datetime import datetime, timedelta
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler

# Create logs directory structure with proper permissions
logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
try:
    os.makedirs(logs_dir, exist_ok=True)
    # Ensure the directory is writable
    os.chmod(logs_dir, 0o777)
except Exception as e:
    print(f"Warning: Could not create or set permissions on logs directory: {e}")

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clear existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Console handler - clean format for terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler with 2-day rotation - wrapped with try/except for permission issues
try:
    price_log_path = os.path.join(logs_dir, 'price_tracker.log')
    # Try to ensure the file is writable
    if os.path.exists(price_log_path):
        try:
            os.chmod(price_log_path, 0o666)
        except:
            pass
            
    file_handler = TimedRotatingFileHandler(
        price_log_path,
        when='midnight',
        interval=1,
        backupCount=2,  # Keep logs for 2 days (today + yesterday)
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    print(f"✅ Successfully set up price_tracker.log")
except Exception as e:
    print(f"❌ Warning: Could not set up file logging: {e}")
    print("Logs will only be shown on console")

# Add a separate handler for Telegram bot logs
try:
    tg_log_path = os.path.join(logs_dir, 'telegram_bot.log')
    # Try to ensure the file is writable
    if os.path.exists(tg_log_path):
        try:
            os.chmod(tg_log_path, 0o666)
        except:
            pass
            
    tg_handler = TimedRotatingFileHandler(
        tg_log_path,
        when='midnight',
        interval=1,
        backupCount=2,
        encoding='utf-8'
    )
    tg_handler.setLevel(logging.INFO)
    tg_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    tg_handler.setFormatter(tg_formatter)
    logging.getLogger('telegram').addHandler(tg_handler)
    print(f"✅ Successfully set up telegram_bot.log")
except Exception as e:
    print(f"❌ Warning: Could not set up Telegram file logging: {e}")

# Silence verbose loggers
for name in ['celery.worker.strategy', 'celery.worker.consumer', 'celery.app.trace', 
             'urllib3.connectionpool', 'scripts.utils.db_mongo']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Global stats tracking
persistent_failures = {}
failure_counts = {}

# Initialize the cycle stats to track each 30-second cycle
cycle_stats = {
    'cycle_count': 0,
    'current': {
        'start_time': datetime.now(),
        'tokens_total': 0,
        'tokens_processed': 0,
        'tokens_succeeded': 0,
        'tokens_failed': 0,
        'failures_by_token': {},
    },
    'history': []  # To keep the last 10 cycles
}

# Regular stats for other purposes
stats = {
    'start_time': datetime.now(),
    'last_update_time': None,
    'tokens': {
        'processed': 0,
        'succeeded': 0,
        'failed': 0,
        'persistent_failures': {}
    },
    'batches': {
        'total': 0,
        'succeeded': 0,
        'failed': 0,
        'last_minute': {'start': datetime.now(), 'processed': 0, 'failed': 0, 'succeeded': 0}
    },
    'api': {
        'calls': 0,
        'success': 0,
        'failed': 0,
        'by_chain': defaultdict(lambda: {'calls': 0, 'success': 0, 'failed': 0})
    },
    'mongodb': {
        'docs_processed': 0,
        'docs_failed': 0
    }
}

def reset_cycle_stats():
    """Reset the current cycle stats to default values."""
    cycle_stats['current'] = {
        'start_time': datetime.now(),
        'tokens_total': 0,
        'tokens_processed': 0,
        'tokens_succeeded': 0,
        'tokens_failed': 0,
        'failures_by_token': {},
    }
    return cycle_stats['current']

def print_box(title, content, icon="ℹ️", max_width=50, show_level="info"):
    """Create a clean, fixed-width box for terminal output with no empty lines."""
    if isinstance(content, str):
        lines = [line for line in content.split('\n') if line.strip()]
    else:
        lines = [line for line in content if line.strip()]
    
    # Calculate box dimensions - keep it compact
    title_width = len(title)
    content_width = max(len(line) for line in lines) if lines else 0
    inner_width = max(title_width, content_width) + 4
    inner_width = min(inner_width, max_width)
    
    # Build the box with no unnecessary space
    box_lines = []
    box_lines.append(f"{icon} ┌{'─' * inner_width}┐")
    box_lines.append(f"{icon} │{title.center(inner_width)}│")
    box_lines.append(f"{icon} ├{'─' * inner_width}┤")
    
    for line in lines:
        if len(line) > inner_width:
            box_lines.append(f"{icon} │ {line[:inner_width-3]}... │")
        else:
            box_lines.append(f"{icon} │ {line.ljust(inner_width-2)} │")
    
    box_lines.append(f"{icon} └{'─' * inner_width}┘")
    
    # Log at the appropriate level
    box_text = "\n".join(box_lines)
    if show_level == "error":
        logger.error(box_text)
    elif show_level == "warning":
        logger.warning(box_text)
    else:
        logger.info(box_text)
    
    return box_text

def track_token_failure(token_id, contract_address, blockchain, error):
    """Track persistent token failures."""
    # Update cycle stats
    cycle_stats['current']['tokens_processed'] += 1
    cycle_stats['current']['tokens_failed'] += 1
    cycle_stats['current']['failures_by_token'][token_id] = error
    
    # Update global stats
    stats['tokens']['failed'] += 1
    stats['tokens']['processed'] += 1
    stats['batches']['last_minute']['failed'] += 1
    stats['batches']['last_minute']['processed'] += 1
    
    # Add to persistent failures tracking
    if token_id not in stats['tokens']['persistent_failures']:
        stats['tokens']['persistent_failures'][token_id] = {
            'count': 1,
            'error': error,
            'contract': contract_address,
            'blockchain': blockchain,
            'last_seen': datetime.now()
        }
    else:
        entry = stats['tokens']['persistent_failures'][token_id]
        entry['count'] += 1
        entry['error'] = error
        entry['last_seen'] = datetime.now()
    
    # Track in failure counts
    failure_counts[token_id] = failure_counts.get(token_id, 0) + 1
    persistent_failures[token_id] = {
        'count': failure_counts[token_id],
        'error': error,
        'contract': contract_address,
        'blockchain': blockchain,
        'last_seen': datetime.now()
    }

def start_new_cycle(tokens_total):
    """Start tracking a new price update cycle."""
    # Finalize the current cycle if it exists
    if cycle_stats['current']['tokens_total'] > 0:
        end_current_cycle()
    
    # Increment cycle count
    cycle_stats['cycle_count'] += 1
    
    # Clear and initialize the new cycle
    cycle_stats['current'] = {
        'start_time': datetime.now(),
        'tokens_total': tokens_total,
        'tokens_processed': 0,
        'tokens_succeeded': 0,
        'tokens_failed': 0,
        'failures_by_token': {},
    }
    
    logger.info(f"Starting price update cycle #{cycle_stats['cycle_count']}")

def track_token_success(token_id):
    """Track token processing success in the current cycle."""
    # Update cycle stats
    cycle_stats['current']['tokens_processed'] += 1
    cycle_stats['current']['tokens_succeeded'] += 1
    
    # Update global stats
    stats['tokens']['succeeded'] += 1
    stats['tokens']['processed'] += 1
    stats['batches']['last_minute']['succeeded'] += 1
    stats['batches']['last_minute']['processed'] += 1

def end_current_cycle():
    """End the current price update cycle and log a summary."""
    current = cycle_stats['current']
    if current['tokens_total'] == 0:
        return
    
    # Calculate duration and success rate
    duration = min((datetime.now() - current['start_time']).total_seconds(), 30.0)
    
    # Only calculate success rate if tokens were actually processed
    if current['tokens_processed'] > 0:
        success_rate = (current['tokens_succeeded'] / current['tokens_processed']) * 100
    else:
        success_rate = 0.0
    
    # Update the last_update_time
    stats['last_update_time'] = datetime.now()
    
    # Create summary with only necessary information
    summary = [
        f"Cycle #{cycle_stats['cycle_count']} Complete",
        f"Duration: {duration:.2f}s",
        f"Tokens Total: {current['tokens_total']}",
        f"Tokens Processed: {current['tokens_processed']}",
        f"Success Rate: {success_rate:.1f}%",
    ]
    
    # Add persistent failures if any
    failures = current['failures_by_token']
    if failures:
        summary.append("")
        summary.append(f"Failed tokens: {len(failures)} with {sum(failure_counts.values())} total failures")
        
        # Show ALL failing tokens with their details
        for token_id, error in sorted(failures.items(), key=lambda x: failure_counts.get(x[0], 0), reverse=True):
            if token_id in persistent_failures:
                count = persistent_failures[token_id]['count']
                error_msg = persistent_failures[token_id]['error']
                if len(error_msg) > 40:
                    error_msg = error_msg[:37] + "..."
                summary.append(f"  • Token ID {token_id}: {count} failures - {error_msg}")
    
    # Store in history (keep last 10)
    cycle_data = {
        'cycle': cycle_stats['cycle_count'],
        'time': cycle_stats['current']['start_time'],
        'tokens_total': current['tokens_total'],
        'tokens_processed': current['tokens_processed'],
        'tokens_succeeded': current['tokens_succeeded'],
        'tokens_failed': len(failures),
        'duration': duration
    }
    
    cycle_stats['history'].append(cycle_data)
    if len(cycle_stats['history']) > 10:
        cycle_stats['history'] = cycle_stats['history'][-10:]
    
    print_box("Price Update Cycle", summary, icon="🔄")

def log_batch_summary(batch_id, total, succeeded, failed, duration, errors=None):
    """Log batch results, only showing in terminal if significant failures occur."""
    # Update the basic stats
    stats['batches']['total'] += 1
    stats['batches']['last_minute']['processed'] += total
    stats['batches']['last_minute']['succeeded'] += succeeded
    stats['batches']['last_minute']['failed'] += failed
    stats['tokens']['processed'] += total
    
    # Only log the batch details to the file, not console
    detailed_batch = {
        "batch_id": batch_id,
        "success_count": succeeded,
        "failed_count": failed,
        "failures_by_error": {str(err): len(tokens) for err, tokens in (errors or {}).items()}
    }
    
    # Log detailed info at DEBUG level so it goes to file but not console
    logger.debug(f"Detailed batch processing: {json.dumps(detailed_batch)}")
    
    # Only show batches with significant failures in console
    if failed > 0 and (failed / total) > 0.1:  # More than 10% failure rate
        summary = [
            f"Batch {batch_id}",
            f"Processed: {total} tokens",
            f"Succeeded: {succeeded}",
            f"Failed: {failed}",
            f"Time: {duration:.2f}s"
        ]
        
        if errors and failed > 0:
            summary.append("")
            summary.append("Errors:")
            for err, tokens in sorted(errors.items(), key=lambda x: len(x[1]), reverse=True)[:3]:
                summary.append(f"  • {err[:30]}: {len(tokens)} tokens")
        
        print_box("Batch Processing", summary, icon="📦", show_level="warning")

def log_minute_summary():
    """Log a summary every minute with accurate token counts."""
    now = datetime.now()
    last = stats['batches']['last_minute']['start']
    
    # Check if a minute has passed
    if (now - last).total_seconds() >= 60:
        processed = stats['batches']['last_minute']['processed']
        succeeded = stats['batches']['last_minute']['succeeded']
        failed = stats['batches']['last_minute']['failed']
        
        # Calculate persistent failures in the last 5 minutes
        persistent = {
            k: v for k, v in persistent_failures.items() 
            if v['count'] >= 3 and (now - v['last_seen']).total_seconds() < 300
        }
        
        # Create meaningful minute summary with actual counts
        summary = [
            f"Minute Summary ({now.strftime('%H:%M')})",
            f"Tokens Processed: {processed}",
            f"Succeeded: {succeeded}",
            f"Failed: {failed}",
        ]
        
        # Only add success rate if we actually processed tokens
        if processed > 0:
            success_rate = (succeeded / processed) * 100
            summary.append(f"Success Rate: {success_rate:.1f}%")
            
        summary.append(f"Persistent Failures: {len(persistent)}")
        
        # Add top persistent failures if any exist
        if persistent:
            summary.append("")
            summary.append("Top Persistent Failures:")
            for token_id, data in sorted(
                persistent.items(), 
                key=lambda x: x[1]['count'], 
                reverse=True
            )[:5]:
                error = data.get('error', 'Unknown')
                if len(error) > 30:
                    error = error[:27] + "..."
                summary.append(f"  • ID {token_id}: {data['count']}x - {error}")
        
        print_box("Minute Summary", summary, icon="⏰")
        
        # Reset the minute tracking
        stats['batches']['last_minute'] = {
            'start': now,
            'processed': 0,
            'succeeded': 0,
            'failed': 0
        }
        
        return True
    
    return False

def track_api_call(success, blockchain=None):
    """Track API call success/failure."""
    stats['api']['calls'] += 1
    if blockchain:
        stats['api']['by_chain'][blockchain]['calls'] += 1
        
    if success:
        stats['api']['success'] += 1
        if blockchain:
            stats['api']['by_chain'][blockchain]['success'] += 1
    else:
        stats['api']['failed'] += 1
        if blockchain:
            stats['api']['by_chain'][blockchain]['failed'] += 1

def analyze_recurring_failures():
    """Analyze and log recurring failures."""
    now = datetime.now()
    # Get tokens that have failed repeatedly
    recurring = {k: v for k, v in persistent_failures.items() 
               if v['count'] >= 5 and (now - v['last_seen']).total_seconds() < 900}
    
    if not recurring:
        logger.debug("No recurring token failures detected")
        return "No recurring failures"
    
    # Group by blockchain for better analysis
    by_blockchain = defaultdict(list)
    for token_id, data in recurring.items():
        by_blockchain[data.get('blockchain', 'unknown')].append((token_id, data))
    
    summary = [
        f"Found {len(recurring)} tokens with persistent failures"
    ]
    
    # Show detailed failures for ALL failing tokens, not limited to a few
    for blockchain, tokens in by_blockchain.items():
        summary.append("")
        summary.append(f"{blockchain.upper()}: {len(tokens)} failing tokens")
        
        # Show ALL tokens with failures
        for token_id, data in sorted(tokens, key=lambda x: x[1]['count'], reverse=True):
            contract = data.get('contract', 'unknown')
            short_contract = f"{contract[:6]}...{contract[-4:]}" if len(contract) > 14 else contract
            error = data.get('error', 'Unknown')[:40]
            summary.append(f"  • ID {token_id} ({short_contract}): {data['count']}x - {error}")
    
    print_box("Recurring Failures Analysis", summary, icon="🔍")
    
    return f"Analyzed {len(recurring)} failing tokens"
