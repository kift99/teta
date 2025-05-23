# -*- coding: utf-8 -*-
# ^^^ Add encoding declaration for good measure ^^^

import asyncio
import logging
import os
import sys
import signal
import time
import random
import string
import csv
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, AsyncGenerator, Set, TYPE_CHECKING
import math
import ssl # Import ssl for TLS context
import mimetypes # Import mimetypes for attachment guessing
import traceback # For printing tracebacks

# --- Third-party Libraries ---
try:
    import aiosmtplib
    import aiofiles
    import dns.asyncresolver
    import dns.resolver
    import dns.exception
    from dotenv import load_dotenv
    from email_validator import validate_email, EmailNotValidError
    from email.message import EmailMessage
    from rich.logging import RichHandler
    # Import necessary Progress components
    from rich.progress import (
        Progress,
        BarColumn,
        TextColumn,
        TimeRemainingColumn,
        ProgressColumn # Import base class for custom column
    )
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text # Import Text for custom column rendering
except ImportError as e:
    print(f"❌ Missing required library: {e.name}. Please install requirements:")
    print("pip install aiosmtplib aiofiles dnspython python-dotenv email-validator rich")
    sys.exit(1)

# --- Type Checking ---
if TYPE_CHECKING:
    from rich.progress import Task # Import Task only for type hinting


# --- Load Environment Variables ---
load_dotenv()

# --- Python Version Check ---
REQUIRED_PYTHON_VERSION = (3, 8, 0)
CURRENT_PYTHON_VERSION = sys.version_info

if CURRENT_PYTHON_VERSION < REQUIRED_PYTHON_VERSION:
    print(
        f"❌ Python version {CURRENT_PYTHON_VERSION.major}.{CURRENT_PYTHON_VERSION.minor}.{CURRENT_PYTHON_VERSION.micro} "
        f"is not supported. Please use Python {REQUIRED_PYTHON_VERSION[0]}.{REQUIRED_PYTHON_VERSION[1]} or higher.",
        file=sys.stderr,
    )
    sys.exit(1)

console = Console()
console.print(f"[blue]✅ Python version {CURRENT_PYTHON_VERSION.major}.{CURRENT_PYTHON_VERSION.minor}.{CURRENT_PYTHON_VERSION.micro} is supported.[/blue]")

# --- Helper function to clean environment variables ---
def get_env_cleaned(var_name: str, default_value: str) -> str:
    """Gets env var, removes inline comments starting with '#' and strips whitespace."""
    value = os.getenv(var_name, default_value)
    if value:
        comment_start = value.find('#')
        if comment_start != -1:
            value = value[:comment_start]
        return value.strip()
    return default_value


# --- Configuration Loading & Validation ---
# Note: Adjust these values in your .env file to tune performance vs. stability
try:
    # --- Performance Tuning ---
    # BATCH_SIZE: Initial number of emails processed together. Adaptive control will adjust this.
    BATCH_SIZE = int(get_env_cleaned("BATCH_SIZE", "35"))
    # INITIAL_CONCURRENCY: How many emails to attempt sending simultaneously at the start. Adaptive control adjusts this.
    INITIAL_CONCURRENCY = int(get_env_cleaned("INITIAL_CONCURRENCY", "22"))
    # MAX_CONCURRENCY: Upper limit for simultaneous sends. Increase cautiously, requires good SMTPs & network.
    MAX_CONCURRENCY = int(get_env_cleaned("MAX_CONCURRENCY", "50")) # Increased default suggestion
    # MIN_CONCURRENCY: Lower limit for simultaneous sends.
    MIN_CONCURRENCY = int(get_env_cleaned("MIN_CONCURRENCY", "8"))
    # MAX_BATCH_SIZE: Upper limit for batch size adjustment.
    MAX_BATCH_SIZE = int(get_env_cleaned("MAX_BATCH_SIZE", "125")) # Increased default suggestion
    # MIN_BATCH_SIZE: Lower limit for batch size adjustment.
    MIN_BATCH_SIZE = int(get_env_cleaned("MIN_BATCH_SIZE", "18"))

    # --- Delays and Retries ---
    # MAX_RETRIES: How many times to retry a temporary failure for a single email.
    MAX_RETRIES = int(get_env_cleaned("MAX_RETRIES", "1"))
    # RETRY_DELAY_MS: Base delay (in ms) before the first retry. Uses exponential backoff. Reducing too much can harm reputation.
    RETRY_DELAY_MS = int(get_env_cleaned("RETRY_DELAY", "500"))
    RETRY_DELAY = RETRY_DELAY_MS / 1000.0
    # Optional Pausing: Helps avoid hitting hourly/daily limits. Set PAUSE_INTERVAL_EMAILS > 0 to enable.
    PAUSE_INTERVAL_EMAILS = int(get_env_cleaned("PAUSE_INTERVAL_EMAILS", "0"))
    PAUSE_DURATION_MS = int(get_env_cleaned("PAUSE_DURATION_MS", "0"))
    PAUSE_DURATION_S = PAUSE_DURATION_MS / 1000.0
    # DELAY_BETWEEN_BATCHES_MS = int(get_env_cleaned("DELAY_BETWEEN_BATCHES", "100")) # Not actively used, concurrency/batch size control pacing
    # DELAY_BETWEEN_BATCHES = DELAY_BETWEEN_BATCHES_MS / 1000.0

    # --- Functionality & Validation ---
    # SMTP_TEST: Simulate sending without actually connecting to SMTP servers. Good for testing flow.
    SMTP_TEST = get_env_cleaned("SMTP_TEST", "false").lower() == "true"
    # SKIP_DNS_FAILURES: If True, queues emails that get a 451 DNS error for retry later, instead of immediate retry.
    SKIP_DNS_FAILURES = get_env_cleaned("SKIP_DNS_FAILURES", "true").lower() == "true"
    # QUIET_MX_ERRORS: If True, suppresses MX validation warnings on the console (still logged to file).
    QUIET_MX_ERRORS = get_env_cleaned("QUIET_MX_ERRORS", "false").lower() == "true"
    # --- MX Validation Modes (Trade-off: Speed vs. Accuracy/Reputation) ---
    # CHECK_MX_DURING_LOAD: Validate MX records when loading recipients. Slows startup, cleans list early.
    CHECK_MX_DURING_LOAD = get_env_cleaned("CHECK_MX_DURING_LOAD", "false").lower() == "true"
    # CHECK_MX_BEFORE_SEND: Validate MX just before sending each email. Can significantly slow sending but is most up-to-date.
    CHECK_MX_BEFORE_SEND = get_env_cleaned("CHECK_MX_BEFORE_SEND", "false").lower() == "true"
    # DISABLE_ALL_MX_VALIDATION: Fastest, but sends to potentially invalid domains, risking reputation. Overrides other CHECK_MX flags.
    DISABLE_ALL_MX_VALIDATION = get_env_cleaned("DISABLE_ALL_MX_VALIDATION", "false").lower() == "true"
    # --- TLS Settings (Hardcoded OFF in this version) ---
    # SKIP_SMTP_VERIFY: Ignored, verification is hardcoded OFF.
    SKIP_SMTP_VERIFY = get_env_cleaned("SKIP_SMTP_VERIFY", "false").lower() == "true"
    # SMTP_REJECT_UNAUTHORIZED: Ignored, verification is hardcoded OFF.
    SMTP_REJECT_UNAUTHORIZED = get_env_cleaned("SMTP_REJECT_UNAUTHORIZED", "true").lower() != "false"

    # --- Logging & Identification ---
    LOG_LEVEL = get_env_cleaned("LOG_LEVEL", "INFO").upper()
    CONSOLE_LOG_LEVEL = get_env_cleaned("CONSOLE_LOG_LEVEL", "INFO").upper()
    FROM_NAME = get_env_cleaned("FROM_NAME", "Claro Br")
    EMAIL_SUBJECT_TEMPLATE = get_env_cleaned("EMAIL_SUBJECT", "📩 18% OFF Pagando HOJE: Evite Bloqueio. Cod:{random:8}")

    # --- File Paths ---
    SMTP_CONFIG_PATH_STR = get_env_cleaned("SMTP_CONFIG_PATH", "smtp_servers.txt")
    RECIPIENT_LIST_PATH_STR = get_env_cleaned("RECIPIENT_LIST_PATH", "recipients.txt")
    MESSAGE_FILE_PATH_STR = get_env_cleaned("MESSAGE_FILE_PATH", "message.html")
    ATTACHMENT_PATH_STR = get_env_cleaned("ATTACHMENT_PATH", "")
    SUCCESS_FILE_PATH_STR = get_env_cleaned("SUCCESS_FILE_PATH", "sucesso.csv")
    FAILED_FILE_PATH_STR = get_env_cleaned("FAILED_FILE_PATH", "falha.csv")

    # Convert paths after cleaning
    SMTP_CONFIG_PATH = Path(SMTP_CONFIG_PATH_STR)
    RECIPIENT_LIST_PATH = Path(RECIPIENT_LIST_PATH_STR)
    MESSAGE_FILE_PATH = Path(MESSAGE_FILE_PATH_STR)
    ATTACHMENT_PATH = Path(ATTACHMENT_PATH_STR) if ATTACHMENT_PATH_STR else None
    SUCCESS_FILE_PATH = Path(SUCCESS_FILE_PATH_STR)
    FAILED_FILE_PATH = Path(FAILED_FILE_PATH_STR)

    # Validate numeric ranges
    if not (MIN_CONCURRENCY <= INITIAL_CONCURRENCY <= MAX_CONCURRENCY):
        console.print(f"[yellow]⚠️ Warning: INITIAL_CONCURRENCY ({INITIAL_CONCURRENCY}) is outside the range [{MIN_CONCURRENCY}-{MAX_CONCURRENCY}]. Adjusting to bounds.[/yellow]")
        INITIAL_CONCURRENCY = max(MIN_CONCURRENCY, min(INITIAL_CONCURRENCY, MAX_CONCURRENCY))
    if not (MIN_BATCH_SIZE <= BATCH_SIZE <= MAX_BATCH_SIZE):
        console.print(f"[yellow]⚠️ Warning: BATCH_SIZE ({BATCH_SIZE}) is outside the range [{MIN_BATCH_SIZE}-{MAX_BATCH_SIZE}]. Adjusting to bounds.[/yellow]")
        BATCH_SIZE = max(MIN_BATCH_SIZE, min(BATCH_SIZE, MAX_BATCH_SIZE))
    if MIN_CONCURRENCY < 1: MIN_CONCURRENCY = 1
    if MIN_BATCH_SIZE < 1: MIN_BATCH_SIZE = 1

except (ValueError, TypeError) as e:
    console.print(f"[red]❌ Error parsing environment variable: {e}[/red]")
    traceback.print_exc()
    sys.exit(1)

# Validate required environment variables and file existence
def validate_config():
    global ATTACHMENT_PATH # Allow modification

    required_env_vars = ["SMTP_CONFIG_PATH", "RECIPIENT_LIST_PATH", "MESSAGE_FILE_PATH"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
         console.print(f"[red]❌ Missing required environment variables defined in .env: {', '.join(missing_vars)}[/red]")
         sys.exit(1)

    required_paths_map = {
        "SMTP_CONFIG_PATH": SMTP_CONFIG_PATH,
        "RECIPIENT_LIST_PATH": RECIPIENT_LIST_PATH,
        "MESSAGE_FILE_PATH": MESSAGE_FILE_PATH,
    }

    # If ATTACHMENT_PATH was set in .env, check if the file exists
    if os.getenv("ATTACHMENT_PATH") and ATTACHMENT_PATH:
        required_paths_map["ATTACHMENT_PATH"] = ATTACHMENT_PATH
    elif ATTACHMENT_PATH: # If path derived from default but not set in .env, ignore it
        ATTACHMENT_PATH = None

    missing_files = []
    for var, path in required_paths_map.items():
        if path and not path.exists():
            missing_files.append(f"{var} ({path})")

    if missing_files:
        console.print(f"[red]❌ Required files not found: {', '.join(missing_files)}[/red]")
        sys.exit(1)

    # Ensure output directories exist
    try:
        SUCCESS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FAILED_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        console.print(f"[red]❌ Could not create output directories: {e}[/red]")
        sys.exit(1)

    console.print("[blue]✅ Environment variables and file paths verified.[/blue]")

validate_config() # Run validation after initial loading

# --- Logger Setup (Rich Handler) ---
# Custom filter to suppress MX errors on console if needed
class MxErrorFilter(logging.Filter):
    def filter(self, record):
        is_mx_error = 'MX Record Error' in record.getMessage() or 'MX Check Failed' in record.getMessage()
        # Suppress only if QUIET_MX_ERRORS is True, it's a WARNING, and it's an MX error
        return not (QUIET_MX_ERRORS and record.levelno == logging.WARNING and is_mx_error)

logging.basicConfig(level=LOG_LEVEL, format="%(message)s", datefmt="[%X]", handlers=[]) # Remove default handlers
logger = logging.getLogger("bulk_sender")

# Console Handler with Rich
rich_handler = RichHandler(
    console=console,
    show_time=True,
    show_level=True,
    show_path=False,
    markup=True, # Allow rich markup in logs
    log_time_format="[%Y-%m-%d %H:%M:%S]",
    level=CONSOLE_LOG_LEVEL
)
rich_handler.addFilter(MxErrorFilter()) # Add the custom filter
logger.addHandler(rich_handler)

# File Handlers
try:
    log_dir = Path(".") # Log files in the current directory
    log_dir.mkdir(exist_ok=True)

    error_handler = logging.FileHandler(log_dir / "error.log", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)

    combined_handler = logging.FileHandler(log_dir / "combined.log", encoding="utf-8")
    combined_handler.setLevel(logging.DEBUG) # Log everything to combined log
    combined_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    combined_handler.setFormatter(combined_formatter)
    logger.addHandler(combined_handler)
except OSError as e:
    console.print(f"[yellow]⚠️ Could not configure file logging: {e}. Logging to console only.[/yellow]")


# --- Define the Custom Column ---
class EmailsPerSecondColumn(ProgressColumn):
    """Renders the speed in emails per second."""

    def render(self, task: "Task") -> Text:
        """Show emails per second speed."""
        speed = task.speed
        if speed is None:
            return Text("? email/s", style="progress.data.speed")
        speed_str = f"{speed:.1f}"
        return Text(f"{speed_str} email/s", style="progress.data.speed")

# --- Progress Bar Setup (Rich) ---
progress = Progress(
    TextColumn("[bold blue]{task.description}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    TextColumn("[green]{task.completed:>0.0f}[/]/[bold]{task.total:>0.0f}[/]"),
    "•",
    TimeRemainingColumn(),
    "•",
    EmailsPerSecondColumn(), # Custom speed column
    console=console,
    transient=False, # Keep visible after completion
)
main_task_id = None
queue_task_id = None
progress_active = False

# Helper function to safely log messages without messing up the progress bar
def safe_log(level, message, *args, **kwargs):
    """Logs messages, temporarily stopping the progress bar if active."""
    is_main_active = main_task_id is not None and main_task_id in progress.task_ids and not progress.tasks[main_task_id].finished
    is_queue_active = queue_task_id is not None and queue_task_id in progress.task_ids and not progress.tasks[queue_task_id].finished
    was_active = progress_active and (is_main_active or is_queue_active)

    if was_active:
        try: progress.stop()
        except Exception: pass

    log_func = getattr(logger, level.lower() if isinstance(level, str) else logging.getLevelName(level).lower(), logger.info)
    log_func(message, *args, **kwargs)

    if was_active:
        try: progress.start()
        except Exception: pass


# --- Adaptive Load Control System ---
class AdaptiveControl:
    def __init__(self):
        self.concurrency = INITIAL_CONCURRENCY
        self.max_concurrency = MAX_CONCURRENCY
        self.min_concurrency = MIN_CONCURRENCY
        self.batch_size = BATCH_SIZE
        self.max_batch_size = MAX_BATCH_SIZE
        self.min_batch_size = MIN_BATCH_SIZE

        self.success_rate = 1.0
        self.error_window = deque(maxlen=100) # Window size for success rate calculation
        self.response_time_window = deque(maxlen=50) # Window size for avg response time
        self.adjustment_counter = 0
        # Adjust slightly more frequently for potentially faster reaction
        self.adjustment_interval = 25 # Adjust parameters every N results

    def register_result(self, success: bool, response_time_s: Optional[float]):
        self.error_window.append(1 if success else 0)
        if success and response_time_s is not None:
            self.response_time_window.append(response_time_s)

        if self.error_window:
            self.success_rate = sum(self.error_window) / len(self.error_window)
        else:
            self.success_rate = 1.0

        self.adjustment_counter += 1
        if self.adjustment_counter >= self.adjustment_interval:
            self.adjust_parameters()
            self.adjustment_counter = 0

    def adjust_parameters(self):
        old_concurrency = self.concurrency
        old_batch_size = self.batch_size

        # Adjust concurrency based on success rate
        if self.success_rate > 0.95: # High success -> increase concurrency
            self.concurrency = min(self.concurrency + 1, self.max_concurrency)
        elif self.success_rate < 0.75: # Low success -> decrease concurrency significantly
            self.concurrency = max(self.concurrency - 2, self.min_concurrency)
        elif self.success_rate < 0.85: # Moderate low success -> decrease concurrency slightly
             self.concurrency = max(self.concurrency - 1, self.min_concurrency)

        # Adjust batch size based on success rate
        if self.success_rate > 0.90: # High success -> increase batch size
             self.batch_size = min(self.batch_size + 2, self.max_batch_size)
        elif self.success_rate < 0.80: # Low success -> decrease batch size
             self.batch_size = max(self.batch_size - 2, self.min_batch_size)

        # Ensure parameters stay within defined bounds
        self.concurrency = max(self.min_concurrency, min(self.concurrency, self.max_concurrency))
        self.batch_size = max(self.min_batch_size, min(self.batch_size, self.max_batch_size))

        if self.concurrency != old_concurrency or self.batch_size != old_batch_size:
            direction = "[green]🔼 Increasing[/green]" if (self.concurrency > old_concurrency or self.batch_size > old_batch_size) else "[yellow]🔽 Reducing[/yellow]"
            safe_log('info', f"[magenta]{direction} limits - Concurrency: {self.concurrency}, Batch Size: {self.batch_size} (Success Rate: {self.success_rate:.1%})[/magenta]")
            global rate_limiter
            rate_limiter.set_concurrency(self.concurrency)

    def get_average_response_time(self) -> float:
        if not self.response_time_window:
            return 1.0 # Default 1 second if no data yet
        return sum(self.response_time_window) / len(self.response_time_window)

    def get_batch_size(self) -> int:
        return max(self.min_batch_size, min(self.batch_size, self.max_batch_size))

    def get_stats(self) -> Dict[str, Any]:
        return {
            "success_rate": self.success_rate,
            "concurrency": self.concurrency,
            "batch_size": self.get_batch_size(),
            "avg_response_time_ms": self.get_average_response_time() * 1000
        }

adaptive_control = AdaptiveControl()

# --- Global Variables & State ---
success_count = 0
failure_count = 0
successful_emails_buffer: List[str] = []
failed_emails_buffer: List[Dict[str, str]] = []
dns_failure_queue: List[Dict[str, Any]] = []
failed_smtp_servers: Dict[str, Dict[str, Any]] = {} # key: email, value: { retry_time: float, reason: str }
invalid_domain_cache: Set[str] = set()
emails_processed_since_last_pause = 0
total_emails = 0
is_shutting_down = False # Flag to signal graceful shutdown

# --- Rate Limiter (using asyncio.Semaphore) ---
class RateLimiter:
    def __init__(self, concurrency: int):
        self._concurrency = max(1, concurrency)
        self.semaphore = asyncio.Semaphore(self._concurrency)
        safe_log('info', f"RateLimiter initialized with concurrency: {self._concurrency}")

    def set_concurrency(self, new_concurrency: int):
        new_concurrency = max(1, new_concurrency)
        if new_concurrency != self._concurrency:
            safe_log('info', f"RateLimiter concurrency updated from {self._concurrency} to: {new_concurrency}")
            self._concurrency = new_concurrency
            self.semaphore = asyncio.Semaphore(self._concurrency)

    async def run(self, coro):
        async with self.semaphore:
            if is_shutting_down:
                raise asyncio.CancelledError("Shutdown initiated, task not started")
            return await coro

rate_limiter = RateLimiter(adaptive_control.concurrency)

# --- Network & DNS Utilities ---
dns_resolver = dns.asyncresolver.Resolver()
dns_resolver.timeout = 3.0 # seconds
dns_resolver.lifetime = 3.0 # seconds

async def validate_domain(domain: str) -> bool:
    """Validates if a domain has MX records (with timeout and caching)."""
    if DISABLE_ALL_MX_VALIDATION:
        return True
    if domain in invalid_domain_cache:
        safe_log('debug', f"MX Check Skipped (Cached Invalid): {domain}")
        return False

    try:
        answer = await dns_resolver.resolve(domain, 'MX')
        if answer and any(record.exchange and record.preference is not None for record in answer):
            safe_log('debug', f"MX Check Success: {domain}")
            return True
        else:
            safe_log('warning', f"MX Record Error: No valid MX records found for domain: {domain}", extra={"domain": domain})
            invalid_domain_cache.add(domain)
            return False
    except dns.resolver.NoAnswer:
        safe_log('warning', f"MX Record Error: No MX records found (NoAnswer) for domain: {domain}", extra={"domain": domain})
        invalid_domain_cache.add(domain)
        return False
    except dns.resolver.NXDOMAIN:
        safe_log('warning', f"MX Record Error: Domain does not exist (NXDOMAIN): {domain}", extra={"domain": domain})
        invalid_domain_cache.add(domain)
        return False
    except dns.exception.Timeout:
        safe_log('warning', f"MX Record Error: DNS lookup timed out for domain: {domain}", extra={"domain": domain})
        return False # Don't cache timeouts
    except dns.resolver.NoNameservers:
         safe_log('error', f"MX Record Error: No nameservers available for domain {domain}", extra={"domain": domain})
         return False # Don't cache
    except Exception as e:
        safe_log('error', f"MX Record Error: Unexpected error validating domain {domain}: {e}", exc_info=False, extra={"domain": domain})
        return False # Don't cache unexpected errors

# --- SMTP Management ---
SmtpConfig = Dict[str, Any] # Type alias for SMTP config dictionary

async def load_smtp_servers(file_path: Path) -> List[SmtpConfig]:
    """
    Loads SMTP server configurations from a file (host|email|password).
    Defaults to port 587 and STARTTLS.
    """
    safe_log('info', f"Loading SMTP configurations from: {file_path} (Format: host|email|password)")
    configs: List[SmtpConfig] = []
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
            line_num = 0
            async for line in f:
                line_num += 1
                line = line.strip()
                if not line or line.startswith('#'): continue
                parts = [p.strip() for p in line.split('|')]
                if len(parts) != 3:
                    safe_log('warning', f"Skipping invalid SMTP line {line_num}: Expected 3 parts, found {len(parts)}. Line: '{line}'")
                    continue
                host, email, password = parts[0], parts[1], parts[2]
                if not host or not email or not password:
                    safe_log('warning', f"Skipping invalid SMTP line {line_num}: Host, email, or password missing.")
                    continue
                try:
                    validate_email(email, check_deliverability=False)
                except EmailNotValidError as val_err:
                    safe_log('warning', f"Skipping invalid SMTP line {line_num}: Invalid email format '{email}': {val_err}")
                    continue
                configs.append({
                    "host": host, "port": 587, "email": email, "password": password,
                    "use_tls": False, "start_tls": True
                })
        safe_log('info', f"Loaded {len(configs)} valid SMTP configurations.")
        if not configs:
            safe_log('critical', 'No valid SMTP configurations found. Cannot proceed.')
            sys.exit(1)
        return configs
    except FileNotFoundError:
        safe_log('critical', f"SMTP configuration file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        safe_log('critical', f"Failed to load SMTP configurations: {e}", exc_info=True)
        sys.exit(1)


def get_next_smtp_config(smtp_servers: List[SmtpConfig]) -> SmtpConfig:
    """Selects the next available SMTP server randomly, respecting temporary failures."""
    now = time.monotonic()
    available_servers = []
    unavailable_details = {}

    for config in smtp_servers:
        email = config['email']
        if email in failed_smtp_servers:
            failure_info = failed_smtp_servers[email]
            if now < failure_info['retry_time']:
                unavailable_details[email] = failure_info['reason']
                continue
            else:
                safe_log('info', f"SMTP {email} is now available for retry (was failing due to: {failure_info['reason']}).")
                del failed_smtp_servers[email]
                available_servers.append(config)
        else:
            available_servers.append(config)

    unavailable_count = len(unavailable_details)
    if unavailable_count > 0 and (unavailable_count == len(smtp_servers) or random.random() < 0.1):
         safe_log('debug', f"{unavailable_count}/{len(smtp_servers)} SMTP server(s) are currently marked as unavailable.")

    if not available_servers:
        safe_log('error', 'All SMTP servers are currently marked as unavailable.')
        if not smtp_servers: raise RuntimeError('No SMTP servers loaded.')
        else:
            next_retry_time = min((info['retry_time'] for info in failed_smtp_servers.values()), default=float('inf'))
            wait_seconds = max(0, next_retry_time - now) if next_retry_time != float('inf') else float('inf')
            wait_minutes_str = f"{math.ceil(wait_seconds / 60)}" if wait_seconds != float('inf') else 'Indefinitely'
            raise RuntimeError(f"All {len(smtp_servers)} SMTP servers are currently unavailable. Next potential retry in ~{wait_minutes_str} minutes.")

    return random.choice(available_servers)

# --- Recipient Loading ---
# Consider streaming for extremely large files (> millions, depending on RAM)
# Current approach loads all valid recipients into memory.
async def load_recipients_in_batches(file_path: Path, batch_size: int) -> AsyncGenerator[List[str], None]:
    """Reads recipient emails from a file asynchronously, validates, handles duplicates, optionally checks MX."""
    safe_log('info', f"Loading recipients from: {file_path}")
    check_mx = CHECK_MX_DURING_LOAD and not DISABLE_ALL_MX_VALIDATION
    safe_log('info', f"MX check during load: {'Enabled' if check_mx else 'Disabled'}")

    batch: List[str] = []
    seen_emails: Set[str] = set()
    line_count = 0
    valid_count = 0
    invalid_format_count = 0
    duplicate_count = 0
    mx_failure_count = 0
    global total_emails # To update the global count

    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8', errors='ignore') as f:
            async for line in f:
                line_count += 1
                email = line.strip().lower() # Normalize to lowercase
                if not email: continue

                try:
                    validate_email(email, check_deliverability=False)
                except EmailNotValidError:
                    invalid_format_count += 1
                    continue

                if email in seen_emails:
                    duplicate_count += 1
                    continue
                seen_emails.add(email)

                if check_mx:
                    try:
                        domain = email.split('@')[1]
                        is_valid_domain = await validate_domain(domain)
                        if not is_valid_domain:
                            mx_failure_count += 1
                            continue
                        batch.append(email)
                        valid_count += 1
                    except IndexError:
                        invalid_format_count += 1
                        continue
                    except Exception as e:
                        safe_log('error', f"Error checking MX for {domain} (email: {email}, Line: {line_count}): {e}. Skipping email.", exc_info=False)
                        mx_failure_count += 1
                        continue
                else:
                    batch.append(email)
                    valid_count += 1

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch: yield batch

        safe_log('info', f"Recipient loading complete. Lines read: {line_count}, Valid & Unique: {valid_count}, Invalid Format: {invalid_format_count}, Duplicates: {duplicate_count}, MX Failures (load): {mx_failure_count}")
        total_emails = valid_count # Set the global total

    except FileNotFoundError:
        safe_log('critical', f"Recipient file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        safe_log('critical', f"Failed to load recipients: {e}", exc_info=True)
        sys.exit(1)


# --- Email Content & Personalization ---
def generate_random_string(length: int = 8) -> str:
    """Generates a random alphanumeric string."""
    if length <= 0: return ""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def fill_template(template: str, recipient_email: str) -> str:
    """Fills placeholders in the email template."""
    try:
        name_part = recipient_email.split('@')[0]
        name = ' '.join(word.capitalize() for word in name_part.replace('.', ' ').replace('_', ' ').replace('-', ' ').split() if not word.isdigit())
        name = name or 'Cliente'
    except Exception:
        name = 'Cliente'

    now = datetime.now()
    replacements = {
        '{email}': recipient_email,
        '{name}': name,
        '{date}': now.strftime('%d/%m/%Y'),
        '{time}': now.strftime('%H:%M:%S'),
    }

    filled = template
    for placeholder, value in replacements.items():
        filled = filled.replace(placeholder, value)

    # Handle {random:N} placeholders
    while True:
        start_tag = filled.find('{random')
        if start_tag == -1: break
        end_tag = filled.find('}', start_tag)
        if end_tag == -1: break
        tag_content = filled[start_tag + 1:end_tag]
        parts = tag_content.split(':')
        length = 8
        if len(parts) > 1:
            try:
                length = int(parts[1])
                if length < 1: length = 8
            except (ValueError, IndexError): pass
        random_str = generate_random_string(length)
        filled = filled[:start_tag] + random_str + filled[end_tag + 1:]

    return filled

# --- Performance Metrics Tracking ---
class Metrics:
    def __init__(self):
        self.start_time = time.monotonic()
        self.total_sent_attempts = 0
        self.total_success = 0
        self.total_failure = 0
        self.smtp_usage: Dict[str, int] = {}
        self.domain_stats: Dict[str, Dict[str, int]] = {}
        self.send_times = deque(maxlen=200)
        self.last_report_time = time.monotonic()
        self.report_interval_s = 60

    def record_attempt(self): self.total_sent_attempts += 1
    def record_success(self, smtp_email: str, domain: str, send_time_s: float):
        self.total_success += 1
        self.smtp_usage[smtp_email] = self.smtp_usage.get(smtp_email, 0) + 1
        stats = self.domain_stats.setdefault(domain, {"success": 0, "failure": 0})
        stats["success"] += 1
        self.send_times.append(send_time_s)
    def record_failure(self, domain: str, error_type: str = 'Unknown'):
        self.total_failure += 1
        stats = self.domain_stats.setdefault(domain, {"success": 0, "failure": 0})
        stats["failure"] += 1
    def get_average_send_time_ms(self) -> Optional[float]:
        if not self.send_times: return None
        return (sum(self.send_times) / len(self.send_times)) * 1000

    def generate_report(self) -> Dict[str, Any]:
        elapsed_seconds = max(0.01, time.monotonic() - self.start_time)
        emails_per_second = self.total_success / elapsed_seconds
        avg_send_time_ms = self.get_average_send_time_ms()
        top_smtps = sorted(self.smtp_usage.items(), key=lambda item: item[1], reverse=True)[:5]
        problem_domains = sorted(
            [(domain, stats) for domain, stats in self.domain_stats.items()
             if stats.get('failure', 0) >= 3 and stats.get('failure', 0) > stats.get('success', 0)],
            key=lambda item: item[1].get('failure', 0), reverse=True
        )[:5]
        return {
            "total_success": self.total_success, "total_failure": self.total_failure,
            "elapsed_time_s": elapsed_seconds, "emails_per_second": emails_per_second,
            "avg_send_time_ms": avg_send_time_ms, "top_smtps": top_smtps,
            "problem_domains": problem_domains, "adaptive_stats": adaptive_control.get_stats()
        }

    def log_periodic_report(self, force: bool = False):
        """Logs the performance report if interval has passed or forced."""
        now = time.monotonic()
        if force or (now - self.last_report_time >= self.report_interval_s):
            report = self.generate_report()
            def print_report_safe():
                safe_log('info', "\n" + "-"*20 + " [yellow bold]Performance Report[/] " + "-"*20)
                safe_log('info', f"  Elapsed Time: {report['elapsed_time_s']:.0f}s")
                safe_log('info', f"  [green]Successful Sends:[/green] {report['total_success']}")
                safe_log('info', f"  [red]Failed Sends:[/red] {report['total_failure']}")
                safe_log('info', f"  [blue]Avg. Speed:[/blue] {report['emails_per_second']:.2f} emails/s")
                avg_time_str = f"{report['avg_send_time_ms']:.0f}ms" if report['avg_send_time_ms'] is not None else "N/A"
                safe_log('info', f"  [blue]Avg. Send Time:[/blue] {avg_time_str}")
                adapt_stats = report['adaptive_stats']
                safe_log('info', f"  [magenta]Adaptive Control:[/magenta] Concurrency={adapt_stats['concurrency']}, BatchSize={adapt_stats['batch_size']}, SuccessRate={adapt_stats['success_rate']:.1%}")
                if report['top_smtps']:
                    safe_log('info', Text("  Top SMTP Servers (by success count):", style="cyan"))
                    for i, (smtp, count) in enumerate(report['top_smtps']): safe_log('info', f"    {i+1}. {smtp}: {count} sends")
                else: safe_log('info', Text("  Top SMTP Servers: None yet.", style="cyan"))
                if report['problem_domains']:
                    safe_log('info', Text("  Domains with High Failure Rate (>3 failures & failure > success):", style="red bold"))
                    for i, (domain, stats) in enumerate(report['problem_domains']): safe_log('info', f"    {i+1}. {domain}: {stats.get('failure', 0)} failures / {stats.get('success', 0)} successes")
                else: safe_log('info', Text("  Domains with High Failure Rate: None observed.", style="red bold"))
                safe_log('info', "-" * (40 + len(" Performance Report ")) + "\n")
            print_report_safe()
            self.last_report_time = now

metrics = Metrics()

# --- Send Progress Display ---
def show_send_progress(type: str, recipient: str, smtp: str, details: str = ""):
    """Logs the status of individual email sends using safe_log."""
    log_level, prefix, style, detail_style = "info", "", "", ""
    if type == 'success': prefix, style, detail_style = "✅ SUCCESS:", "green", "green"
    elif type == 'retry': prefix, style, detail_style = "🔄 RETRY:", "blue", "blue"
    elif type == 'error': log_level, prefix, style, detail_style = "warning", "❌ FAILURE:", "red", "red"
    elif type == 'queued': prefix, style, detail_style = "🕒 QUEUED:", "yellow", "yellow"
    else: return
    message = f"[{style}]{prefix}[/] {recipient} via {smtp}"
    if details: message += f" - [{detail_style}]{details}[/]"
    safe_log(log_level, message)


# --- Core Email Sending Function ---
async def send_email(
    smtp_config: SmtpConfig,
    recipient: str,
    subject: str,
    html_body: str,
    attachment_path: Optional[Path],
    smtp_servers_ref: List[SmtpConfig], # Pass reference for retries
    retries: int = 0,
) -> bool:
    """Attempts to send a single email, handling retries and specific errors."""
    start_time = time.monotonic()
    metrics.record_attempt()
    domain = recipient.split('@')[1] if '@' in recipient else 'unknown_domain'
    smtp_email = smtp_config['email']

    try:
        # 1. Pre-send Checks
        if domain == 'unknown_domain' or '.' not in domain:
            raise ValueError('Invalid recipient email domain format')
        if CHECK_MX_BEFORE_SEND and not CHECK_MX_DURING_LOAD and not DISABLE_ALL_MX_VALIDATION:
            if not await validate_domain(domain):
                raise RuntimeError(f'MX Check Failed before send for domain: {domain}')

        # 2. SMTP Test Mode Simulation
        if SMTP_TEST:
            simulated_delay = random.uniform(0.05, 0.15)
            await asyncio.sleep(simulated_delay)
            show_send_progress('success', recipient, smtp_email, '[TEST MODE]')
            metrics.record_success(smtp_email, domain, simulated_delay)
            adaptive_control.register_result(True, simulated_delay)
            return True

        # 3. Construct Email Message
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = f'"{FROM_NAME}" <{smtp_email}>'
        msg['To'] = recipient
        msg['X-Priority'] = '1 (Highest)'
        msg['X-MSMail-Priority'] = 'High'
        msg['Importance'] = 'High'
        msg.set_content("Please enable HTML to view this message.")
        msg.add_alternative(html_body, subtype='html')

        # 4. Add Attachment
        if attachment_path and attachment_path.exists():
            try:
                async with aiofiles.open(attachment_path, 'rb') as afp: file_data = await afp.read()
                ctype, encoding = mimetypes.guess_type(str(attachment_path))
                if ctype is None or encoding is not None: ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)
                msg.add_attachment(file_data, maintype=maintype, subtype=subtype, filename=attachment_path.name)
                safe_log('debug', f"Attached {attachment_path.name} for {recipient}")
            except FileNotFoundError:
                 safe_log('error', f"Attachment file not found: {attachment_path}. Sending without.")
            except Exception as attach_err:
                safe_log('error', f"Error attaching file {attachment_path}: {attach_err}. Sending without.")

        # 5. Configure TLS Context (Hardcoded to disable verification)
        tls_context = None
        if smtp_config['start_tls'] or smtp_config['use_tls']:
             tls_context = ssl.create_default_context()
             tls_context.check_hostname = False
             tls_context.verify_mode = ssl.CERT_NONE

        # 6. Establish SMTP Connection and Send
        smtp_client = aiosmtplib.SMTP(
            hostname=smtp_config['host'], port=smtp_config['port'],
            use_tls=smtp_config['use_tls'], start_tls=smtp_config['start_tls'],
            timeout=20.0, tls_context=tls_context
        )
        async with smtp_client:
            if smtp_config.get('password'):
                try:
                    await smtp_client.login(smtp_email, smtp_config['password'])
                    safe_log('debug', f"SMTP Login successful for {smtp_email}")
                except aiosmtplib.SMTPAuthenticationError as auth_err: raise auth_err
                except Exception as login_err:
                    safe_log('error', f"Login phase error for {smtp_email}: {login_err}")
                    raise login_err
            await smtp_client.send_message(msg)

        # 7. Record Success
        send_time = time.monotonic() - start_time
        show_send_progress('success', recipient, smtp_email, f"({send_time*1000:.0f}ms)")
        metrics.record_success(smtp_email, domain, send_time)
        adaptive_control.register_result(True, send_time)
        return True

    # --- Exception Handling & Retry Logic ---
    except Exception as error:
        send_time = time.monotonic() - start_time
        adaptive_control.register_result(False, send_time)
        error_message = str(error).lower()

        # --- Extract code and specific message carefully ---
        error_code = None
        specific_error_detail = str(error)
        is_recipient_refused_type = isinstance(error, aiosmtplib.SMTPRecipientsRefused)

        if is_recipient_refused_type:
            try:
                first_recipient_error_detail = error.recipients[0]
                if isinstance(first_recipient_error_detail, aiosmtplib.SMTPRecipientRefused):
                    error_code = first_recipient_error_detail.code
                    specific_error_detail = first_recipient_error_detail.message
                elif isinstance(first_recipient_error_detail, tuple) and len(first_recipient_error_detail) >= 2:
                   error_code = first_recipient_error_detail[0]
                   specific_error_detail = first_recipient_error_detail[1]
            except (IndexError, AttributeError, TypeError):
                 safe_log('debug', f"Could not extract detailed code/message from SMTPRecipientsRefused: {error}")
        elif hasattr(error, 'code'):
            error_code = getattr(error, 'code', None)
            if hasattr(error, 'message'): specific_error_detail = getattr(error, 'message', str(error))

        # --- Classify Error Types ---
        is_recipient_error = is_recipient_refused_type or \
                             (isinstance(error_code, int) and error_code in [550, 553] and ('recipient' in error_message or 'user unknown' in error_message or 'mailbox unavailable' in error_message or 'no such user' in error_message))
        is_auth_error = isinstance(error, aiosmtplib.SMTPAuthenticationError)
        is_sender_error = isinstance(error, aiosmtplib.SMTPSenderRefused)
        is_data_error = isinstance(error, aiosmtplib.SMTPDataError)
        is_mx_error = 'mx check failed' in error_message or \
                      (isinstance(error_code, int) and error_code == 550 and ('domain' in error_message or 'host unknown' in error_message or 'no such domain' in error_message)) or \
                      isinstance(error, RuntimeError) and 'mx check failed' in error_message
        is_connection_error = isinstance(error, (aiosmtplib.SMTPConnectError, aiosmtplib.SMTPTimeoutError, ConnectionRefusedError, ConnectionResetError, TimeoutError, OSError, ssl.SSLError)) or \
                              'timeout' in error_message or 'connection refused' in error_message or 'connection reset' in error_message or 'network is unreachable' in error_message or 'lookup timed out' in error_message
        is_temporary_error = isinstance(error, aiosmtplib.SMTPServerDisconnected) or \
                             (isinstance(error_code, int) and 400 <= error_code < 500) or \
                             'try again later' in error_message or 'temporary failure' in error_message or 'service unavailable' in error_message or 'resources temporarily unavailable' in error_message or 'greylisted' in error_message
        is_dns_451_error = error_code == 451 and ('dns' in error_message or '4.4.0' in error_message or 'unable to resolve' in error_message or 'temporary lookup failure' in error_message)
        is_dns_timeout_error = isinstance(error, dns.exception.Timeout)

        # --- Handle Specific Error Cases ---
        if is_auth_error:
            safe_log('error', f"Auth failed for {smtp_email} ({smtp_config['host']}): {error}. Marking failed (30 min).")
            show_send_progress('error', recipient, smtp_email, 'Auth Failure')
            failed_smtp_servers[smtp_email] = {"retry_time": time.monotonic() + 1800, "reason": f"Auth Failed: {error}"}
            if retries == 0:
                safe_log('warning', f"Trying {recipient} with different SMTP due to auth failure on {smtp_email}.")
                try:
                    next_smtp_config = get_next_smtp_config(smtp_servers_ref)
                    if is_shutting_down: return False
                    return await send_email(next_smtp_config, recipient, subject, html_body, attachment_path, smtp_servers_ref, retries=0)
                except RuntimeError as next_smtp_error:
                    safe_log('error', f"No alternative SMTP for {recipient} after auth failure. Failing permanently. Error: {next_smtp_error}")
                    metrics.record_failure(domain, f"Auth failure, no alternative SMTP")
                    failed_emails_buffer.append({"email": recipient, "error": f"Auth failure on {smtp_email}, no alternative SMTP: {error}"})
                    return False
            else:
                safe_log('error', f"Auth failure on retry for {recipient} via {smtp_email}. Failing permanently.")
                metrics.record_failure(domain, f"Auth failure on retry")
                failed_emails_buffer.append({"email": recipient, "error": f"Auth failure on retry with {smtp_email}: {error}"})
                return False

        elif is_recipient_error or is_mx_error or is_sender_error or is_dns_timeout_error:
            reason = "Recipient Rejected" if is_recipient_error else \
                     ("MX/Domain Error" if is_mx_error else \
                     ("Sender Rejected" if is_sender_error else "DNS Timeout"))
            cleaned_specific_detail = specific_error_detail.replace('\n', ' ')
            log_msg = f"Permanent failure for {recipient}: {reason}. Code: {error_code}, Msg: {cleaned_specific_detail}"
            safe_log('warning', log_msg)
            show_send_progress('error', recipient, smtp_email, reason)
            metrics.record_failure(domain, f"{reason}: {error_code} {cleaned_specific_detail}")
            failed_emails_buffer.append({"email": recipient, "error": f"{reason}: {error_code} {cleaned_specific_detail}"})
            return False # No retry

        elif is_dns_451_error:
            if SKIP_DNS_FAILURES and retries == 0:
                safe_log('warning', f"Queueing {recipient} due to 451 DNS error from {smtp_email}. Error: {error}")
                show_send_progress('queued', recipient, smtp_email, '451 DNS Error - Queued')
                dns_failure_queue.append({
                    "recipient": recipient, "subject": subject, "html_body": html_body,
                    "attachment_path": attachment_path, "attempted_at": time.monotonic(),
                    "initial_smtp": smtp_email })
                return False
            else:
                max_dns_retries = MAX_RETRIES + 2
                if retries < max_dns_retries:
                    delay = min(RETRY_DELAY * (2.5 ** retries), 60.0)
                    safe_log('warning', f"Temp DNS error 451 for {recipient}. Retrying in {delay:.1f}s (Attempt {retries + 1}/{max_dns_retries}). Error: {error}")
                    show_send_progress('retry', recipient, smtp_email, f"451 DNS Error - Wait {delay:.0f}s")
                    if is_shutting_down: return False
                    await asyncio.sleep(delay)
                    try:
                        next_smtp_config = get_next_smtp_config(smtp_servers_ref)
                        if is_shutting_down: return False
                        return await send_email(next_smtp_config, recipient, subject, html_body, attachment_path, smtp_servers_ref, retries + 1)
                    except RuntimeError as next_smtp_error:
                         safe_log('error', f"No alternative SMTP for DNS 451 retry for {recipient}. Failing permanently. Error: {next_smtp_error}")
                         metrics.record_failure(domain, f"DNS 451 error, no alternative SMTP")
                         failed_emails_buffer.append({"email": recipient, "error": f"DNS 451 error, no alternative SMTP for retry: {error}"})
                         return False
                else:
                    safe_log('error', f"Permanent failure for {recipient} after {max_dns_retries} attempts (451 DNS error). Error: {error}")
                    show_send_progress('error', recipient, smtp_email, f"451 DNS Error - Max Retries")
                    metrics.record_failure(domain, f"451 DNS Error - Max Retries")
                    failed_emails_buffer.append({"email": recipient, "error": f"451 DNS Error - Max Retries Exceeded: {error}"})
                    return False

        elif is_temporary_error or is_connection_error:
            if retries < MAX_RETRIES:
                delay = min(RETRY_DELAY * (2 ** retries), 30.0)
                reason = "Connection Error" if is_connection_error else "Temporary Server Error"
                safe_log('warning', f"{reason} for {recipient}. Retrying in {delay:.1f}s (Attempt {retries + 1}/{MAX_RETRIES}). Code: {error_code}, Msg: {error}")
                show_send_progress('retry', recipient, smtp_email, f"{reason} - Wait {delay:.0f}s")
                if is_shutting_down: return False
                await asyncio.sleep(delay)
                try:
                    next_smtp_config = get_next_smtp_config(smtp_servers_ref)
                    if is_shutting_down: return False
                    return await send_email(next_smtp_config, recipient, subject, html_body, attachment_path, smtp_servers_ref, retries + 1)
                except RuntimeError as next_smtp_error:
                    safe_log('error', f"No alternative SMTP for {reason} retry for {recipient}. Failing permanently. Error: {next_smtp_error}")
                    metrics.record_failure(domain, f"{reason}, no alternative SMTP")
                    failed_emails_buffer.append({"email": recipient, "error": f"{reason}, no alternative SMTP for retry: {error}"})
                    return False
            else:
                reason = "Connection Error" if is_connection_error else "Temporary Server Error"
                safe_log('error', f"Permanent failure for {recipient} after {MAX_RETRIES} attempts ({reason}). Code: {error_code}, Msg: {error}")
                show_send_progress('error', recipient, smtp_email, f"{reason} - Max Retries")
                metrics.record_failure(domain, f"{reason} - Max Retries")
                failed_emails_buffer.append({"email": recipient, "error": f"{reason} - Max Retries Exceeded: {error_code} {error}"})
                return False

        else: # Unhandled error type
            safe_log('error', f"Unhandled error type for {recipient}. Code: {error_code}, Type: {type(error).__name__}, Error: {error}", exc_info=True)
            if retries < MAX_RETRIES:
                delay = min(RETRY_DELAY * (2 ** retries), 30.0)
                safe_log('warning', f"Unknown error type. Retrying in {delay:.1f}s (Attempt {retries + 1}/{MAX_RETRIES})...")
                show_send_progress('retry', recipient, smtp_email, f"Unknown Error - Wait {delay:.0f}s")
                if is_shutting_down: return False
                await asyncio.sleep(delay)
                try:
                    next_smtp_config = get_next_smtp_config(smtp_servers_ref)
                    if is_shutting_down: return False
                    return await send_email(next_smtp_config, recipient, subject, html_body, attachment_path, smtp_servers_ref, retries + 1)
                except RuntimeError as next_smtp_error:
                    safe_log('error', f"No alternative SMTP for Unknown Error retry for {recipient}. Failing permanently. Error: {next_smtp_error}")
                    metrics.record_failure(domain, f"Unknown error, no alternative SMTP")
                    failed_emails_buffer.append({"email": recipient, "error": f"Unknown error, no alternative SMTP for retry: {error}"})
                    return False
            else:
                safe_log('error', f"Permanent failure for {recipient} after {MAX_RETRIES} attempts (Unknown Error). Error: {error}")
                show_send_progress('error', recipient, smtp_email, 'Unknown Error - Max Retries')
                metrics.record_failure(domain, f"Unknown Error - Max Retries")
                failed_emails_buffer.append({"email": recipient, "error": f"Unknown Error - Max Retries Exceeded: {error}"})
                return False


# --- Batch Processing ---
async def process_batch(
    batch: List[str],
    smtp_servers: List[SmtpConfig],
    subject_template: str,
    html_template: str,
    attachment_path: Optional[Path]
):
    """Processes a batch of emails concurrently using the rate limiter."""
    tasks = []
    for recipient in batch:
        if is_shutting_down:
            safe_log('debug', f"Skipping task creation for {recipient} due to shutdown.")
            break

        async def send_task(current_recipient=recipient):
            global emails_processed_since_last_pause
            success = False
            try:
                smtp_config = get_next_smtp_config(smtp_servers)
                html_body = fill_template(html_template, current_recipient)
                subject = fill_template(subject_template, current_recipient)
                success = await send_email(
                    smtp_config, current_recipient, subject, html_body,
                    attachment_path, smtp_servers
                )
            except RuntimeError as e:
                 safe_log('error', f"Critical error processing {current_recipient} before send: {e}")
                 domain = current_recipient.split('@')[1] if '@' in current_recipient else 'unknown'
                 metrics.record_failure(domain, f"Preprocessing error: {e}")
                 failed_emails_buffer.append({"email": current_recipient, "error": f"Preprocessing error: {e}"})
            except asyncio.CancelledError:
                 safe_log('warning', f"Task for {current_recipient} cancelled during shutdown.")
                 failed_emails_buffer.append({"email": current_recipient, "error": "Task cancelled during shutdown"})
                 raise
            except Exception as e:
                 safe_log('error', f"Unexpected critical error for {current_recipient}: {e}", exc_info=True)
                 domain = current_recipient.split('@')[1] if '@' in current_recipient else 'unknown'
                 metrics.record_failure(domain, f"Unexpected error: {e}")
                 failed_emails_buffer.append({"email": current_recipient, "error": f"Unexpected error: {e}"})

            # --- Post-send processing ---
            if success:
                successful_emails_buffer.append(current_recipient)

            # Update main progress bar
            if main_task_id is not None and main_task_id in progress.task_ids and not progress.tasks[main_task_id].finished:
                 elapsed = time.monotonic() - metrics.start_time
                 current_speed = metrics.total_success / elapsed if elapsed > 0 else 0
                 try: progress.update(main_task_id, advance=1, speed=current_speed)
                 except Exception as progress_err: safe_log('debug', f"Error updating progress bar: {progress_err}")

            # --- Pause Logic ---
            emails_processed_since_last_pause += 1
            if PAUSE_INTERVAL_EMAILS > 0 and PAUSE_DURATION_S > 0:
                if emails_processed_since_last_pause >= PAUSE_INTERVAL_EMAILS:
                    if is_shutting_down: return success
                    safe_log('info', f"\n[yellow]⏸️ Pausing for {PAUSE_DURATION_S:.1f} seconds after ~{emails_processed_since_last_pause} emails processed...[/yellow]\n")
                    await asyncio.sleep(PAUSE_DURATION_S)
                    emails_processed_since_last_pause = 0
                    safe_log('info', "[green]▶️ Resuming email sending...[/green]")

            metrics.log_periodic_report()
            return success

        tasks.append(rate_limiter.run(send_task()))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results (optional: log unexpected exceptions from gather)
    for i, result in enumerate(results):
        if isinstance(result, asyncio.CancelledError): pass
        elif isinstance(result, Exception):
            recipient = batch[i] if i < len(batch) else "unknown recipient"
            safe_log('error', f"Unexpected error returned by gather for task {i} (recipient: {recipient}): {result}", exc_info=True)
            if not any(d.get('email') == recipient for d in failed_emails_buffer):
                 failed_emails_buffer.append({"email": recipient, "error": f"Unhandled error in gather: {result}"})


# --- Reporting ---
async def save_report(file_path: Path, emails_to_save: List[Any]):
    """Appends email addresses or failure details to a CSV file asynchronously."""
    if not emails_to_save: return

    count = len(emails_to_save)
    is_failure_report = isinstance(emails_to_save[0], dict) if count > 0 else False
    report_type = 'failures' if is_failure_report else 'successes'
    safe_log('info', f"Saving {count} {report_type} to {file_path}...")

    def write_csv_sync():
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = file_path.exists()
            with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['email', 'error'] if is_failure_report else ['email']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                if not file_exists or file_path.stat().st_size == 0: writer.writeheader()
                for item in emails_to_save:
                    if is_failure_report:
                        if isinstance(item, dict) and 'email' in item:
                            error_msg = str(item.get('error', 'Unknown Error'))[:1024] # Truncate long errors
                            writer.writerow({'email': item['email'], 'error': error_msg})
                        else: safe_log('warning', f"Skipping invalid item in failure report: {item}")
                    else:
                        if isinstance(item, str): writer.writerow({'email': item})
                        else: safe_log('warning', f"Skipping invalid item in success report: {item}")
        except IOError as e:
            safe_log('error', f"IOError saving report to {file_path}: {e}")
            raise
        except Exception as e:
             safe_log('error', f"Unexpected error writing report to {file_path}: {e}", exc_info=True)
             raise

    try:
        await asyncio.to_thread(write_csv_sync)
        safe_log('debug', f"Successfully saved {count} {report_type} to {file_path}.")
    except Exception as e:
        safe_log('error', f"Failed to save report to {file_path} (error logged previously).")


# --- Main Execution Function ---
async def main():
    global progress_active, main_task_id, queue_task_id, total_emails
    global successful_emails_buffer, failed_emails_buffer, success_count, failure_count

    console.print(Panel("[bold cyan]Bulk Email Sender (Python Async)[/]\n[cyan]Initialization Started[/]", title="Welcome", border_style="cyan"))

    # --- Initial Setup ---
    try:
        smtp_servers = await load_smtp_servers(SMTP_CONFIG_PATH)
        async with aiofiles.open(MESSAGE_FILE_PATH, mode='r', encoding='utf-8') as f:
            html_template = await f.read()
    except SystemExit: raise
    except Exception as e:
        console.print(f"[red]❌ Critical error during initial data loading: {e}[/red]")
        logger.critical("Data loading error", exc_info=True)
        sys.exit(1)

    # --- Load Recipients ---
    console.print("[blue]⏳ Loading recipients... (This may take time if MX check is enabled)[/blue]")
    all_recipients = []
    # Note: For extremely large lists, consider a streaming approach instead of loading all to memory.
    recipient_generator = load_recipients_in_batches(RECIPIENT_LIST_PATH, 1000) # Batch size for loading
    try:
        async for batch in recipient_generator: all_recipients.extend(batch)
    except SystemExit: return
    except Exception as e:
        console.print(f"[red]❌ Error occurred while loading recipients: {e}[/red]")
        logger.critical("Recipient loading error", exc_info=True)
        return

    if total_emails == 0:
        console.print("[yellow]⚠️ No valid recipients found after filtering. Exiting.[/yellow]")
        return
    console.print(f"[green]✅ Loaded {total_emails} valid and unique recipients.[/green]")

    # --- Display Configuration Summary ---
    mx_validation_mode = '[red]Disabled[/]'
    if not DISABLE_ALL_MX_VALIDATION:
        if CHECK_MX_DURING_LOAD: mx_validation_mode = '[yellow]During Load[/]'
        elif CHECK_MX_BEFORE_SEND: mx_validation_mode = '[cyan]Before Send[/]'
        else: mx_validation_mode = '[grey]None (Rely on SMTP)[/]'
    tls_verification_mode = '[bold red]DISABLED (Hardcoded)[/]' # Reflects hardcoded setting

    config_summary = f"""[bold]Configuration Summary[/]
---------------------------
SMTP Servers: {len(smtp_servers)} | Total Recipients: {total_emails}
Subject: "{EMAIL_SUBJECT_TEMPLATE}" | Attachment: {ATTACHMENT_PATH.name if ATTACHMENT_PATH else 'None'}
Concurrency: {INITIAL_CONCURRENCY} (Adaptive: [{MIN_CONCURRENCY}-{MAX_CONCURRENCY}]) | Batch Size: {BATCH_SIZE} (Adaptive: [{MIN_BATCH_SIZE}-{MAX_BATCH_SIZE}])
Retries: {MAX_RETRIES} | Retry Delay: {RETRY_DELAY*1000:.0f}ms (Exp) | TLS Verify: {tls_verification_mode}
MX Mode: {mx_validation_mode} | Queue 451 DNS: {'[green]Yes[/]' if SKIP_DNS_FAILURES else '[red]No[/]'} | Test Mode: {'[yellow bold]YES[/]' if SMTP_TEST else 'No'}
Pause: {'Every {} emails for {:.1f}s'.format(PAUSE_INTERVAL_EMAILS, PAUSE_DURATION_S) if PAUSE_INTERVAL_EMAILS > 0 else '[grey]Disabled[/]'}
Logs: Success='{SUCCESS_FILE_PATH}', Failure='{FAILED_FILE_PATH}'
---------------------------
[dim]Tip: Adjust MAX_CONCURRENCY/MAX_BATCH_SIZE in .env for speed. Monitor errors![/dim]
[dim]Tip: Set CHECK_MX_BEFORE_SEND=false in .env for faster sends (if list is clean).[/dim]"""
    console.print(Panel(config_summary, title="Setup", border_style="cyan"))

    # --- Start Sending Process ---
    metrics.start_time = time.monotonic()
    console.print("[blue]🚀 Starting email sending process... Press Ctrl+C to stop gracefully.[/blue]")

    processed_index = 0
    try:
        with progress:
            main_task_id = progress.add_task("[green]Sending", total=total_emails, speed=0)
            progress_active = True

            while processed_index < total_emails and not is_shutting_down:
                current_batch_size = adaptive_control.get_batch_size()
                batch = all_recipients[processed_index : processed_index + current_batch_size]
                if not batch: break

                safe_log('debug', f"Processing batch {processed_index // current_batch_size + 1}, Size: {len(batch)}. Concurrency: {adaptive_control.concurrency}")
                await process_batch(batch, smtp_servers, EMAIL_SUBJECT_TEMPLATE, html_template, ATTACHMENT_PATH)
                processed_index += len(batch)

                # --- Save reports periodically ---
                current_success_in_buffer = len(successful_emails_buffer)
                current_failure_in_buffer = len(failed_emails_buffer)
                save_tasks = []
                if current_success_in_buffer > 0:
                    success_to_save = list(successful_emails_buffer)
                    successful_emails_buffer.clear()
                    save_tasks.append(save_report(SUCCESS_FILE_PATH, success_to_save))
                else: success_to_save = []
                if current_failure_in_buffer > 0:
                    failure_to_save = list(failed_emails_buffer)
                    failed_emails_buffer.clear()
                    save_tasks.append(save_report(FAILED_FILE_PATH, failure_to_save))
                else: failure_to_save = []

                if save_tasks:
                    await asyncio.gather(*save_tasks)
                    success_count += len(success_to_save) # Update counts AFTER saving
                    failure_count += len(failure_to_save)
                    safe_log('debug', f"Saved reports. Batch Success: {len(success_to_save)}, Fail: {len(failure_to_save)}. Total S: {success_count}, F: {failure_count}")

                if is_shutting_down:
                    safe_log('info', "Shutdown detected after batch processing and saving.")
                    break

            if not is_shutting_down and main_task_id is not None and main_task_id in progress.task_ids:
                final_processed = success_count + failure_count
                final_completed = min(final_processed, total_emails)
                progress.update(main_task_id, completed=final_completed)

    finally:
        progress_active = False

    # --- Process DNS Failure Queue ---
    if dns_failure_queue and not is_shutting_down:
        console.print(f"\n[yellow]--- Retrying {len(dns_failure_queue)} emails from DNS 451 Failure Queue ---[/yellow]")
        dns_failure_queue.sort(key=lambda item: item['attempted_at'])
        queue_concurrency = max(1, adaptive_control.min_concurrency // 2)
        queue_rate_limiter = RateLimiter(queue_concurrency)
        safe_log('info', f"Using concurrency {queue_concurrency} for DNS queue processing.")

        queue_tasks = []
        try:
            with progress:
                queue_task_id = progress.add_task("[yellow]DNS Queue", total=len(dns_failure_queue))
                progress_active = True

                for item in dns_failure_queue:
                    if is_shutting_down: break

                    async def retry_queued_task(queued_item: Dict[str, Any]):
                        success = False
                        recipient = queued_item['recipient']
                        try:
                            await asyncio.sleep(random.uniform(0.5, 2.0)) # Small delay
                            safe_log('info', f"Retrying queued email to {recipient} (Original SMTP: {queued_item['initial_smtp']})")
                            smtp_config = get_next_smtp_config(smtp_servers)
                            success = await send_email(
                                smtp_config, recipient, queued_item['subject'], queued_item['html_body'],
                                queued_item['attachment_path'], smtp_servers, retries=0
                            )
                        except RuntimeError as e:
                            safe_log('error', f"Error retrying queued email {recipient} (SMTP selection): {e}")
                            failed_emails_buffer.append({"email": recipient, "error": f"Failed on queue retry (SMTP selection): {e}"})
                        except asyncio.CancelledError:
                            safe_log('warning', f"DNS Queue Task for {recipient} cancelled.")
                            failed_emails_buffer.append({"email": recipient, "error": "DNS Queue Task cancelled"})
                            raise
                        except Exception as e:
                            safe_log('error', f"Unexpected error retrying queued email {recipient}: {e}", exc_info=True)
                            failed_emails_buffer.append({"email": recipient, "error": f"Failed on queue retry: {e}"})

                        if success: successful_emails_buffer.append(recipient)

                        if queue_task_id is not None and queue_task_id in progress.task_ids and not progress.tasks[queue_task_id].finished:
                            try: progress.update(queue_task_id, advance=1)
                            except Exception as progress_err: safe_log('debug', f"Error updating DNS queue progress bar: {progress_err}")
                        return success

                    queue_tasks.append(queue_rate_limiter.run(retry_queued_task(item)))

                queue_results = await asyncio.gather(*queue_tasks, return_exceptions=True)

                # --- Save final reports from queue processing ---
                queue_success_to_save = list(successful_emails_buffer)
                queue_failure_to_save = list(failed_emails_buffer)
                successful_emails_buffer.clear()
                failed_emails_buffer.clear()
                final_queue_saves = []
                if queue_success_to_save: final_queue_saves.append(save_report(SUCCESS_FILE_PATH, queue_success_to_save))
                if queue_failure_to_save: final_queue_saves.append(save_report(FAILED_FILE_PATH, queue_failure_to_save))

                if final_queue_saves:
                    await asyncio.gather(*final_queue_saves)
                    success_count += len(queue_success_to_save) # Update counts AFTER saving
                    failure_count += len(queue_failure_to_save)
                    safe_log('debug', f"Saved DNS queue reports. Success: {len(queue_success_to_save)}, Fail: {len(queue_failure_to_save)}. Final S: {success_count}, F: {failure_count}")

                for r in queue_results:
                    if isinstance(r, asyncio.CancelledError): pass
                    elif isinstance(r, Exception): safe_log('error', f"Unexpected error returned by gather for DNS queue task: {r}", exc_info=True)

                if not is_shutting_down and queue_task_id is not None and queue_task_id in progress.task_ids:
                     final_queue_processed = len(queue_success_to_save) + len(queue_failure_to_save)
                     final_queue_completed = min(final_queue_processed, len(dns_failure_queue))
                     progress.update(queue_task_id, completed=final_queue_completed)

        finally:
            progress_active = False

    # --- Final Report ---
    if not is_shutting_down:
        metrics.log_periodic_report(force=True) # Log final report
        total_duration_seconds = time.monotonic() - metrics.start_time
        final_success_rate = ((success_count / total_emails) * 100) if total_emails > 0 else 0
        final_avg_speed = (success_count / total_duration_seconds) if total_duration_seconds > 0 else 0

        completion_summary = f"""[bold green]Process Complete[/]
---------------------------
Total Time: {total_duration_seconds:.2f} seconds ({total_duration_seconds/60:.1f} minutes)
[green]Successful Sends:[/green] {success_count} ({final_success_rate:.1f}%) | [red]Failed Sends:[/red] {failure_count}
Final Average Speed: {final_avg_speed:.2f} emails/s
Logs: Success='{SUCCESS_FILE_PATH}', Failure='{FAILED_FILE_PATH}'
DNS Queue Items Processed: {len(dns_failure_queue)} (Results included above)
---------------------------"""
        console.print(Panel(completion_summary, title="Finished", border_style="green"))


# --- Graceful Shutdown & Signal Handling ---
_shutdown_task = None

async def graceful_shutdown(sig: signal.Signals):
    """Handles SIGINT/SIGTERM for graceful shutdown."""
    global is_shutting_down, progress_active, _shutdown_task
    if is_shutting_down:
        safe_log('debug', f"Shutdown already in progress, ignoring signal {sig.name}")
        return
    is_shutting_down = True
    console.print(f"\n[yellow]🛑 Signal {sig.name} received. Starting graceful shutdown...[/yellow]")
    safe_log('warning', f"Shutdown initiated by signal {sig.name}")

    if progress_active:
        try:
            if main_task_id is not None or queue_task_id is not None: progress.stop()
            safe_log('debug',"Progress bar stopped.")
        except Exception as e: safe_log('warning', f"Error stopping progress bar: {e}")
        finally: progress_active = False

    safe_log('info', "Cancelling pending tasks...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    cancelled_count = 0
    if tasks:
        for task in tasks:
            if not task.done(): task.cancel(); cancelled_count += 1
        if cancelled_count > 0:
            safe_log('info', f"Waiting up to 5s for {cancelled_count} tasks to cancel...")
            done, pending = await asyncio.wait(tasks, timeout=5.0, return_when=asyncio.ALL_COMPLETED)
            if pending: safe_log('warning', f"{len(pending)} tasks did not cancel gracefully.")
            else: safe_log('info', "All cancellable tasks finished.")
        else: safe_log('info', "No active tasks needed cancellation.")
    else: safe_log('info', "No other tasks were running.")

    console.print("[yellow]💾 Saving pending reports due to shutdown...[/yellow]")
    try:
        final_success = list(successful_emails_buffer)
        final_failed = list(failed_emails_buffer)
        successful_emails_buffer.clear()
        failed_emails_buffer.clear()

        remaining_queue_items = []
        if dns_failure_queue:
             queue_items_processed_estimate = 0
             if queue_task_id is not None and queue_task_id in progress.task_ids:
                 try: queue_items_processed_estimate = int(progress.tasks[queue_task_id].completed)
                 except Exception: pass
             remaining_queue_items = dns_failure_queue[queue_items_processed_estimate:]

        interrupted_items = [{"email": item['recipient'], "error": f"Interrupted by {sig.name} before/during DNS queue processing"} for item in remaining_queue_items]
        final_failed.extend(interrupted_items)

        save_tasks = []
        if final_success: save_tasks.append(save_report(SUCCESS_FILE_PATH, final_success))
        if final_failed: save_tasks.append(save_report(FAILED_FILE_PATH, final_failed))

        if save_tasks:
            await asyncio.gather(*save_tasks)
            if interrupted_items: console.print(f"[yellow]💾 Saved {len(interrupted_items)} pending/interrupted DNS queue emails to {FAILED_FILE_PATH}.[/yellow]")
            console.print("[green]✅ Pending reports saved.[/green]")
        else: console.print("[yellow]✅ No pending reports needed saving.[/yellow]")

    except Exception as e:
        console.print(f"[red]❌ Error saving reports during shutdown: {e}[/red]")
        logger.exception("Error during shutdown save")

    console.print("[yellow]👋 Exiting now.[/yellow]")
    logging.shutdown()
    os._exit(0) # Forceful exit after cleanup

def shutdown_handler(sig, frame):
    """Synchronous signal handler wrapper for SIGTERM/SIGINT."""
    global is_shutting_down, _shutdown_task
    safe_log('debug', f"Signal {sig} received by sync handler.")
    if not is_shutting_down and _shutdown_task is None:
        try:
            loop = asyncio.get_running_loop()
            _shutdown_task = loop.create_task(graceful_shutdown(signal.Signals(sig)))
        except RuntimeError:
            is_shutting_down = True
            print(f"\n[yellow]🛑 Signal {signal.Signals(sig).name} received, but event loop is not running. Attempting direct exit.[/yellow]")
            logging.shutdown()
            sys.exit(0)
        except Exception as e:
            print(f"\n[red]❌ Error scheduling shutdown task: {e}[/red]")
            logging.shutdown()
            sys.exit(1)

def handle_sigusr1(sig, frame):
    """Synchronous signal handler for SIGUSR1 to reset failed SMTPs."""
    safe_log('info', "\n[yellow]🔄 SIGUSR1 received. Resetting failed SMTP server list...[/yellow]")
    count = len(failed_smtp_servers)
    failed_smtp_servers.clear()
    safe_log('info', f"[green]✅ Cleared {count} failed SMTP entries. They will be retried on next use.[/green]\n")


# --- Script Entry Point ---
if __name__ == "__main__":
    # --- Setup Signal Handlers ---
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    try:
        sigusr1 = getattr(signal, 'SIGUSR1', None)
        if sigusr1:
            signal.signal(sigusr1, handle_sigusr1)
            console.print(f"[blue]ℹ️ SIGUSR1 handler registered (PID: {os.getpid()}). Use 'kill -SIGUSR1 {os.getpid()}' to reset failed SMTPs.[/blue]")
        else: console.print("[yellow]⚠️ SIGUSR1 signal not available on this OS (likely Windows).[/yellow]")
    except (AttributeError, ValueError, OSError) as e:
         console.print(f"[yellow]⚠️ Could not set SIGUSR1 handler: {e}[/yellow]")

    # --- Run the Main Async Function ---
    exit_code = 0
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        if not is_shutting_down: console.print("\n[yellow]Main execution interrupted or cancelled unexpectedly.[/yellow]")
        exit_code = 1
    except SystemExit as e:
         exit_code = e.code if isinstance(e.code, int) else 1
         if exit_code != 0: console.print(f"[red]Exiting with code {exit_code}[/red]")
    except Exception as e:
        if progress_active:
            try: progress.stop()
            except Exception: pass
        console.print(f"\n[bold red]💥 Unhandled critical error in main execution:[/]")
        logger.critical("Unhandled error in main execution", exc_info=True)
        exit_code = 1

        # --- Emergency Save Attempt ---
        if not is_shutting_down:
             console.print("[yellow]Attempting emergency synchronous save before exiting due to error...[/yellow]")
             try:
                 def save_sync_emergency(filepath, data):
                     if not data: return
                     is_failure = isinstance(data[0], dict) if data else False
                     try:
                         filepath.parent.mkdir(parents=True, exist_ok=True)
                         exists = filepath.exists()
                         with open(filepath, 'a', newline='', encoding='utf-8') as f:
                             fieldnames = ['email', 'error'] if is_failure else ['email']
                             writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                             if not exists or filepath.stat().st_size == 0: writer.writeheader()
                             for item in data:
                                 if is_failure: writer.writerow({'email': item.get('email','?'), 'error': str(item.get('error','?'))[:1024]})
                                 else: writer.writerow({'email': str(item)})
                         console.print(f"[yellow]💾 Emergency save to {filepath} attempted ({len(data)} items).[/yellow]")
                     except Exception as save_err: console.print(f"[red]❌ Emergency save to {filepath} failed: {save_err}[/red]")
                 save_sync_emergency(SUCCESS_FILE_PATH, successful_emails_buffer)
                 save_sync_emergency(FAILED_FILE_PATH, failed_emails_buffer)
             except Exception as final_save_err: console.print(f"[red]Error during emergency save attempt: {final_save_err}[/red]")
    finally:
        if not is_shutting_down: safe_log('info', "Main execution finished or terminated.")
        logging.shutdown()
        console.print(f"[grey]Application finished with exit code {exit_code}.[/grey]")
        sys.exit(exit_code)