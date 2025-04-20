import glob
import os
import re
import subprocess
import threading
import time
import queue
import atexit
from datetime import datetime
from flask import Flask, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# --- Bot Process Management (for telegram_monitor.py directly) ---
# This remains separate from the interactive CLI management
BOT_PROCESS = None
BOT_LOCK = threading.Lock()
BOT_LOG = "logs/telegram_bot.log"
START_BOT_CMD = ["python", "scripts/telegram_monitor.py"] # Command to start the monitor directly

def start_bot():
    """Starts the telegram_monitor.py process directly."""
    global BOT_PROCESS
    with BOT_LOCK:
        if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
            return False, "Bot monitor process already running."
        print("Starting bot monitor process...")
        # Start telegram_monitor.py directly
        BOT_PROCESS = subprocess.Popen(START_BOT_CMD, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(f"Bot monitor process started with PID: {BOT_PROCESS.pid}")
        return True, "Bot monitor process started."

def stop_bot():
    """Stops the telegram_monitor.py process."""
    global BOT_PROCESS
    with BOT_LOCK:
        if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
            print(f"Stopping bot monitor process (PID: {BOT_PROCESS.pid})...")
            BOT_PROCESS.terminate()
            try:
                BOT_PROCESS.wait(timeout=10)
                print("Bot monitor process stopped.")
            except subprocess.TimeoutExpired:
                print("Bot monitor process did not terminate gracefully, killing.")
                BOT_PROCESS.kill()
            BOT_PROCESS = None
            return True, "Bot monitor process stopped."
        return False, "Bot monitor process not running."

def bot_status():
    """Checks the status of the telegram_monitor.py process."""
    with BOT_LOCK:
        if BOT_PROCESS is not None and BOT_PROCESS.poll() is None:
            return True, f"Bot monitor process is running (PID: {BOT_PROCESS.pid})."
        return False, "Bot monitor process is not running."

# --- NEW Interactive CLI Management (for start_bot.sh) ---
CLI_PROCESS = None
CLI_OUTPUT_QUEUE = queue.Queue()
CLI_LOCK = threading.Lock()
CLI_PROMPT = "TgBot> " # The prompt string from start_bot.sh
CLI_READER_THREAD = None
CLI_STOP_EVENT = threading.Event()
START_BOT_SHELL_CMD = ["bash", "start_bot.sh"] # Command to start the interactive shell

def read_cli_output():
    """Reads output from the CLI process and puts it into the queue."""
    print("CLI output reader thread started.")
    while not CLI_STOP_EVENT.is_set():
        if CLI_PROCESS is None or CLI_PROCESS.stdout is None:
            break
        try:
            line = CLI_PROCESS.stdout.readline()
            if line:
                # print(f"CLI_OUT: {line.strip()}") # Debug print
                CLI_OUTPUT_QUEUE.put(line)
            else:
                # Process likely exited or stdout closed
                print("CLI stdout readline returned empty, exiting reader thread.")
                break
        except ValueError:
             # Handle case where stdout might be closed during readline
             print("CLI stdout closed, exiting reader thread.")
             break
        except Exception as e:
            # Handle other potential exceptions during read
            error_line = f"Error reading CLI output: {e}\n"
            print(error_line)
            CLI_OUTPUT_QUEUE.put(error_line)
            break
    print("CLI output reader thread finished.")

def start_persistent_cli():
    """Starts the persistent start_bot.sh process and its output reader."""
    global CLI_PROCESS, CLI_READER_THREAD
    # Ensure lock is acquired before modifying globals
    with CLI_LOCK:
        if CLI_PROCESS is not None and CLI_PROCESS.poll() is None:
            print("CLI process already running.")
            return True, "CLI process already running."

        print("Starting persistent CLI process (start_bot.sh)...")
        try:
            # Ensure start_bot.sh is executable
            if os.path.exists("start_bot.sh"):
                 os.chmod("start_bot.sh", 0o755)
            else:
                 return False, "start_bot.sh not found."

            CLI_PROCESS = subprocess.Popen(
                START_BOT_SHELL_CMD,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, # Redirect stderr to stdout
                text=True,
                bufsize=1, # Line-buffered
                universal_newlines=True, # Ensures text mode works correctly
                shell=False # Use list of args, not a shell string
            )
            print(f"CLI process started with PID: {CLI_PROCESS.pid}")
            CLI_STOP_EVENT.clear()
            CLI_READER_THREAD = threading.Thread(target=read_cli_output, daemon=True)
            CLI_READER_THREAD.start()

            # Wait for the initial prompt to ensure readiness
            print("Waiting for initial CLI prompt...")
            initial_output_lines = []
            start_time = time.time()
            # Increased timeout for potentially long start_bot.sh setup
            initial_prompt_timeout = 90

            while time.time() - start_time < initial_prompt_timeout:
                try:
                    line = CLI_OUTPUT_QUEUE.get(timeout=1)
                    initial_output_lines.append(line)
                    # print(f"INIT_OUT: {line.strip()}") # Debug print
                    if CLI_PROMPT in line:
                        print("Initial CLI prompt received. Process ready.")
                        # Optionally clear initial output from queue if not needed by caller
                        # Or return it if useful
                        return True, "CLI process started successfully."
                except queue.Empty:
                    # Check if process died during startup
                    if CLI_PROCESS.poll() is not None:
                         print("CLI process died unexpectedly during startup.")
                         # Read any remaining output
                         while not CLI_OUTPUT_QUEUE.empty():
                              initial_output_lines.append(CLI_OUTPUT_QUEUE.get())
                         error_msg = f"CLI process terminated during startup. Output:\n{''.join(initial_output_lines)}"
                         stop_persistent_cli() # Clean up
                         return False, error_msg
                    continue # No output yet, keep waiting
                except Exception as e:
                     print(f"Error waiting for initial prompt: {e}")
                     stop_persistent_cli() # Clean up on error
                     return False, f"Error waiting for initial prompt: {e}"

            # Timeout waiting for initial prompt
            print(f"Timeout ({initial_prompt_timeout}s) waiting for initial CLI prompt.")
            # Read any output received during timeout
            while not CLI_OUTPUT_QUEUE.empty():
                 initial_output_lines.append(CLI_OUTPUT_QUEUE.get())
            error_msg = f"Timeout waiting for initial CLI prompt. Output received:\n{''.join(initial_output_lines)}"
            stop_persistent_cli() # Clean up
            return False, error_msg

        except Exception as e:
            print(f"Failed to start CLI process: {e}")
            CLI_PROCESS = None
            return False, f"Failed to start CLI process: {e}"

def stop_persistent_cli():
    """Stops the persistent CLI process and reader thread."""
    global CLI_PROCESS, CLI_READER_THREAD
    # Acquire lock to prevent race conditions during shutdown
    with CLI_LOCK:
        print("Attempting to stop persistent CLI process...")
        CLI_STOP_EVENT.set() # Signal reader thread to stop

        if CLI_PROCESS and CLI_PROCESS.poll() is None:
            print(f"CLI process (PID: {CLI_PROCESS.pid}) is running. Sending 'exit' command.")
            try:
                # Try sending exit command first
                CLI_PROCESS.stdin.write("exit\n")
                CLI_PROCESS.stdin.flush()
                CLI_PROCESS.wait(timeout=5)
                print("CLI process exited gracefully.")
            except (OSError, BrokenPipeError, subprocess.TimeoutExpired) as e:
                print(f"'exit' command failed or timed out ({e}). Terminating process.")
                # Force terminate if exit doesn't work or pipe is broken
                CLI_PROCESS.terminate()
                try:
                    CLI_PROCESS.wait(timeout=5)
                    print("CLI process terminated.")
                except subprocess.TimeoutExpired:
                    print("Termination timed out. Killing process.")
                    CLI_PROCESS.kill() # Last resort
                    print("CLI process killed.")
            except Exception as e_inner:
                 print(f"Unexpected error during CLI process stop: {e_inner}")
                 # Attempt kill as fallback
                 try:
                      CLI_PROCESS.kill()
                      print("CLI process killed as fallback.")
                 except Exception as e_kill:
                      print(f"Failed to kill CLI process: {e_kill}")

        elif CLI_PROCESS and CLI_PROCESS.poll() is not None:
             print("CLI process already terminated.")
        else:
             print("CLI process was not running.")

        CLI_PROCESS = None # Clear the process variable

        # Wait for reader thread to finish
        if CLI_READER_THREAD and CLI_READER_THREAD.is_alive():
            print("Waiting for CLI reader thread to join...")
            CLI_READER_THREAD.join(timeout=2)
            if CLI_READER_THREAD.is_alive():
                 print("Reader thread did not join.")
            else:
                 print("Reader thread joined.")
        CLI_READER_THREAD = None

        # Clear any remaining items in the queue
        cleared_count = 0
        while not CLI_OUTPUT_QUEUE.empty():
            try:
                CLI_OUTPUT_QUEUE.get_nowait()
                cleared_count += 1
            except queue.Empty:
                break
        if cleared_count > 0:
             print(f"Cleared {cleared_count} items from CLI output queue.")

        print("Persistent CLI resources cleaned up.")


# --- Log Reading ---
def get_latest_telegram_log():
    log_files = glob.glob("logs/telegram_bot_*.log")
    if not log_files and os.path.exists(BOT_LOG):
        return BOT_LOG
    if not log_files:
        return None
    # Pick the most recently modified log file
    log_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
    return log_files[0]

def read_log(lines=50):
    logfile = get_latest_telegram_log()
    if not logfile:
         return "Error: No log file found."
    try:
        with open(logfile, "r", encoding='utf-8') as f:
            # Efficiently read last N lines
            return "".join(f.readlines()[-lines:])
    except FileNotFoundError:
        return f"Error reading log: File not found at {logfile}"
    except Exception as e:
        return f"Error reading log: {e}"

# --- Real-Time Log Streaming ---
@app.route("/log/stream")
def stream_log():
    logfile = get_latest_telegram_log()
    if not logfile:
         return Response("Error: No log file found.", status=404, mimetype="text/plain")
    def generate():
        try:
            with open(logfile, "r", encoding='utf-8') as f:
                # Seek to the end of the file initially
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if not line:
                        # No new line, wait briefly and check again
                        time.sleep(0.5)
                        # Check if process is still running or file still exists
                        if not os.path.exists(logfile):
                             yield "Log file disappeared.\n"
                             break
                        continue
                    yield line
        except Exception as e:
             yield f"Error streaming log: {e}\n"
    return Response(stream_with_context(generate()), mimetype="text/plain")

# --- Forwarding Status ---
def get_forwarding_status():
    try:
        with open("config/forwarding_status.txt", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return "unknown (file not found)"
    except Exception as e:
        return f"unknown (error: {e})"

# --- Docker Management ---
def docker_command(args):
    try:
        # Use docker compose command directly
        cmd = ["docker", "compose"] + args
        print(f"Running Docker command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30) # Increased timeout
        output = result.stdout + result.stderr
        print(f"Docker command output:\n{output}")
        return output
    except FileNotFoundError:
         return "Error running docker command: 'docker compose' not found. Is Docker installed and in PATH?"
    except subprocess.TimeoutExpired:
         return "Error running docker command: Command timed out."
    except Exception as e:
        return f"Error running docker command: {e}"

# --- Database Checks ---
def get_container_id(name_fragment):
     """Helper to get the full ID of a running container by name fragment."""
     try:
          cmd = ["docker", "ps", "-qf", f"name={name_fragment}"]
          result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=5)
          container_id = result.stdout.strip().split('\n')[0] # Get first match if multiple
          return container_id
     except Exception:
          return None

def check_postgres():
    pg_container_id = get_container_id("postgres")
    if not pg_container_id:
         return "Error checking postgres: Container 'postgres' not found or not running."
    try:
        cmd = [
            "docker", "exec", pg_container_id,
            "pg_isready", "-U", "bot", "-d", "crypto_db", "-h", "localhost" # Assuming db runs in container localhost
        ]
        print(f"Running PG check: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout + result.stderr
    except Exception as e:
        return f"Error checking postgres: {e}"

def check_mongo():
    mongo_container_id = get_container_id("mongo")
    if not mongo_container_id:
         return "Error checking mongo: Container 'mongo' not found or not running."
    try:
        # Updated command for modern MongoDB versions
        cmd = [
            "docker", "exec", mongo_container_id,
            "mongosh", "--eval", "db.runCommand({ serverStatus: 1 }).ok",
            "--quiet", # Suppress connection messages
            "-u", "bot", "-p", "bot1234", "--authenticationDatabase", "admin" # Add auth
        ]
        print(f"Running Mongo check: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        output = result.stdout.strip() + result.stderr.strip()
        if output == '1': # mongosh --eval returns '1' for OK in this case
             return "MongoDB connection successful (serverStatus.ok: 1)"
        else:
             return f"MongoDB check failed or returned unexpected output: {output}"
    except Exception as e:
        return f"Error checking mongo: {e}"


# --- Get Available CLI Commands ---
def get_startbot_commands():
    try:
        with open("start_bot.sh", "r") as f:
            content = f.read()
        # Find the commands=... line
        match = re.search(r'commands="([^"]+)"', content)
        if match:
            cmds = match.group(1).split()
            return cmds
        # Fallback: parse from help section if commands= line not found
        help_cmds = re.findall(r'echo -e ".*?\$\{GREEN\}([\w-]+)', content)
        if help_cmds:
             return help_cmds
        return ["Error: Could not parse commands from start_bot.sh"]
    except FileNotFoundError:
         return ["Error: start_bot.sh not found"]
    except Exception as e:
        return [f"Error parsing commands: {e}"]

@app.route("/cli/commands", methods=["GET"])
def api_cli_commands():
    cmds = get_startbot_commands()
    return jsonify({"commands": cmds})

# --- Flask Endpoints ---

# Endpoints for direct telegram_monitor.py process management
@app.route("/start", methods=["POST"])
def api_start():
    ok, msg = start_bot()
    return jsonify({"success": ok, "message": msg})

@app.route("/stop", methods=["POST"])
def api_stop():
    ok, msg = stop_bot()
    return jsonify({"success": ok, "message": msg})

@app.route("/status", methods=["GET"])
def api_status():
    ok, msg = bot_status()
    return jsonify({"running": ok, "message": msg})

# NEW Endpoint for interactive start_bot.sh CLI
@app.route("/interactive_cli", methods=["POST"])
def api_interactive_cli():
    data = request.get_json(force=True)
    cmd = data.get("command")
    if not cmd:
        return jsonify({"error": "Missing 'command' in request body"}), 400

    # Use a longer timeout for reading command output, adjustable if needed
    read_timeout = data.get("timeout", 60) # Allow client to specify timeout

    output = "Error: CLI interaction lock could not be acquired." # Default error
    if CLI_LOCK.acquire(timeout=10): # Wait up to 10s for the lock
        try:
            # Ensure CLI process is running
            if CLI_PROCESS is None or CLI_PROCESS.poll() is not None:
                print("CLI process not running, attempting to start...")
                ok, msg = start_persistent_cli()
                if not ok:
                    return jsonify({"error": f"CLI process not running and failed to start: {msg}"}), 500
                print("CLI process started for interactive command.")

            # Send command
            try:
                print(f"Sending command to interactive CLI: {cmd}")
                CLI_PROCESS.stdin.write(cmd + "\n")
                CLI_PROCESS.stdin.flush()
            except (OSError, BrokenPipeError) as e:
                print(f"Error writing to CLI stdin: {e}. Stopping CLI process.")
                stop_persistent_cli() # Stop potentially broken process
                return jsonify({"error": f"Error sending command to CLI (pipe broken?): {e}"}), 500
            except Exception as e:
                 print(f"Unexpected error writing to CLI stdin: {e}")
                 return jsonify({"error": f"Unexpected error sending command: {e}"}), 500


            # Read output until prompt or timeout
            output_lines = []
            full_output_for_debug = [] # Store all lines received for debugging timeouts
            start_time = time.time()
            prompt_found = False

            while time.time() - start_time < read_timeout:
                try:
                    line = CLI_OUTPUT_QUEUE.get(timeout=1) # Check queue every second
                    full_output_for_debug.append(line)
                    # print(f"READ_OUT: {line.strip()}") # Debug print

                    # Check if the line contains the prompt
                    if CLI_PROMPT in line:
                        prompt_found = True
                        # Capture content before the prompt on the same line, if any
                        prompt_index = line.find(CLI_PROMPT)
                        if prompt_index > 0:
                            output_lines.append(line[:prompt_index])
                        break # Prompt found, command finished
                    else:
                        # Append line, ensuring it ends with a newline for consistency
                        output_lines.append(line.rstrip('\n') + '\n')

                except queue.Empty:
                    # No output for 1 second, check if process died
                    if CLI_PROCESS.poll() is not None:
                         print("CLI process terminated unexpectedly while waiting for output.")
                         output_lines.append("\nError: CLI process terminated unexpectedly.\n")
                         stop_persistent_cli() # Clean up
                         break
                    # Otherwise, just continue waiting for output
                    continue
                except Exception as e:
                     error_line = f"\nError reading output queue: {e}\n"
                     print(error_line)
                     output_lines.append(error_line)
                     break # Stop reading on error

            # After loop, check for timeout
            if not prompt_found and (time.time() - start_time >= read_timeout):
                timeout_msg = f"\nError: Timeout ({read_timeout}s) waiting for command '{cmd}' to complete (prompt '{CLI_PROMPT}' not received).\n"
                print(timeout_msg.strip())
                print(f"Full output received before timeout: {''.join(full_output_for_debug)}")
                output_lines.append(timeout_msg)

            output = "".join(output_lines)

        finally:
            CLI_LOCK.release() # Ensure lock is always released
    else:
         print("Failed to acquire CLI lock for command:", cmd)
         # Return error if lock couldn't be acquired
         return jsonify({"error": "Could not acquire lock for CLI interaction, server busy?"}), 503


    # print(f"Command '{cmd}' final output:\n{output}") # Debug log final output
    return jsonify({"output": output})


# Log reading endpoint
@app.route("/log", methods=["GET"])
def api_log():
    lines = int(request.args.get("lines", 50))
    log_content = read_log(lines)
    return jsonify({"log": log_content})

# Forwarding status endpoint
@app.route("/forwarding_status", methods=["GET"])
def api_forwarding_status():
    status = get_forwarding_status()
    return jsonify({"forwarding_status": status})

# Docker management endpoints
@app.route("/docker/restart/<service>", methods=["POST"])
def api_docker_restart(service):
    # Use 'compose restart'
    output = docker_command(["restart", service])
    return jsonify({"output": output})

@app.route("/docker/status", methods=["GET"])
def api_docker_status():
    # Use 'compose ps'
    output = docker_command(["ps"])
    return jsonify({"output": output})

# Database check endpoints
@app.route("/db/postgres", methods=["GET"])
def api_db_postgres():
    output = check_postgres()
    return jsonify({"output": output})

@app.route("/db/mongo", methods=["GET"])
def api_db_mongo():
    output = check_mongo()
    return jsonify({"output": output})

# --- Server Start/Stop ---

# Flag to prevent multiple initializations by Flask reloader
cli_initialized = False

def initialize_persistent_cli():
    """Initialize the CLI process exactly once."""
    global cli_initialized
    if not cli_initialized:
        print("Initializing persistent CLI process...")
        start_persistent_cli()
        cli_initialized = True
    else:
        print("Persistent CLI already initialized.")

# Register cleanup function to stop the CLI process on exit
atexit.register(stop_persistent_cli)

if __name__ == "__main__":
    # Initialize CLI before starting Flask app
    # This runs *before* Flask's reloader forks, ensuring only one CLI process
    initialize_persistent_cli()
    # Run Flask app (debug=True enables the reloader)
    # Use debug=False or use_reloader=False in production or if reloader causes issues
    app.run(host="0.0.0.0", port=5055, debug=True)
