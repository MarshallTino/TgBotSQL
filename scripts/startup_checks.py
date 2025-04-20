#!/usr/bin/env python3
import subprocess
import platform
import time
import os
import sys
import logging

# Add project root to Python path if needed
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('startup_checks')

def run_command(command, capture_output=True, text=True, check=False, shell=False, timeout=30):
    """Run a shell command and return the result."""
    logger.debug(f"Running command: {command}")
    try:
        result = subprocess.run(command, 
                             capture_output=capture_output, 
                             text=text, 
                             check=check, 
                             shell=shell,
                             timeout=timeout)
        if capture_output:
            logger.debug(f"Command result: {result.returncode}")
            logger.debug(f"Stdout: {result.stdout[:200] if result.stdout else ''}")
            if result.stderr:
                logger.debug(f"Stderr: {result.stderr[:200]}")
        return result
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out: {command}")
        return None
    except FileNotFoundError:
        logger.error(f"Command not found: {command[0]}")
        return None
    except Exception as e:
        logger.error(f"Error executing command {command}: {str(e)}")
        return None

def is_wsl():
    """Check if running in WSL."""
    if platform.system() == "Linux":
        with open('/proc/version', 'r') as f:
            if 'microsoft' in f.read().lower():
                return True
    return False

def is_docker_running():
    """Check if Docker daemon is running and responsive."""
    logger.info("Checking if Docker is running...")
    result = run_command(['docker', 'info'])
    return result is not None and result.returncode == 0

def ensure_wsl_running():
    """Ensure WSL is running (only relevant when script is run from Windows)."""
    if not is_wsl():  # If we're not in WSL, this function doesn't apply
        logger.info("Not running in WSL environment, skipping WSL check")
        return True
    
    logger.info("Running in WSL, checking if WSL integration is properly set up")
    # If we're inside WSL and can run this script, WSL is technically "running"
    # The question is whether Docker Desktop's WSL integration is enabled
    
    # Check if docker command exists
    docker_check = run_command(['which', 'docker'])
    if docker_check and docker_check.returncode == 0:
        logger.info("Docker command is available in WSL")
    else:
        logger.warning("Docker command not found in WSL. Docker Desktop WSL integration may not be enabled.")
        logger.info("Attempting to notify user to check Docker Desktop settings...")
        # We're already in WSL, so we can't directly start Docker Desktop
        # Just return a helpful error
        print("\n⚠️  Docker command not found in WSL.")
        print("Please ensure Docker Desktop is running on Windows with WSL integration enabled.")
        print("Go to Docker Desktop > Settings > Resources > WSL Integration > Enable integration with this distro")
        print("Then restart Docker Desktop and try again.\n")
        return False
    
    return True

def start_docker_desktop():
    """Attempt to start Docker Desktop if possible."""
    logger.info("Attempting to start Docker Desktop...")
    
    if is_wsl():
        # We're in WSL, need to start Docker Desktop from Windows
        try:
            # Try to start Docker Desktop via PowerShell
            logger.info("Starting Docker Desktop from WSL via PowerShell...")
            cmd = [
                'powershell.exe', 
                '-Command', 
                'Start-Process "C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe"'
            ]
            run_command(cmd, capture_output=False)
            print("🔄 Starting Docker Desktop... Please wait")
            
            # Give Docker Desktop time to start
            attempts = 0
            max_attempts = 15  # Increased wait time (15 * 3 = 45 seconds)
            while attempts < max_attempts:
                attempts += 1
                print(f"Waiting for Docker to start... ({attempts}/{max_attempts})")
                time.sleep(3)  # Wait 3 seconds between checks
                
                # Check if Docker daemon is responsive
                if is_docker_running():
                    logger.info("Docker daemon is responsive. Checking compose...")
                    # Now check if docker compose is available
                    compose_check = run_command(['docker', 'compose', 'version'])
                    if compose_check and compose_check.returncode == 0:
                        logger.info("Docker Desktop and Compose successfully started")
                        return True
                    else:
                        logger.warning("Docker daemon running, but compose check failed. Still waiting...")
            
            logger.error("Docker Desktop failed to start or compose did not become responsive")
            return False
        except Exception as e:
            logger.error(f"Error starting Docker Desktop: {str(e)}")
            return False
    else:
        # We're on a regular Linux system
        logger.info("On Linux, attempting to start Docker service...")
        result = run_command(['sudo', 'systemctl', 'start', 'docker'])
        if result and result.returncode == 0:
            logger.info("Docker service started")
            time.sleep(5)  # Give Docker a moment to fully initialize
            return is_docker_running()
        else:
            logger.error("Failed to start Docker service")
            return False

def check_docker_compose_services():
    """Check if required docker-compose services are running."""
    if not is_docker_running():
        logger.error("Docker is not running, cannot check services")
        return False
    
    logger.info("Checking Docker Compose services...")
    
    # Attempt to get running services
    result = run_command(['docker', 'compose', 'ps', '--services'])
    if not result or result.returncode != 0:
        logger.error("Failed to get Docker Compose services")
        return False
    
    return True

def run_startup_checks():
    """Run all startup checks and ensure services are running."""
    logger.info("Starting environment checks...")
    
    # Step 1: Ensure WSL is properly configured (if applicable)
    if not ensure_wsl_running():
        logger.error("WSL is not properly configured for Docker")
        return False
    
    # Step 2: Check if Docker is running, start it if not
    if not is_docker_running():
        logger.warning("Docker is not running, attempting to start it")
        if not start_docker_desktop():
            logger.error("Failed to start Docker")
            return False
    else:
        logger.info("Docker is already running")
    
    # Step 3: Check if required services are running
    if not check_docker_compose_services():
        logger.error("Docker Compose services check failed")
        return False
    
    logger.info("All startup checks passed")
    return True

if __name__ == "__main__":
    success = run_startup_checks()
    if success:
        print("\n✅ Environment checks passed. Docker is running and available.")
        sys.exit(0)
    else:
        print("\n❌ Environment checks failed. Please fix the issues above.")
        sys.exit(1)
