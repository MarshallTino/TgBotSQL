# Telegram Bot MCP Server API Documentation

This document describes the API endpoints provided by the Flask server (`scripts/mcp_server.py`) for managing and interacting with the Telegram bot. The server typically runs on `http://localhost:5055`.

## Bot Management

### Start Bot
*   **Method:** `POST`
*   **Path:** `/start`
*   **Description:** Starts the Telegram bot process using the `start_bot.sh` script.
*   **Request Body:** None
*   **Response:**
    ```json
    {
      "success": true,
      "message": "Bot started."
    }
    ```
    or
    ```json
    {
      "success": false,
      "message": "Bot already running."
    }
    ```

### Stop Bot
*   **Method:** `POST`
*   **Path:** `/stop`
*   **Description:** Stops the running Telegram bot process.
*   **Request Body:** None
*   **Response:**
    ```json
    {
      "success": true,
      "message": "Bot stopped."
    }
    ```
    or
    ```json
    {
      "success": false,
      "message": "Bot not running."
    }
    ```

### Get Bot Status
*   **Method:** `GET`
*   **Path:** `/status`
*   **Description:** Checks if the Telegram bot process is currently running.
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "running": true,
      "message": "Bot is running."
    }
    ```
    or
    ```json
    {
      "running": false,
      "message": "Bot is not running."
    }
    ```

## Bot Interaction

### Execute CLI Command
*   **Method:** `POST`
*   **Path:** `/cli`
*   **Description:** Executes a command within the context of the `start_bot.sh` script's interactive prompt.
*   **Request Body:**
    ```json
    {
      "command": "<command_string>"
    }
    ```
*   **Response:**
    ```json
    {
      "output": "<output_from_command>"
    }
    ```

### List Available CLI Commands
*   **Method:** `GET`
*   **Path:** `/cli/commands`
*   **Description:** Retrieves the list of commands available in the `start_bot.sh` script.
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "commands": ["command1", "command2", ...]
    }
    ```

## Logging

### Read Log File
*   **Method:** `GET`
*   **Path:** `/log`
*   **Description:** Reads the last N lines from the most recent Telegram bot log file (`logs/telegram_bot_*.log` or `logs/telegram_bot.log`).
*   **Request Parameters:**
    *   `lines` (integer, optional, default: 50): Number of lines to retrieve.
*   **Response:**
    ```json
    {
      "log": "<last_n_lines_of_log_content>"
    }
    ```

### Stream Log File
*   **Method:** `GET`
*   **Path:** `/log/stream`
*   **Description:** Provides a real-time stream of the latest log file content. Useful for live monitoring.
*   **Response:** A stream of text lines (`text/plain`).

## Configuration & Status

### Get Forwarding Status
*   **Method:** `GET`
*   **Path:** `/forwarding_status`
*   **Description:** Reads the current forwarding status from `config/forwarding_status.txt`.
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "forwarding_status": "<status_string>"
    }
    ```

## Docker Management

### Restart Docker Service
*   **Method:** `POST`
*   **Path:** `/docker/restart/<service>`
*   **Description:** Restarts a specific service within the Docker Compose setup (e.g., `postgres`, `mongo`).
*   **Path Parameters:**
    *   `service` (string): The name of the Docker Compose service to restart.
*   **Request Body:** None
*   **Response:**
    ```json
    {
      "output": "<output_from_docker_compose_restart>"
    }
    ```

### Get Docker Status
*   **Method:** `GET`
*   **Path:** `/docker/status`
*   **Description:** Gets the status of all services managed by Docker Compose (`docker compose ps`).
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "output": "<output_from_docker_compose_ps>"
    }
    ```

## Database Checks

### Check PostgreSQL Status
*   **Method:** `GET`
*   **Path:** `/db/postgres`
*   **Description:** Checks the readiness of the PostgreSQL database container using `pg_isready`.
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "output": "<output_from_pg_isready>"
    }
    ```

### Check MongoDB Status
*   **Method:** `GET`
*   **Path:** `/db/mongo`
*   **Description:** Checks the status of the MongoDB container using `mongo --eval "db.stats()"`.
*   **Request Parameters:** None
*   **Response:**
    ```json
    {
      "output": "<output_from_mongo_command>"
    }