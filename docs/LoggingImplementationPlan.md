# Logging Implementation Plan

1.  **Goal:**
    *   Improve observability and debugging capabilities by providing clear, structured logs.
    *   Specifically target easier debugging of data exchanged with external interfaces (LLM, MPC Server, NiFi Server).
    *   Enhance traceability of operations from the initial user request through subsequent system actions decided by the LLM.

2.  **Core Components:**
    *   **Library:** `Loguru` will be used for its simplicity in configuration, colorization, file logging, and context management.
    *   **Configuration:**
        *   A `logging_config.yaml` file will define logging levels, file paths, formats, and the toggle for interface debug logging.
        *   A shared configuration module (e.g., `config/settings.py`, moved from the previous `nifi_chat_ui/config.py`) will load both `.env` variables and `logging_config.yaml` (using `PyYAML`), making settings centrally accessible.
    *   **Log Destinations & Levels:**
        *   **Console:** `INFO` level and above. Colored output for readability (timestamps, levels, context IDs).
        *   **`logs/client.log`:** `DEBUG` level and above. Standard text format. Overwritten on application start.
        *   **`logs/server.log`:** `DEBUG` level and above. Standard text format. Overwritten on application start.
        *   **Interface Debug Logs (Conditional):**
            *   Activated by a setting in `logging_config.yaml` (e.g., `interface_debug_enabled: true`).
            *   `logs/llm_debug.log`: Logs LLM request/response data as pretty-printed JSON. `DEBUG` level. Overwritten on application start.
            *   `logs/mpc_debug.log`: Logs MPC Server request/response data as pretty-printed JSON. `DEBUG` level. Overwritten on application start.
            *   `logs/nifi_debug.log`: Logs NiFi Server request/response data as pretty-printed JSON. `DEBUG` level. Overwritten on application start.
    *   **Contextual Tracking:**
        *   Introduce two context identifiers:
            *   `user_request_id`: A unique ID generated for each distinct user interaction/request originating from the chat interface. Persists across all actions taken for that request.
            *   `action_id`: A unique ID generated **only** when the LLM decides on a specific action (e.g., tool use, API call). This ID propagates down through the execution of that specific action.
        *   These IDs should be included in all relevant log messages (console and files) to allow tracing. `Loguru`'s `bind()` or context features can facilitate this.
    *   **Log Rotation:** Deferred for now. Log files will be overwritten each time the application starts.

3.  **Implementation Steps:**
    *   Add `PyYAML` and `loguru` as dependencies.
    *   Create a `config/` directory at the project root.
    *   Move `nifi_chat_ui/config.py` to `config/settings.py` and update its import paths if necessary.
    *   Modify `config/settings.py` to load `logging_config.yaml` and expose the logging configuration alongside other settings.
    *   Create a default `logging_config.yaml` in the project root.
    *   Implement a central logging setup function (e.g., `setup_logging()` in a shared location, perhaps `config/logging_setup.py`) that configures `Loguru` based on the loaded settings (console sink, file sinks, conditional interface debug sinks, formats). This function should be called early in the application startup (both client and server if applicable).
    *   Integrate context ID generation and propagation. Generate `user_request_id` upon receiving a user message. Generate `action_id` when processing an LLM response that dictates an action. Pass these IDs down through function calls or use `Loguru`'s context features.
    *   Refactor existing logging calls (`print`, standard `logging`) to use `Loguru`.
    *   Implement specific logging calls around LLM, MPC, and NiFi interactions, sending the request/response data to the appropriate debug logger (if enabled) using a specific format or method that triggers the JSON serialization. Ensure context IDs are bound.

4.  **Example `logging_config.yaml`:**

    ```yaml
    # Basic Logging Configuration
    log_directory: "logs"
    interface_debug_enabled: false # Master toggle for interface JSON logs

    console:
      level: "INFO"
      format: "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <yellow>Req:{extra[user_request_id]}</yellow> | <blue>Act:{extra[action_id]}</blue> - <level>{message}</level>"

    client_file:
      enabled: true
      path: "{log_directory}/client.log"
      level: "DEBUG"
      rotation: null # Overwrite on start
      format: "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | Req:{extra[user_request_id]} | Act:{extra[action_id]} - {message}"

    server_file:
      enabled: true
      path: "{log_directory}/server.log"
      level: "DEBUG"
      rotation: null # Overwrite on start
      format: "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | Req:{extra[user_request_id]} | Act:{extra[action_id]} - {message}"

    # Interface Debug Logs (only active if interface_debug_enabled is true)
    llm_debug_file:
      path: "{log_directory}/llm_debug.log"
      level: "DEBUG"
      rotation: null # Overwrite on start
      # Special handler/formatter needed for JSON pretty printing

    mpc_debug_file:
      path: "{log_directory}/mpc_debug.log"
      level: "DEBUG"
      rotation: null # Overwrite on start
      # Special handler/formatter needed for JSON pretty printing

    nifi_debug_file:
      path: "{log_directory}/nifi_debug.log"
      level: "DEBUG"
      rotation: null # Overwrite on start
      # Special handler/formatter needed for JSON pretty printing
    ```

5.  **Future Considerations:**
    *   Implement log rotation when needed.
    *   Explore more advanced context propagation mechanisms if required.
    *   Consider asynchronous logging if performance becomes a concern.
