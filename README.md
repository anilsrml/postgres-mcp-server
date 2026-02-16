# PostgreSQL DBQA with MCP (Model Context Protocol)

This project acts as a secure bridge connecting your **PostgreSQL** database to **Artificial Intelligence (AI)** models (like Claude, Cursor, etc.).

By leveraging the **Model Context Protocol (MCP)**, it enables AI models to understand your database schema, securely query it (SELECT), and perform controlled data modifications (INSERT, UPDATE, DELETE).

##  Features

*   **Schema Analysis:** Automatically introduces database tables, columns, and relationships to the AI.
*   **Natural Language Querying:** AI translates natural language questions into SQL and retrieves results.
*   **Secure Write Operations (NEW):** 
    *   Supports `INSERT`, `UPDATE`, `DELETE` operations.
    *   **Two-Phase Approval:** AI first previews the affected rows using `modify_data`, then confirms the modification with `confirm_modification`.
    *   **Kill Switch:** Write permissions can be instantly disabled with `WRITE_ENABLED=false`.

##  7-Layer Security Model

This server employs a multi-layered protection system to ensure data safety:

1.  **DDL Blocking:** Structural modification commands like `DROP`, `TRUNCATE`, `ALTER`, `CREATE` are **always strictly forbidden**.
2.  **WHERE Clause Enforcement:** `UPDATE` and `DELETE` queries are prevented from running without a `WHERE` clause.
3.  **Row Limit:** The maximum number of rows affected by a single query is limited (Default: 100).
4.  **Table Whitelist:** Write permission is granted only to tables specified in the `.env` file.
5.  **Two-Phase Transaction:** Write operations first run in "Dry-Run" (preview) mode to calculate the number of affected rows.
6.  **Separate Isolation:** Distinct connection managers and validators are used for read and write operations.
7.  **Audit Log:** All operations are logged in detail.

##  Installation

1.  Install the necessary dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Create a `.env` file and enter your database credentials:
    ```env
    # Database Connection
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=your_database_name
    DB_USER=your_username
    DB_PASSWORD=your_password

    # Write Operations Settings
    WRITE_ENABLED=true
    WRITABLE_TABLES=customers,orders,products  # Only these tables are writable (empty = all)
    MAX_WRITE_ROWS=100                         # Max 100 rows can be changed at once
    ```

##  Usage

To start the MCP server:

```bash
python mcp_server.py
```

###  AI Client Configuration (Claude Desktop / Cursor)

Add the following information to your AI assistant's configuration file (e.g., `claude_desktop_config.json` or Cursor settings). It is recommended to specify `.env` variables within the `env` block:

```json
{
  "mcpServers": {
    "postgres-dbq": {
      "command": "python",
      "args": ["*/mcp_server.py"],
      "env": {
        "DB_HOST": "",
        "DB_PORT": "",
        "DB_NAME": "",
        "DB_USER": "",
        "DB_PASSWORD": "",
        "WRITE_ENABLED": "",
        "WRITABLE_TABLES": "",
        "MAX_WRITE_ROWS": ""
      }
    }
  }
}
```

> **Note:** On Windows, be careful to use `\\` or `/` in file paths. You may need to specify the full Python environment path (e.g., `c:/Users/.../venv/Scripts/python.exe`).

##  Project Structure

*   `mcp_server.py`: Main MCP server file. Tool definitions are located here.
*   `src/database/`: 
    *   `executor.py`: SQL execution engine (`preview_write` and `execute_write` methods).
    *   `schema_manager.py`: Module analyzing the database schema.
    *   `src/validation/`: 
    *   `sql_validator.py`: SQL security checks and validation logic.
    *   `rules.py`: Forbidden keywords and limit definitions.
    *   `src/config.py`: Pydantic-based configuration management.

## ⚠️ Security Warning

This tool possesses powerful capabilities. When `WRITE_ENABLED=true` is set, your AI model can make changes to the database.
*   **Configure `WRITABLE_TABLES`** strictly before using in a Live (Production) environment.
*   Ensure you have regular backups of your critical data.
