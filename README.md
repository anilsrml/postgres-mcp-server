# PostgreSQL DBQA with MCP (Model Context Protocol)

This project acts as a secure bridge connecting your **PostgreSQL** database to **Artificial Intelligence (AI)** models (like Claude, Cursor, etc.).

By leveraging the **Model Context Protocol (MCP)**, it enables AI models to understand your database schema, securely query it (SELECT), and perform controlled data modifications (INSERT, UPDATE, DELETE).

##  Features

*   **Schema Analysis:** Automatically introduces database tables, columns, and relationships to the AI.
*   **Natural Language Querying:** AI translates natural language questions into SQL and retrieves results.
*   **Secure Write Operations:** 
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

##  Installation (Docker)

1.  Docker image oluşturun:
    ```bash
    docker build -t postgres-mcp-server .
    ```

2.  Bir `.env` dosyası oluşturun (`.env.example`'dan kopyalayabilirsiniz):
    ```bash
    copy .env.example .env   # Windows
    cp .env.example .env     # Linux/Mac
    ```

3.  `.env` dosyasında veritabanı URI'nizi ayarlayın:
    ```env
    # Bağlantı URI - tek satırda tüm bilgiler
    DATABASE_URI=postgresql://username:password@host:port/dbname

    # Yazma İşlemleri
    WRITE_ENABLED=true
    WRITABLE_TABLES=customers,orders,products  # Boş = tüm tablolar
    MAX_WRITE_ROWS=100
    ```

##  Usage

###  AI Client Configuration (Claude Desktop / Cursor)

Add the following to your AI client config (e.g., `claude_desktop_config.json` or Cursor MCP settings):

```json
{
  "mcpServers": {
    "postgres-dbq": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--network", "host",
        "-e", "DATABASE_URI",
        "-e", "WRITE_ENABLED",
        "-e", "WRITABLE_TABLES",
        "-e", "MAX_WRITE_ROWS",
        "postgres-mcp-server"
      ],
      "env": {
        "DATABASE_URI": "postgresql://postgres:your_password@localhost:5432/your_database",
        "WRITE_ENABLED": "true",
        "WRITABLE_TABLES": "",
        "MAX_WRITE_ROWS": "100"
      }
    }
  }
}
```

> **Note:** `--network host` allows the container to access `localhost` services (e.g., PostgreSQL running on the host). On Windows/Mac Docker Desktop, you may also use `host.docker.internal` in the URI instead of `localhost`.

###  Local Development (without Docker)

```bash
pip install -r requirements.txt
python mcp_server.py
```

##  Project Structure

*   `mcp_server.py`: Main MCP server file. Tool definitions are located here.
*   `src/database/`: 
    *   `executor.py`: SQL execution engine (`preview_write` and `execute_write` methods).
    *   `schema_manager.py`: Module analyzing the database schema.
*   `src/validation/`: 
    *   `sql_validator.py`: SQL security checks and validation logic.
    *   `rules.py`: Forbidden keywords and limit definitions.
*   `src/config.py`: Pydantic-based configuration management.
*   `Dockerfile`: Docker image definition.

##  Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URI` | ✅ | — | PostgreSQL connection URI (`postgresql://user:pass@host:port/db`) |
| `WRITE_ENABLED` | ❌ | `false` | Enable write operations |
| `WRITABLE_TABLES` | ❌ | `""` (all) | Comma-separated list of writable tables |
| `MAX_WRITE_ROWS` | ❌ | `100` | Max rows affected per write query |
| `MAX_QUERY_TIMEOUT` | ❌ | `30` | Query timeout in seconds |
| `MAX_RESULT_ROWS` | ❌ | `1000` | Max rows returned per SELECT |

##  Security Warning

This tool possesses powerful capabilities. When `WRITE_ENABLED=true` is set, your AI model can make changes to the database.
*   **Configure `WRITABLE_TABLES`** strictly before using in a Production environment.
*   Ensure you have regular backups of your critical data.
