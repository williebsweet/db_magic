# db_magic

Jupyter magic commands for Databricks SQL queries with OAuth authentication.

## Installation

```bash
# Install with uv
uv pip install /path/to/db_magic

# Or install in editable mode for development
cd /path/to/db_magic
uv pip install -e .
```

## Configuration

Set environment variables for your Databricks workspace:

```bash
export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'
export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/your-warehouse-id'
```

Or create `~/.databricks/config.json`:

```json
{
    "server_hostname": "https://your-workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/your-warehouse-id"
}
```

## Usage

### Load the extension

```python
%load_ext db_magic
```

### Cell magic: `%%sql`

Execute multi-line SQL queries:

```python
%%sql results
SELECT * FROM my_table LIMIT 100
```

Results are stored in the variable `results` (or `_df` by default).

### Line magic: `%sql_line`

Execute single-line queries:

```python
%sql_line SELECT count(*) FROM users

# Store in a specific variable
%sql_line count = SELECT count(*) FROM users
```

### Variable substitution

Use `{variable}` syntax to inject Python variables into queries:

```python
user_id = 123
table = 'users'

%%sql
SELECT * FROM {table} WHERE id = {user_id}
```

Strings are automatically quoted; numbers are inserted as-is.

### Suppress auto-display

For large queries where you don't want automatic display:

```python
%%sql --no-display big_df
SELECT * FROM huge_table
```

Or use the short form:

```python
%%sql -q big_df
SELECT * FROM huge_table
```

### Check configuration

```python
# Show current config
%databricks_config

# Test authentication
%databricks_config --show-auth
```

## Features

- OAuth authentication via browser
- Query timing display
- Auto-display of first 10 rows
- Variable substitution in queries
- Both cell (`%%sql`) and line (`%sql_line`) magics
- `--no-display` flag for large queries
- Error display with HTML formatting
