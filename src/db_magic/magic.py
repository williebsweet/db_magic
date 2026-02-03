"""
Jupyter magic commands for Databricks SQL queries.
"""

import os
import re
import time
from pathlib import Path
from typing import Dict

import pandas as pd

try:
    from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
    from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
    from IPython.display import display, HTML
except ImportError:
    raise ImportError("IPython is required for magic commands. Run: pip install ipython")

from .auth import DatabricksAuth


@magics_class
class DatabricksMagics(Magics):
    """Jupyter magic commands for Databricks SQL queries."""

    def __init__(self, shell=None):
        super().__init__(shell)
        self._auth = None
        self._config = self._load_databricks_config()

    def _load_databricks_config(self) -> Dict[str, str]:
        """Load Databricks configuration from environment or config file."""
        config = {}

        # Try environment variables first (using standard Databricks env var names)
        config['server_hostname'] = os.getenv('DATABRICKS_HOST', os.getenv('DATABRICKS_SERVER_HOSTNAME', ''))
        config['http_path'] = os.getenv('DATABRICKS_HTTP_PATH', '')

        # Try config file if env vars not set
        if not all(config.values()):
            config_file = Path.home() / '.databricks' / 'config.json'
            if config_file.exists():
                try:
                    import json
                    with open(config_file) as f:
                        file_config = json.load(f)
                        config.update(file_config)
                except Exception as e:
                    print(f"Error loading config file: {e}")

        return config

    def _get_auth(self) -> DatabricksAuth:
        """Get or create DatabricksAuth instance."""
        if not self._auth:
            server_hostname = self._config.get('server_hostname')
            http_path = self._config.get('http_path')

            if not server_hostname or not http_path:
                raise ValueError(
                    "Databricks configuration not found. Set environment variables:\n"
                    "export DATABRICKS_HOST='https://your-workspace.cloud.databricks.com'\n"
                    "export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/your-warehouse-id'\n"
                    "Or create ~/.databricks/config.json with these values."
                )

            self._auth = DatabricksAuth(server_hostname, http_path)

        return self._auth

    def _substitute_variables(self, query: str) -> str:
        """
        Substitute {variable} placeholders in query with values from user namespace.

        Args:
            query: SQL query with optional {variable} placeholders

        Returns:
            Query with variables substituted
        """
        pattern = r'\{(\w+)\}'

        def replace(match):
            var_name = match.group(1)
            if var_name in self.shell.user_ns:
                val = self.shell.user_ns[var_name]
                # Quote strings, leave numbers as-is
                if isinstance(val, str):
                    # Escape single quotes in the string
                    escaped = val.replace("'", "''")
                    return f"'{escaped}'"
                return str(val)
            # If variable not found, leave placeholder unchanged
            return match.group(0)

        return re.sub(pattern, replace, query)

    @magic_arguments()
    @argument('--no-display', '-q', action='store_true',
              help='Suppress auto-display of results')
    @argument('var_name', nargs='?', default='_df',
              help='Variable name to store results (default: _df)')
    @cell_magic
    def sql(self, line: str, cell: str):
        """
        Execute SQL query against Databricks and assign results to variable.

        Usage:
            %%sql
            SELECT * FROM my_table LIMIT 10

            %%sql df_name
            SELECT * FROM my_table

            %%sql --no-display big_df
            SELECT * FROM huge_table

        Variable substitution:
            user_id = 123
            %%sql
            SELECT * FROM users WHERE id = {user_id}
        """
        args = parse_argstring(self.sql, line)
        var_name = args.var_name
        no_display = args.no_display

        # Substitute variables in query
        query = self._substitute_variables(cell.strip())

        try:
            # Get authentication and execute query with timing
            auth = self._get_auth()

            start = time.time()
            df = auth.execute_query(query)
            elapsed = time.time() - start

            # Assign to variable in user namespace
            self.shell.user_ns[var_name] = df

            # Print timing info
            print(f"Query completed in {elapsed:.2f}s ({len(df)} rows)")

            # Auto-display results unless suppressed
            if not no_display and len(df) > 0:
                display(df.head(10))
                if len(df) > 10:
                    print(f"... showing 10 of {len(df)} rows")

        except Exception as e:
            display(HTML(f'<span style="color:red"><b>Error:</b> {e}</span>'))
            # Still create empty DataFrame so variable exists
            self.shell.user_ns[var_name] = pd.DataFrame()

    @line_magic
    def sql_line(self, line: str):
        """
        Execute single-line SQL query.

        Usage:
            %sql_line SELECT count(*) FROM users

            %sql_line result = SELECT count(*) FROM users

        Variable substitution:
            table_name = 'users'
            %sql_line SELECT count(*) FROM {table_name}
        """
        # Parse: var = SELECT ... or just SELECT ...
        if '=' in line and not line.strip().startswith('SELECT'):
            parts = line.split('=', 1)
            var_name = parts[0].strip()
            query = parts[1].strip()
        else:
            var_name = '_df'
            query = line.strip()

        # Substitute variables
        query = self._substitute_variables(query)

        try:
            auth = self._get_auth()

            start = time.time()
            df = auth.execute_query(query)
            elapsed = time.time() - start

            self.shell.user_ns[var_name] = df

            print(f"Query completed in {elapsed:.2f}s ({len(df)} rows)")

            # Auto-display for line magic
            if len(df) > 0:
                display(df.head(10))
                if len(df) > 10:
                    print(f"... showing 10 of {len(df)} rows")

        except Exception as e:
            display(HTML(f'<span style="color:red"><b>Error:</b> {e}</span>'))
            self.shell.user_ns[var_name] = pd.DataFrame()

    @line_magic
    def databricks_config(self, line: str):
        """
        Display or set Databricks configuration.

        Usage:
            %databricks_config                    # Show current config
            %databricks_config --show-auth        # Show authentication status
        """
        if '--show-auth' in line:
            try:
                auth = self._get_auth()
                auth.test_connection()

                # Test SQL connection
                connection = auth.connect()
                print("Databricks SQL connection successful")
                connection.close()

            except Exception as e:
                print(f"Authentication/connection failed: {e}")
        else:
            print("Current Databricks configuration:")
            for key, value in self._config.items():
                # Mask sensitive values
                display_value = value if key not in ['access_token'] else '***'
                print(f"  {key}: {display_value}")


def load_ipython_extension(ipython):
    """Load the db_magic extension."""
    ipython.register_magics(DatabricksMagics)
    print("db_magic loaded. Use %%sql to query Databricks.")
