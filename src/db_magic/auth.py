"""
Databricks OAuth authentication and SQL connection management.
"""

import os
from typing import Optional

try:
    from databricks.sdk import WorkspaceClient
    from databricks import sql
    import pandas as pd
except ImportError as e:
    raise ImportError(
        f"Required dependencies not installed: {e}. "
        "Run: pip install databricks-sdk databricks-sql-connector pandas"
    )


class DatabricksAuth:
    """Handles Databricks OAuth authentication and SQL connection management."""

    def __init__(self, server_hostname: Optional[str] = None, http_path: Optional[str] = None):
        """
        Initialize Databricks authentication.

        Args:
            server_hostname: Databricks workspace hostname (optional, can be set via env var)
            http_path: SQL warehouse HTTP path (optional, can be set via env var)
        """
        self.server_hostname = server_hostname or os.getenv('DATABRICKS_HOST')
        self.http_path = http_path or os.getenv('DATABRICKS_HTTP_PATH')
        self._workspace_client = None
        self._sql_connection = None

    def _get_workspace_client(self) -> WorkspaceClient:
        """Get or create Databricks WorkspaceClient with OAuth authentication."""
        if not self._workspace_client:
            if not self.server_hostname:
                raise ValueError(
                    "Databricks hostname not provided. Set DATABRICKS_HOST environment variable "
                    "or pass server_hostname parameter."
                )

            # Try external browser OAuth authentication
            try:
                self._workspace_client = WorkspaceClient(
                    host=self.server_hostname,
                    auth_type='external-browser'
                )
            except Exception as e:
                print(f"External browser auth failed: {e}")
                # Fallback to default auth (might prompt for other methods)
                self._workspace_client = WorkspaceClient(host=self.server_hostname)

        return self._workspace_client

    def authenticate(self) -> WorkspaceClient:
        """
        Authenticate with Databricks OAuth.

        Returns:
            Databricks WorkspaceClient

        Raises:
            ValueError: If server_hostname is not provided
            Exception: If authentication fails
        """
        return self._get_workspace_client()

    def test_connection(self) -> bool:
        """Test if the Databricks connection is working."""
        try:
            client = self._get_workspace_client()
            # Test connection by getting current user info
            user = client.current_user.me()
            print(f"Authenticated as: {user.user_name}")
            return True
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False

    def connect(self):
        """
        Create and return a Databricks SQL connection using OAuth.

        Returns:
            Databricks SQL connection
        """
        if self._sql_connection:
            try:
                # Test if connection is still valid
                cursor = self._sql_connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchall()
                cursor.close()
                return self._sql_connection
            except:
                # Connection is stale, create new one
                self._sql_connection = None

        if not self.http_path:
            raise ValueError(
                "Databricks HTTP path not provided. Set DATABRICKS_HTTP_PATH environment variable "
                "or pass http_path parameter."
            )

        # Get WorkspaceClient to handle OAuth authentication
        self._get_workspace_client()

        # Use external browser auth for SQL connection to match workspace auth
        self._sql_connection = sql.connect(
            server_hostname=self.server_hostname.replace('https://', ''),
            http_path=self.http_path,
            auth_type='external-browser'
        )

        return self._sql_connection

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as pandas DataFrame.

        Args:
            query: SQL query string

        Returns:
            Query results as pandas DataFrame
        """
        connection = self.connect()

        try:
            cursor = connection.cursor()
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch all results
            results = cursor.fetchall()

            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)

            cursor.close()
            return df

        except Exception as e:
            raise Exception(f"Query execution failed: {e}")

    def close(self) -> None:
        """Close the Databricks connections."""
        if self._sql_connection:
            self._sql_connection.close()
            self._sql_connection = None
        # WorkspaceClient doesn't need explicit closing
