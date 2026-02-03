"""
db_magic: Jupyter magic commands for Databricks SQL queries.

Usage:
    %load_ext db_magic

    %%sql results
    SELECT * FROM my_table LIMIT 100
"""

from .auth import DatabricksAuth
from .magic import DatabricksMagics, load_ipython_extension

__version__ = "0.1.0"
__all__ = ["DatabricksAuth", "DatabricksMagics", "load_ipython_extension"]
