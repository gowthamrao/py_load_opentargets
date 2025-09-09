import os
import logging
from typing import Dict, Any

import psycopg
import fsspec

from .data_acquisition import list_available_versions

logger = logging.getLogger(__name__)


class ValidationService:
    """A service to run validation checks on the configuration and connections."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def run_all_checks(self) -> Dict[str, Dict[str, Any]]:
        """
        Runs all validation checks and returns a dictionary of results.
        """
        results = {
            "Database Connection": self.check_db_connection(),
            "Data Source Connection": self.check_source_connection(),
            "Dataset Definitions": self.check_dataset_definitions(),
        }
        return results

    def check_db_connection(self) -> Dict[str, Any]:
        """
        Checks if a connection to the database can be established.
        """
        db_conn_str = os.getenv("DB_CONN_STR")
        if not db_conn_str:
            return {"success": False, "message": "DB_CONN_STR environment variable not set."}

        try:
            # Hide password from logs
            hidden_conn_str = str(psycopg.conninfo.make_conninfo(db_conn_str))
            logger.info(f"Attempting to connect to database: {hidden_conn_str}")
            with psycopg.connect(db_conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1;")
                    result = cursor.fetchone()
                    if result and result[0] == 1:
                        return {"success": True, "message": "Database connection successful."}
                    else:
                        return {"success": False, "message": "Connection established, but test query failed."}
        except Exception as e:
            error_msg = str(e).split('\n')[0] # Get the first line of the error
            logger.error(f"Database connection failed: {error_msg}")
            return {"success": False, "message": f"Failed to connect: {error_msg}"}

    def check_source_connection(self) -> Dict[str, Any]:
        """
        Checks if the remote data source is accessible and contains valid versions.
        """
        discovery_uri = self.config.get("source", {}).get("version_discovery_uri")
        if not discovery_uri:
            return {"success": False, "message": "Missing 'version_discovery_uri' in config."}

        try:
            logger.info(f"Attempting to discover versions at: {discovery_uri}")
            versions = list_available_versions(discovery_uri)
            if versions:
                return {"success": True, "message": f"Successfully found versions (latest: {versions[0]})."}
            else:
                return {"success": False, "message": f"Could not find any versions at '{discovery_uri}'."}
        except Exception as e:
            error_msg = str(e).split('\n')[0]
            logger.error(f"Source connection failed: {error_msg}")
            return {"success": False, "message": f"Failed to connect to source: {error_msg}"}

    def check_dataset_definitions(self) -> Dict[str, Any]:
        """
        Checks if all configured datasets have a primary key defined.
        """
        datasets = self.config.get("datasets", {})
        if not datasets:
            return {"success": False, "message": "'datasets' section is missing or empty in config."}

        missing_pk = []
        for name, conf in datasets.items():
            if not conf.get("primary_key"):
                missing_pk.append(name)

        if missing_pk:
            return {"success": False, "message": f"Datasets missing 'primary_key': {', '.join(missing_pk)}"}
        else:
            return {"success": True, "message": "All dataset definitions are valid."}
