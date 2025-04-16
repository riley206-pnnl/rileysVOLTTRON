#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TNSHistorian2

A VOLTTRON historian that properly leverages BaseHistorian features
to store TNS data records. The historian subscribes to TNS topics and
stores the complete record data in the value field of the historian.
"""

__version__ = "3.0.0"

import logging
import json
import os
import sqlite3
from volttron.platform.agent import utils
from volttron.platform.vip.agent import RPC, compat
from volttron.platform.agent.base_historian import BaseHistorian
from volttron.platform.messaging import topics, headers as headers_mod
from tent.data_manager.local_data_manager import LocalDataManager

_log = logging.getLogger(__name__)
utils.setup_logging()


def historian(config_path, **kwargs):
    """
    Parse the config file and initialize the historian with it.
    """
    config = utils.load_config(config_path) or {
        "connection": {
            "type": "sqlite",
            "params": {"url": "sqlite:////home/volttron/historian_data.sqlite"}
        },
        "topic_replace_list": [],
        "custom_topics": {"TNS": ["TNS/"]}  # Ensure we capture TNS topics
    }

    connection = config.get("connection")
    if "connection" in config:
        del config["connection"]

    utils.update_kwargs_with_config(kwargs, config)
    return TNSHistorian2(connection=connection, **kwargs)


class TNSHistorian2(BaseHistorian):
    """
    Historian implementation that extends the BaseHistorian to handle TNS records.
    It subscribes to TNS topics and stores the complete record data in the value field.
    """

    def __init__(self, connection, **kwargs):
        _log.info("TNSHistorian2 initialization started")

        # Save connection info before calling parent constructor
        self.connection = connection

        # Ensure SQLite connection parameters include a URL if using SQLite
        if self.connection.get("type", "").lower() == "sqlite":
            db_params = self.connection.get("params", {})

            # If using SQLite, make sure the path is accessible
            if "url" in db_params and db_params["url"].startswith("sqlite:///"):
                # Extract the file path from the SQLite URL
                db_path = db_params["url"].replace("sqlite:///", "")

                # If it's an absolute path, make sure the directory exists
                if db_path.startswith("/"):
                    db_dir = os.path.dirname(db_path)
                    if not os.path.exists(db_dir):
                        try:
                            _log.info(f"Creating directory for database: {db_dir}")
                            os.makedirs(db_dir, exist_ok=True)
                        except Exception as e:
                            _log.warning(f"Cannot create directory for database: {e}")
                            # Fallback to using a database in the agent's data directory
                            agent_data_dir = self._get_agent_data_dir()
                            new_db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                            _log.info(f"Using fallback database path: {new_db_path}")
                            db_params["url"] = f"sqlite:///{new_db_path}"

            # If no URL is provided, use one in the agent's data directory
            if "url" not in db_params:
                agent_data_dir = self._get_agent_data_dir()
                db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                db_params["url"] = f"sqlite:///{db_path}"

            self.connection["params"] = db_params
            _log.info(f"Using SQLite connection parameters: {db_params}")

        # Initialize BaseHistorian - this will handle subscriptions and caching
        super(TNSHistorian2, self).__init__(**kwargs)

        # Initialize the TENTS data manager after BaseHistorian is set up
        try:
            self.data_manager = LocalDataManager(
                transactive_node=None,  # We don't need a transactive node for direct calls
                db_params=self.connection["params"]
            )
            _log.info("Successfully created TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to create TENTS data manager: {e}")
            self.data_manager = None

    def _get_agent_data_dir(self):
        """
        Get the agent's data directory, creating it if necessary.

        :return: Path to the agent's data directory
        """
        # Check if we're in an agent-data directory structure
        current_dir = os.getcwd()
        agent_name = os.path.basename(current_dir)
        agent_data_dir = os.path.join(current_dir, f"{agent_name}.agent-data")

        if not os.path.exists(agent_data_dir):
            try:
                os.makedirs(agent_data_dir, exist_ok=True)
            except Exception as e:
                _log.warning(f"Cannot create agent data directory: {e}")
                # Fall back to the current directory
                agent_data_dir = current_dir

        return agent_data_dir

    def historian_setup(self):
        """Called at startup and after configuration changes."""
        _log.info("Setting up TNSHistorian2")

        # Initialize the data manager if needed
        if self.data_manager is None:
            try:
                self.data_manager = LocalDataManager(
                    transactive_node=None,
                    db_params=self.connection["params"]
                )
            except Exception as e:
                _log.error(f"Failed to initialize TENTS data manager: {e}")
                return

        # Initialize the data manager
        try:
            self.data_manager.init_archive()
            _log.info("Successfully initialized TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to initialize TENTS data manager: {e}")

    def _capture_record_data(self, peer, sender, bus, topic, headers, message):
        """
        Override the _capture_record_data method to properly handle TNS topics.
        For TNS topics, we store the entire message as the value.
        """
        # Anon the topic if necessary
        topic = self.get_renamed_topic(topic)

        timestamp_string = headers.get(headers_mod.DATE, None)
        timestamp = utils.get_aware_utc_now()

        if timestamp_string is not None:
            timestamp, my_tz = utils.process_timestamp(timestamp_string, topic)
            headers['time_error'] = self.does_time_exceed_tolerance(topic, timestamp)

        if sender == 'pubsub.compat':
            message = compat.unpack_legacy_message(headers, message)

        if self.gather_timing_data:
            utils.add_timing_data_to_header(headers, self.core.agent_uuid or self.core.identity, "collected")

        # For TNS topics, we want to preserve the entire message structure
        if topic.startswith('TNS/'):
            _log.debug(f"Processing TNS topic: {topic} with message: {message}")
            # Put the entire message in the 'value' field to be stored by the base historian
            self._event_queue.put({
                'source': 'record',
                'topic': topic,
                'readings': [(timestamp, message)],  # Store entire message as value
                'meta': {},
                'headers': headers
            })
        else:
            # For non-TNS topics, use the default behavior
            self._event_queue.put({
                'source': 'record',
                'topic': topic,
                'readings': [(timestamp, message)],
                'meta': {},
                'headers': headers
            })

    def publish_to_historian(self, to_publish_list):
        """
        Main publish method for the TNSHistorian2.
        For TNS records, we don't report them as handled so they stay in the cache.

        :param to_publish_list: List of records to publish
        """
        _log.debug(f"Processing {len(to_publish_list)} records")

        if not to_publish_list:
            return

        try:
            # Process each record
            for record in to_publish_list:
                _log.debug(f"Publishing record: {record}")
                # We just log the records but we DON'T call report_all_handled()
                # This ensures they stay in the cache until transferred via RPC

            # Comment out or remove this line:
            # self.report_all_handled()

        except Exception as e:
            _log.error(f"Error in publish_to_historian: {e}", exc_info=True)

    @RPC.export
    def inspect_cache_database(self):
        """
        RPC method to inspect the cache database and return information about stored topics and records.

        :return: Dictionary with information about topics and records in the cache
        """
        try:
            # Get path to the cache database
            cache_db_path = self._get_cache_db_path()
            _log.info(f"Inspecting cache database at: {cache_db_path}")

            # Connect to the SQLite cache database
            conn = sqlite3.connect(cache_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Query topics table
            cursor.execute("SELECT topic_id, topic_name FROM topics")
            topics = [dict(row) for row in cursor.fetchall()]

            # Count records for each topic
            topic_counts = {}
            tns_records_sample = {}

            for topic in topics:
                topic_id = topic['topic_id']
                topic_name = topic['topic_name']

                cursor.execute("SELECT COUNT(*) FROM outstanding WHERE topic_id = ?", (topic_id,))
                count = cursor.fetchone()[0]
                topic_counts[topic_name] = count

                # For TNS topics, get a sample record
                if topic_name.startswith('TNS/'):
                    cursor.execute(
                        "SELECT id, ts, value_string FROM outstanding WHERE topic_id = ? LIMIT 1",
                        (topic_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        try:
                            # Try to parse the value string
                            value_data = json.loads(row['value_string'])

                            # Extract just the essential parts for the sample
                            if isinstance(value_data, dict):
                                tns_records_sample[topic_name] = {
                                    "id": row['id'],
                                    "timestamp": row['ts'],
                                    "table_name": value_data.get('table_name', 'unknown'),
                                    "data_keys": list(value_data.get('data', {}).keys()) if 'data' in value_data else []
                                }
                        except Exception as e:
                            tns_records_sample[topic_name] = f"Error parsing: {e}"

            # Get total record count
            cursor.execute("SELECT COUNT(*) FROM outstanding")
            total_records = cursor.fetchone()[0]

            # Get schema information
            cursor.execute("pragma table_info(outstanding)")
            outstanding_schema = [dict(row) for row in cursor.fetchall()]

            conn.close()

            return {
                "status": "success",
                "total_records": total_records,
                "topics": topics,
                "topic_counts": topic_counts,
                "tns_records_sample": tns_records_sample,
                "outstanding_schema": outstanding_schema,
                "cache_path": cache_db_path
            }

        except Exception as e:
            _log.error(f"Error inspecting cache database: {e}", exc_info=True)
            return {"status": "error", "message": f"Exception: {str(e)}"}

    @RPC.export
    def transfer_data_to_tents(self, table_name, table_columns):
        """
        RPC method to transfer data from the cache database to the TENTS data manager.

        :param table_name: Name of the table to query in the cache and create/use in TENTS
        :param table_columns: List of column definitions for the table
        :return: Dictionary with status and results information
        """
        _log.info(f"Received request to transfer data for table: {table_name}")

        if not table_name:
            return {"status": "error", "message": "Table name cannot be empty"}

        if not isinstance(table_columns, list):
            return {"status": "error", "message": "Table columns must be provided as a list"}

        # Check if data manager is initialized
        if self.data_manager is None:
            try:
                self.data_manager = LocalDataManager(
                    transactive_node=None,
                    db_params=self.connection["params"]
                )
                self.data_manager.init_archive()
            except Exception as e:
                _log.error(f"Failed to initialize data manager: {e}")
                return {"status": "error", "message": f"Data manager initialization failed: {str(e)}"}

        try:
            # Step 1: Ensure the table exists in the data manager
            self._ensure_table_exists(table_name, table_columns)

            # Step 2: Extract matching records from cache
            records = self._extract_records_from_cache(table_name)

            if not records:
                return {
                    "status": "success",
                    "message": f"No records found for table {table_name} in cache",
                    "records_transferred": 0
                }

            # Step 3: Transfer records to TENTS data manager
            self._transfer_records_to_tents(records)

            return {
                "status": "success",
                "message": f"Successfully transferred records for table {table_name}",
                "records_transferred": len(records)
            }
        except Exception as e:
            _log.error(f"Error transferring data to TENTS: {e}", exc_info=True)
            return {"status": "error", "message": f"Exception: {str(e)}"}

    def _ensure_table_exists(self, table_name, table_columns):
        """
        Ensure the table exists in the TENTS data manager.

        :param table_name: Name of the table
        :param table_columns: List of column definitions
        """
        _log.debug(f"Ensuring table {table_name} exists in TENTS database")

        # Check if table already exists in the data manager
        if not hasattr(self.data_manager, 'orm') or table_name not in self.data_manager.orm:
            _log.info(f"Creating table {table_name} in TENTS database")

            # Create table definition
            table_def = {
                "name": table_name,
                "columns": table_columns
            }

            # Add table to data manager
            self.data_manager.add_tables([table_def])

            # Create the table in the database
            self.data_manager.init_archive()
        else:
            _log.debug(f"Table {table_name} already exists in TENTS database")

    def _extract_records_from_cache(self, table_name):
        """
        Extract records for the given table from the cache database.

        :param table_name: Name of the table to query for
        :return: List of records in the format expected by TENTS data manager
        """
        _log.debug(f"Extracting records for {table_name} from cache")

        # Get path to the cache database
        cache_db_path = self._get_cache_db_path()

        records = []

        try:
            # Connect to the SQLite cache database
            conn = sqlite3.connect(cache_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get topic ID for the table name
            cursor.execute("SELECT topic_id FROM topics WHERE topic_name = ?", (f"TNS/{table_name}",))
            result = cursor.fetchone()

            if not result:
                _log.info(f"No topic found for table {table_name} in cache")
                return records

            topic_id = result[0]

            # Query the outstanding records for this topic
            cursor.execute("""
                SELECT id, ts, source, topic_id, value_string, header_string 
                FROM outstanding 
                WHERE topic_id = ? 
                ORDER BY ts
            """, (topic_id,))

            rows = cursor.fetchall()

            # Process each row
            for row in rows:
                # Parse the value string (which contains our TNS record)
                value = json.loads(row['value_string'])

                # Extract the relevant data
                if isinstance(value, dict) and 'table_name' in value and 'data' in value:
                    # We have the right structure - use it as is
                    records.append(value)
                else:
                    _log.warning(f"Record with ID {row['id']} does not have the expected structure")

            _log.info(f"Extracted {len(records)} records for table {table_name}")
            return records

        except Exception as e:
            _log.error(f"Error extracting records from cache: {e}", exc_info=True)
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def _transfer_records_to_tents(self, records):
        """
        Transfer records to the TENTS data manager.
        :param records: List of records to transfer
        """
        if not records:
            return

        _log.debug(f"Transferring {len(records)} records to TENTS database")
        try:
            # Process records to convert string timestamps to datetime objects
            for record in records:
                if isinstance(record['data'], dict) and 'timestamp' in record['data']:
                    # Convert timestamp string to datetime object
                    timestamp_str = record['data']['timestamp']
                    if isinstance(timestamp_str, str):
                        try:
                            from datetime import datetime
                            import pytz
                            # Parse ISO format timestamp
                            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            if dt.tzinfo is None:
                                dt = pytz.UTC.localize(dt)
                            record['data']['timestamp'] = dt
                        except ValueError:
                            _log.error(f"Unable to parse timestamp: {timestamp_str}")

            # Use the TENTS data manager to archive the records
            self.data_manager.archive_data(records)
            _log.info(f"Successfully transferred {len(records)} records to TENTS database")
        except Exception as e:
            _log.error(f"Error transferring records to TENTS database: {e}", exc_info=True)
            raise

    def _get_cache_db_path(self):
        """
        Get the path to the cache database.

        :return: Path to the cache database
        """
        # The backup database is typically in the agent-data directory
        # Check if we're in an agent-data directory structure
        current_dir = os.getcwd()
        agent_data_dir = os.path.join(current_dir, f"{os.path.basename(current_dir)}.agent-data")

        if os.path.exists(agent_data_dir):
            return os.path.join(agent_data_dir, 'backup.sqlite')
        else:
            # Fall back to the current directory
            return os.path.join(current_dir, 'backup.sqlite')

    def version(self):
        """
        Return the current version number of the historian
        :return: version number
        """
        return __version__

    def query_historian(self, topic, start=None, end=None, agg_type=None,
                        agg_period=None, skip=0, count=None, order=None):
        """
        Implementation of the query functionality for the historian.
        """
        # This would be implemented based on your storage backend
        _log.warning("Query operations are not fully implemented in TNSHistorian2")
        return {"values": [], "metadata": {}}

    def query_topic_list(self):
        """Return list of topics in the database"""
        _log.debug("Requested topic list")
        return []

    def query_topics_by_pattern(self, topic_pattern):
        """Find topics matching the given pattern"""
        _log.debug(f"Requested topics matching pattern: {topic_pattern}")
        return []

    def query_topics_metadata(self, topics):
        """Return metadata for the specified topics"""
        _log.debug(f"Requested metadata for topics: {topics}")
        return {}

    def query_aggregate_topics(self):
        """Return a list of available aggregate topics"""
        _log.debug("Requested aggregate topics list")
        return []


def main():
    try:
        utils.vip_main(historian, version=__version__)
    except Exception as e:
        _log.error(f"Historian error: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()