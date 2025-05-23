#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TNSHistorian2
A VOLTTRON historian that properly leverages BaseHistorian features to store TNS data records.
The historian subscribes to TNS topics and stores the complete record data in the value field of the historian.
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

#TODO Test create tables in bulk
#TODO ensure schemas are working as intended
#TODO ensure all topics work with custom topic defined
#TODO ensure unknown tables are handled. unknown tables?

def historian(config_path, **kwargs):
    """
    Parse the config file and initialize the historian with it.
    """
    # Note: schema is not part of the historian configuration because it is only passed to LocalDataManager.
    config = utils.load_config(config_path) or {
        "connection": {
            "type": "sqlite",
            "params": {"url": "sqlite:////home/volttron/historian_data.sqlite"}
        },
        "topic_replace_list": [],
        "custom_topics": {"capture_record_data": ["TNS/"]}  # Correct format
    }
    connection = config.get("connection")
    if "connection" in config:
        del config["connection"]
    utils.update_kwargs_with_config(kwargs, config)
    return TNSHistorian2(connection=connection, **kwargs)

class TNSHistorian2(BaseHistorian):
    """
    Historian implementation that extends the BaseHistorian for TNS records.
    It subscribes to TNS topics and stores the complete record data in the value field.
    """
    def __init__(self, connection, **kwargs):
        _log.info("TNSHistorian2 initialization started")
        # Save connection info before calling parent constructor.
        self.connection = connection
        # Track which tables are automatically stored.
        self.tracked_tables = set()

        # Ensure SQLite connection parameters include a URL if using SQLite.
        if self.connection.get("type", "").lower() == "sqlite":
            db_params = self.connection.get("params", {})
            if "url" in db_params and db_params["url"].startswith("sqlite:///"):
                db_path = db_params["url"].replace("sqlite:///", "")
                if db_path.startswith("/"):
                    db_dir = os.path.dirname(db_path)
                    if not os.path.exists(db_dir):
                        try:
                            _log.info(f"Creating directory for database: {db_dir}")
                            os.makedirs(db_dir, exist_ok=True)
                        except Exception as e:
                            _log.warning(f"Cannot create directory: {e}")
                            agent_data_dir = self._get_agent_data_dir()
                            new_db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                            _log.info(f"Using fallback database path: {new_db_path}")
                            db_params["url"] = f"sqlite:///{new_db_path}"
            if "url" not in db_params:
                agent_data_dir = self._get_agent_data_dir()
                db_path = os.path.join(agent_data_dir, "historian_data.sqlite")
                db_params["url"] = f"sqlite:///{db_path}"
            self.connection["params"] = db_params
            _log.info(f"Using SQLite connection parameters: {db_params}")

        # Initialize BaseHistorian – it handles subscriptions and caching.
        super(TNSHistorian2, self).__init__(**kwargs)
        # Initialize the TENTS data manager after BaseHistorian is set up.
        # We pass schema "tns" to LocalDataManager.
        try:
            self.data_manager = LocalDataManager(
                transactive_node=None,  # Not needed for direct calls.
                db_params=self.connection["params"],
                schema="tns"
            )
            _log.info("Successfully created TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to create TENTS data manager: {e}")
            self.data_manager = None

    def _get_agent_data_dir(self):
        """
        Get (and create if necessary) the agent's data directory.
        """
        current_dir = os.getcwd()
        agent_name = os.path.basename(current_dir)
        agent_data_dir = os.path.join(current_dir, f"{agent_name}.agent-data")
        if not os.path.exists(agent_data_dir):
            try:
                os.makedirs(agent_data_dir, exist_ok=True)
            except Exception as e:
                _log.warning(f"Cannot create agent data directory: {e}")
                agent_data_dir = current_dir
        return agent_data_dir

    def historian_setup(self):
        """Called at startup and after configuration changes."""
        _log.info("Setting up TNSHistorian2")
        if self.data_manager is None:
            try:
                self.data_manager = LocalDataManager(
                    transactive_node=None,
                    db_params=self.connection["params"],
                    schema="tns"
                )
            except Exception as e:
                _log.error(f"Failed to initialize TENTS data manager: {e}")
                return
        try:
            self.data_manager.init_archive()
            _log.info("Successfully initialized TENTS data manager")
        except Exception as e:
            _log.error(f"Failed to initialize TENTS data manager: {e}")

    @RPC.export
    def register_table(self, table_name, table_columns):
        """
        RPC method to register a table for automatic storage.
        """
        _log.info(f"Registering table: {table_name}")
        if not table_name or not isinstance(table_columns, list):
            return {"status": "error", "message": "Invalid table name or columns"}
        if self.data_manager is None:
            try:
                _log.info(f"Initializing data manager with params: {self.connection['params']}")
                self.data_manager = LocalDataManager(
                    transactive_node=None,
                    db_params=self.connection["params"],
                    schema="tns"
                )
                self.data_manager.init_archive()
            except Exception as e:
                _log.error(f"Failed to initialize data manager: {e}")
                return {"status": "error", "message": f"Data manager init failed: {str(e)}"}
        try:
            self._ensure_table_exists(table_name, table_columns)
            self.tracked_tables.add(table_name)
            _log.info(f"Table {table_name} registered. Tracked tables: {list(self.tracked_tables)}")
            # (Additional verifications can be added here.)
            return {"status": "success", "message": f"Table {table_name} registered", "tracked_tables": list(self.tracked_tables)}
        except Exception as e:
            _log.error(f"Error registering table: {e}", exc_info=True)
            return {"status": "error", "message": f"Exception: {str(e)}"}

    @RPC.export
    def create_tables(self, message):
        try:
            # Parse the message.
            if isinstance(message, str):
                payload = json.loads(message)
            else:
                payload = message
            definitions_str = payload.get("definitions", "[]")
            schema_from_msg = payload.get("schema")
            if schema_from_msg:
                self.schema = schema_from_msg
            definitions = json.loads(definitions_str)
        except Exception as e:
            _log.error("Failed to parse create_tables message: %s", e)
            return {"status": "error", "message": "Failed to parse message"}

        _log.info("Creating tables with schema: %s", self.schema)
        # Register the tables in the metadata.
        self.data_manager.add_tables(definitions)

        # Commit the tables to the actual database.
        self.data_manager.registry.metadata.create_all(self.data_manager.engine, checkfirst=True)
        _log.info("Tables have been created and committed to the database.")

        return {"status": "success", "message": "Tables created successfully."}

    def publish_to_historian(self, to_publish_list):
        """
        Main publish method for TNSHistorian2.
        """
        _log.debug(f"Processing {len(to_publish_list)} records")
        if not to_publish_list:
            return
        try:
            records_to_store = []
            _log.info(f"Tracked tables: {self.tracked_tables}")
            for record in to_publish_list:
                _log.info(f"Processing record: {record}")
                topic = record.get('topic', '')
                if topic.startswith('TNS/'):
                    _log.info("Found TNS topic")
                    if 'value' in record:
                        value = record['value']
                    elif record.get('readings'):
                        timestamp, value = record['readings'][0]
                    else:
                        _log.warning("No value or readings found in record")
                        continue
                    if isinstance(value, dict):
                        table_name = value.get('table_name')
                        if table_name and table_name in self.tracked_tables:
                            data = value.get('data', {}).copy()
                            if isinstance(data.get('timestamp'), str):
                                try:
                                    from datetime import datetime
                                    import pytz
                                    dt = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
                                    if dt.tzinfo is None:
                                        dt = pytz.UTC.localize(dt)
                                    data['timestamp'] = dt
                                except ValueError as e:
                                    _log.error(f"Timestamp parse error: {data['timestamp']} - {e}")
                            records_to_store.append({'table_name': table_name, 'data': data})
                        else:
                            _log.info(f"Table {table_name} not tracked")
                    else:
                        _log.warning(f"Value is not a dict: {type(value)}")
                else:
                    _log.debug(f"Skipping non-TNS topic: {topic}")
            _log.info(f"Records to store: {len(records_to_store)}")
            if records_to_store:
                try:
                    _log.info("Archiving records...")
                    self.data_manager.archive_data(records_to_store)
                    _log.info("Records archived successfully")
                    self.report_handled(to_publish_list)
                except Exception as e:
                    _log.error(f"Error storing records: {e}", exc_info=True)
            else:
                _log.info("No records to store")
        except Exception as e:
            _log.error(f"Error in publish_to_historian: {e}", exc_info=True)

    @RPC.export
    def get_tracked_tables(self):
        """
        RPC method to return the list of tracked tables.
        """
        return list(self.tracked_tables)

    @RPC.export
    def inspect_cache_database(self):
        """
        RPC method to inspect the cache database and return information.
        """
        try:
            cache_db_path = self._get_cache_db_path()
            _log.info(f"Inspecting cache database at: {cache_db_path}")
            conn = sqlite3.connect(cache_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT topic_id, topic_name FROM topics")
            topics = [dict(row) for row in cursor.fetchall()]
            topic_counts = {}
            tns_records_sample = {}
            for topic in topics:
                topic_id = topic['topic_id']
                topic_name = topic['topic_name']
                cursor.execute("SELECT COUNT(*) FROM outstanding WHERE topic_id = ?", (topic_id,))
                count = cursor.fetchone()[0]
                topic_counts[topic_name] = count
                if topic_name.startswith('TNS/'):
                    cursor.execute("SELECT id, ts, value_string FROM outstanding WHERE topic_id = ? LIMIT 1", (topic_id,))
                    row = cursor.fetchone()
                    if row:
                        try:
                            value_data = json.loads(row['value_string'])
                            if isinstance(value_data, dict):
                                tns_records_sample[topic_name] = {
                                    "id": row['id'],
                                    "timestamp": row['ts'],
                                    "table_name": value_data.get('table_name', 'unknown'),
                                    "data_keys": list(value_data.get('data', {}).keys()) if 'data' in value_data else []
                                }
                        except Exception as e:
                            tns_records_sample[topic_name] = f"Error parsing: {e}"
            cursor.execute("SELECT COUNT(*) FROM outstanding")
            total_records = cursor.fetchone()[0]
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
            return {"status": "error", "message": f"Exception: {e}"}

    @RPC.export
    def transfer_data_to_tents(self, table_name, table_columns):
        """
        RPC method to transfer data from cache to the TENTS data manager.
        """
        _log.info(f"Transferring data for table: {table_name}")
        if not table_name or not isinstance(table_columns, list):
            return {"status": "error", "message": "Invalid table name or columns"}
        if self.data_manager is None:
            try:
                self.data_manager = LocalDataManager(
                    transactive_node=None,
                    db_params=self.connection["params"],
                    schema="tns"
                )
                self.data_manager.init_archive()
            except Exception as e:
                _log.error(f"Failed to initialize data manager: {e}")
                return {"status": "error", "message": f"Data manager init failed: {e}"}
        try:
            self._ensure_table_exists(table_name, table_columns)
            records = self._extract_records_from_cache(table_name)
            if not records:
                return {"status": "success", "message": f"No records for table {table_name} in cache", "records_transferred": 0}
            self._transfer_records_to_tents(records)
            return {"status": "success", "message": f"Transferred records for table {table_name}", "records_transferred": len(records)}
        except Exception as e:
            _log.error(f"Error transferring data to TENTS: {e}", exc_info=True)
            return {"status": "error", "message": f"Exception: {e}"}

    def _ensure_table_exists(self, table_name, table_columns):
        """
        Ensure the table exists in the TENTS data manager.
        """
        _log.debug(f"Ensuring table {table_name} exists")
        if not hasattr(self.data_manager, 'orm') or table_name not in self.data_manager.orm:
            _log.info(f"Creating table {table_name}")
            table_def = {"name": table_name, "columns": table_columns}
            self.data_manager.add_tables([table_def])
            self.data_manager.init_archive()
        else:
            _log.debug(f"Table {table_name} already exists")

    def _extract_records_from_cache(self, table_name):
        """
        Extract records for the given table from the cache.
        """
        _log.debug(f"Extracting records for {table_name}")
        cache_db_path = self._get_cache_db_path()
        records = []
        try:
            conn = sqlite3.connect(cache_db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT topic_id FROM topics WHERE topic_name = ?", (f"TNS/{table_name}",))
            result = cursor.fetchone()
            if not result:
                _log.info(f"No topic for table {table_name} in cache")
                return records
            topic_id = result[0]
            cursor.execute("""
                SELECT id, ts, source, topic_id, value_string, header_string 
                FROM outstanding 
                WHERE topic_id = ? 
                ORDER BY ts
            """, (topic_id,))
            rows = cursor.fetchall()
            for row in rows:
                value = json.loads(row['value_string'])
                if isinstance(value, dict) and 'table_name' in value and 'data' in value:
                    records.append(value)
                else:
                    _log.warning(f"Record {row['id']} does not have expected structure")
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
        """
        if not records:
            return
        _log.debug(f"Transferring {len(records)} records")
        try:
            for record in records:
                if isinstance(record['data'], dict) and 'timestamp' in record['data']:
                    timestamp_str = record['data']['timestamp']
                    if isinstance(timestamp_str, str):
                        try:
                            from datetime import datetime
                            import pytz
                            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            if dt.tzinfo is None:
                                dt = pytz.UTC.localize(dt)
                            record['data']['timestamp'] = dt
                        except ValueError:
                            _log.error(f"Unable to parse timestamp: {timestamp_str}")
            self.data_manager.archive_data(records)
            _log.info(f"Transferred {len(records)} records to TENTS database")
        except Exception as e:
            _log.error(f"Error transferring records: {e}", exc_info=True)
            raise

    def _get_cache_db_path(self):
        """
        Get the cache database path.
        """
        current_dir = os.getcwd()
        agent_data_dir = os.path.join(current_dir, f"{os.path.basename(current_dir)}.agent-data")
        if os.path.exists(agent_data_dir):
            return os.path.join(agent_data_dir, 'backup.sqlite')
        else:
            return os.path.join(current_dir, 'backup.sqlite')

    def version(self):
        """
        Return the current version number of the historian.
        """
        return __version__

    def query_historian(self, topic, start=None, end=None, agg_type=None,
                         agg_period=None, skip=0, count=None, order=None):
        _log.warning("Query operations not fully implemented")
        return {"values": [], "metadata": {}}

    def query_topic_list(self):
        _log.debug("Requested topic list")
        return []

    def query_topics_by_pattern(self, topic_pattern):
        _log.debug(f"Topics matching pattern {topic_pattern}")
        return []

    def query_topics_metadata(self, topics):
        _log.debug(f"Metadata for topics {topics}")
        return {}

    def query_aggregate_topics(self):
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
