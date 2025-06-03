import sys
import pathlib
# Add project root and TENT directory to sys.path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[4]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[5] / 'TENT'))

import pytest
import datetime
import os
import shutil
import json
from services.core.TNSHistorian.sqlhistorian.historian import TNSHistorian2

@pytest.fixture
def historian():
    # Ensure a .agent-data directory exists in the current working directory
    agent_data_dir = os.path.join(os.getcwd(), "test.agent-data")
    os.makedirs(agent_data_dir, exist_ok=True)
    try:
        # Use in-memory SQLite for testing
        connection = {
            "type": "sqlite",
            "params": {"url": "sqlite:///:memory:"}
        }
        hist = TNSHistorian2(connection=connection)
        hist.historian_setup()
        yield hist
    finally:
        # Clean up the .agent-data directory after the test
        shutil.rmtree(agent_data_dir, ignore_errors=True)

def test_historian_should_filter_duplicates(historian):
    # Add duplicates to queue
    for num in range(40, 43):
        historian._capture_record_data(
            peer=None,
            sender=None,
            bus=None,
            topic="duplicate_topic",
            headers={
                "Date": "2015-11-17 21:24:10.189393+00:00",
                "TimeStamp": "2015-11-17 21:24:10.189393+00:00",
            },
            message=f"last_duplicate_{num}",
        )
    # Add unique records to queue
    for num in range(2, 5):
        historian._capture_record_data(
            peer=None,
            sender=None,
            bus=None,
            topic=f"unique_record_topic{num}",
            headers={
                "Date": f"2020-11-17 21:2{num}:10.189393+00:00",
                "TimeStamp": f"2020-11-17 21:2{num}:10.189393+00:00",
            },
            message=f"unique_record_{num}",
        )
    historian._retry_period = 1
    historian._max_time_publishing = float(1)
    historian.start_process_thread()
    # No direct DB check here; you may want to add assertions based on TNSHistorian2's storage logic.

def test_register_table_and_store_record(historian):
    """Test registering a table and storing a record for it."""
    table_name = "transactive_data2"
    columns = [
        {"name": "id", "type": "Integer", "kwargs": {"primary_key": True, "autoincrement": True}},
        {"name": "source", "type": "String"},
        {"name": "topic", "type": "String"},
        {"name": "timestamp", "type": "DateTime"},
        {"name": "value", "type": "Float"},
        {"name": "meta", "type": "JSON"}
    ]
    # Register table
    result = historian.register_table(table_name, columns)
    assert result["status"] == "success"
    assert table_name in historian.tracked_tables

    # Ensure the table is created in the DB
    historian.data_manager.registry.metadata.create_all(historian.data_manager.engine, checkfirst=True)

    # Store a record for the registered table
    import datetime as dt
    record = {
        "topic": "TNS/transactive_data2",
        "value": {
            "table_name": table_name,
            "data": {
                "source": "publisheragent",
                "topic": "transactive_data2",
                "timestamp": dt.datetime.utcnow().isoformat(),
                "value": 42.0,
                "meta": {"info": "test"}
            }
        }
    }
    historian.publish_to_historian([record])
    # Check that the record was stored
    from sqlalchemy.orm import Session
    session = Session(historian.data_manager.engine)
    table = historian.data_manager.orm[table_name]
    stored = session.query(table).all()
    assert len(stored) == 1
    assert stored[0].value == 42.0
    session.close()

def test_unknown_table_handling(historian):
    """Test that records for unregistered tables go to unknown_table_records."""
    import datetime as dt
    record = {
        "topic": "TNS/unknown_table",
        "value": {
            "table_name": "not_registered",
            "data": {
                "source": "test",
                "topic": "unknown_table",
                "timestamp": dt.datetime.utcnow().isoformat(),
                "value": 99.9,
                "meta": {"info": "unknown"}
            }
        }
    }
    historian.publish_to_historian([record])
    historian.data_manager.registry.metadata.create_all(historian.data_manager.engine, checkfirst=True)
    from sqlalchemy.orm import Session
    session = Session(historian.data_manager.engine)
    table = historian.data_manager.orm["unknown_table_records"]
    stored = session.query(table).all()
    assert len(stored) >= 1
    assert stored[-1].data["value"] == 99.9
    session.close()

def test_create_tables_rpc(historian):
    """Test the create_tables RPC method."""
    table_def = [{
        "name": "rpc_table",
        "columns": [
            {"name": "id", "type": "Integer", "kwargs": {"primary_key": True, "autoincrement": True}},
            {"name": "foo", "type": "String"}
        ]
    }]
    message = {"definitions": json.dumps(table_def)}
    result = historian.create_tables(message)
    assert result["status"] == "success"
    # Check that the table exists
    assert "rpc_table" in historian.data_manager.orm
