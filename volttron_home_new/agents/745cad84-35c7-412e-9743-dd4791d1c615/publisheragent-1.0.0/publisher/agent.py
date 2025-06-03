#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TransactiveDataPublisher Agent

Publishes a record in Archivable format every 10 seconds with a random value.
"""

__version__ = "1.0.0"

import logging
import random  # New import for random values
from datetime import datetime
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core

try:
    from volttron.client.messaging import headers as headers_mod
except ImportError:
    from volttron.platform.messaging import headers as headers_mod

utils.setup_logging()
_log = logging.getLogger(__name__)

class TransactiveDataPublisher(Agent):
    def __init__(self, config_path, **kwargs):
        super().__init__(**kwargs)
        self.config = utils.load_config(config_path)
        # Use defaults if not set
        self.table_name = self.config.get("table_name", "transactive_data2")
        self.publish_interval = int(self.config.get("publish_interval", 10))
        self.source = self.config.get("source", "publisheragent")
        _log.info("Starting publisher for table %s every %s seconds", self.table_name, self.publish_interval)

    @Core.receiver("onstart")
    def on_start(self, sender, **kwargs):
        self.core.periodic(self.publish_interval, self.publish_data)

    def publish_data(self):
        # Create a record with a random value.
        record = {
            "table_name": self.table_name,
            'schema_name': 'tns_data',  # Include the schema name here
            "data": {
                "source": self.source,
                "topic": self.table_name,
                # For simplicity, keep the same timestamp or use datetime.now()
                "timestamp": datetime.fromisoformat("2025-02-18T17:00:00").isoformat(),
                "value": round(random.uniform(0, 100), 2),
                "meta": {"info": "This is a test record"}
            }
        }
        headers = {
            headers_mod.TIMESTAMP: utils.format_timestamp(datetime.utcnow())
        }
        topic = f"TNS/{self.table_name}"
        _log.info("Publishing to %s: %s", topic, record)
        try:
            self.vip.pubsub.publish(peer="pubsub", topic=topic, headers=headers, message=record).get(timeout=10)
        except Exception as e:
            _log.error("Error publishing data: %s", e)

def publisher(config_path, **kwargs):
    utils.update_kwargs_with_config(kwargs, utils.load_config(config_path))
    return TransactiveDataPublisher(config_path, **kwargs)

def main():
    try:
        utils.vip_main(publisher, version=__version__)
    except Exception as e:
        _log.exception("Unhandled exception", exc_info=True)

if __name__ == '__main__':
    main()
