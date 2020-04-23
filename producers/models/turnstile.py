"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )
        #print('Inside Turnstile 1:',station_name)
        super().__init__(
            #f"{station_name}", # TODO: Come up with a better topic name
            topic_name='cta.kafka.turnstiles',
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=1,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("turnstile kafka integration started")
        
        self.producer.produce(
            topic=self.topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            key={"timestamp": self.time_millis()},
            value={
                # TODO: Configure this
                "station_id": self.station.station_id,
                "station_name": self.station.name,
                "line": self.station.color.name,
                "entries": num_entries
            },
        )
        #for _ in range(num_entries):
        #    self.producer.produce(
        #        topic=self.topic_name,
        #        key_schema=self.key_schema,
        #        value_schema=self.value_schema,
        #        key={"timestamp": self.time_millis()},
        #        value={
        #            # TODO: Configure this
        #            'station_id': self.station.station_id,
        #            'station_name': self.station.name,
        #            'line': self.station.color,
        #        },
        #    )
            #print('Inside Turnstile 2:',self.station.station_id)
            #print('Inside Turnstile 3:',self.station.name)
            #print('Inside Turnstile 4:',self.station.color)
