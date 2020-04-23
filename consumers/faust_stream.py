"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="PLAINTEXT://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("cta.stations", value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("cta.stations.out", value_type=TransformedStation, partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "cta_stations",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def cta_station_event(cta_stations):
    async for station in cta_stations:
        table["station_id"] = station.station_id
        table["station_name"] = station.station_name
        table["order"] = station.order
        
        if station.red:
            table["line"] = "red"
        elif station.blue:
            table["line"] = "blue"
        elif station.green:
            table["line"] = "green"
        #TransformedStation.station_id = station.station_id
        #TransformedStation.station_name = station.station_name
        #TransformedStation.order = station.order
        #TransformedStation.line = 'red' if station.red else 'blue' if station.blue else 'green'
        #station.forward(out_topic)


if __name__ == "__main__":
    app.main()
