from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import WriteType
import typing
import atexit
import datetime
import time as t
import reactivex as rx
from reactivex import operators as ops

DEFAULT_TIME_PRECISION = "ns"
DEFAULT_POOL_SIZE = 25
# https://docs.influxdata.com/influxdb/v2/write-data/best-practices/optimize-writes/#batch-writes
DEFAULT_BATCH_SIZE = 5000
DEFAULT_QUEUE_SIZE = DEFAULT_BATCH_SIZE
DEFAULT_QUEUE_FLUSH_TIMEDELTA = datetime.timedelta(minutes=5)
FLUSH_PERIODICALLY = True
DEFAULT_DATABASE = 'icinga2'
DEFAULT_RETENTION = 'autogen'


class AsyncInfluxDBPublisher():
    write_queue = []

    def __init__(self, username, password, database: str = DEFAULT_DATABASE, url: str = "http://localhost:8086"):
        self.client = InfluxDBClient(url=url, token=f"{username}:{password}", org="-",
                                     connection_pool_maxsize=DEFAULT_POOL_SIZE, enable_gzip=True)
        self.write_api = self.client.write_api()
        self.bucket = f"{database}/{DEFAULT_RETENTION}"
        self.last_queue_flush = datetime.datetime.now()
        atexit.register(self.cleanup)

    def cleanup(self):
        self.flush_queue()
        self.write_api.close()
        self.client.close()

    def write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]):
        # En vez de añadir el servicio a un dict ya existente, lo creamos asi directamente para asegurarnos de que está ordenado lexicograficamente
        # https://docs.influxdata.com/influxdb/v2/write-data/best-practices/optimize-writes/#sort-tags-by-key
        time = t.time_ns()
        val = {
            "measurement": measurement,
            "time": time,
            "tags": {
                **tag_values
            },
            "fields": {
                "value": value
            }
        }
        return self.write_api.write(bucket=self.bucket, record=[val])

    def batch_write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]):
        time = t.time_ns()
        dt = datetime.datetime.now()
        val = {
            "measurement": measurement,
            "time": time,
            "tags": {
                **tag_values
            },
            "fields": {
                "value": value
            }
        }

        self.write_queue.append(val)

        if len(self.write_queue) >= DEFAULT_QUEUE_SIZE \
                or (FLUSH_PERIODICALLY and (dt - self.last_queue_flush) > DEFAULT_QUEUE_FLUSH_TIMEDELTA):
            return self.flush_queue()

    def flush_queue(self):
        # Puede pasar si la cola está vacía cuando se borra la instancia.

        if not self.write_queue:
            return
        size = len(self.write_queue)
        if result := self.write_api.write(bucket=self.bucket, record=self.write_queue):
            self.write_queue.clear()
            self.last_queue_flush = datetime.datetime.now()
        print(f"Flushing queue of size {size}. Result: {result}")
        return result
