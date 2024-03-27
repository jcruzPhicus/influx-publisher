from influxdb import InfluxDBClient
import typing
import atexit
import datetime
import time as t


DEFAULT_TIME_PRECISION = "n"
DEFAULT_POOL_SIZE = 25
# https://docs.influxdata.com/influxdb/v2/write-data/best-practices/optimize-writes/#batch-writes
DEFAULT_BATCH_SIZE = 5000
DEFAULT_QUEUE_SIZE = DEFAULT_BATCH_SIZE * 10
DEFAULT_QUEUE_FLUSH_TIMEDELTA = datetime.timedelta(minutes=5)
FLUSH_PERIODICALLY = True

NANOSECONDS_TO_MICROSECONDS = 10**3
NANOSECONDS_TO_SECONDS = 10**9


class InfluxDBPublisher():
    write_queue = []

    def __init__(self, username, password, database):
        self.client = InfluxDBClient(username=username, password=password,
                                     database=database, pool_size=DEFAULT_POOL_SIZE, gzip=True)
        self.last_queue_flush = t.time_ns()
        atexit.register(self.cleanup)

    def cleanup(self):
        self.flush_queue()
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
        return self.client.write_points([val])

    def batch_write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]):
        # time = datetime.datetime.now()
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

        self.write_queue.append(val)
        if len(self.write_queue) >= DEFAULT_QUEUE_SIZE \
                or (FLUSH_PERIODICALLY
                    and (time - self.last_queue_flush) / NANOSECONDS_TO_SECONDS > 300):
            return self.flush_queue()

    def flush_queue(self):
        # Puede pasar si la cola está vacía cuando se borra la instancia.

        if not self.flush_queue:
            return
        size = len(self.write_queue)
        if result := self.client.write_points(self.write_queue, batch_size=DEFAULT_BATCH_SIZE,
                                              time_precision=DEFAULT_TIME_PRECISION):
            self.write_queue.clear()
            self.last_queue_flush = t.time_ns()
        print(f"Flushing queue of size {size}. Result: {result}")
        return result
