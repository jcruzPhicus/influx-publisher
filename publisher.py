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
DEFAULT_QUEUE_FLUSH_SIZE_TIME = int(
    datetime.timedelta(minutes=5).total_seconds())
FLUSH_PERIODICALLY = True

NANOSECONDS_TO_MICROSECONDS = 10**3
NANOSECONDS_TO_SECONDS = 10**9


class InfluxDBPublisher():
    write_queue: typing.List[dict] = []

    def __init__(self, username: str, password: str, database: str):
        self.client: InfluxDBClient = InfluxDBClient(username=username, password=password,
                                                     database=database, pool_size=DEFAULT_POOL_SIZE, gzip=True)
        self.last_queue_flush: int = t.time_ns()
        atexit.register(self.cleanup)

    def cleanup(self):
        self.flush_queue()
        self.client.close()

    def write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]) -> bool:
        # En vez de añadir el servicio a un dict ya existente, lo creamos asi directamente para asegurarnos de que está ordenado lexicograficamente
        # https://docs.influxdata.com/influxdb/v2/write-data/best-practices/optimize-writes/#sort-tags-by-key
        """Escribe una métrica de forma inmediata a InfluxDB.

        Args:
            measurement (str): Nombre de la medida (p.ej: check_txrx)
            tag_values (dict): Tags de la medida (p.ej: servicio, unidad de la medida)
            value (typing.Union[int, float]): Valor de la medida

        Returns:
            bool: Si la operación ha funcionado correctamente.
        """
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

    def batch_write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]) -> bool:
        """Escribe una métrica a InfluxDB, haciendo uso de la escritura por lotes para hacerlo de forma eficiente.
        Por lo general, sólo añade el dato a una cola. Cuando la cola llega al valor de la variable de DEFAULT_QUEUE_SIZE
        se envían todos los datos a InfluxDB, minimizando así la cantidad de escrituras que se hacen, que hace que sea más efectivo
        y que el publisher gaste menos tiempo haciendo peticiones de red.

        Es importante tener en cuenta que una métrica escrita con esta función no aparecerá de forma inmediata e incluso nisiquiera
        de forma rápida: se espera a que se llene la cola, o pasen DEFAULT_QUEUE_FLUSH_SIZE_TIME segundos antes de reflejar los cambios en la BBDD.

        Args:
            measurement (str): El nombre de la medida.
            tag_values (dict): Tags de la medida.
            value (typing.Union[int, float]): Valor de la medida.

        Returns:
            bool: Si la operación ha sido correcta. Notesé que llamar esta función no siempre hace llamadas a Influx,
                por lo tanto en ciertos casos se pueden hacer muchas llamadas que den true, y justo fallar la que llama a Influx.
        """
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
                    and (time - self.last_queue_flush) / NANOSECONDS_TO_SECONDS > DEFAULT_QUEUE_FLUSH_SIZE_TIME):
            return self.flush_queue()
        return True

    def flush_queue(self) -> bool:
        """Función que envía todos los datos de la cola que se añaden con batch_write_metric

        Returns:
            bool: Resultado de la operación de Influx, False si ha fallado o no se ha realizado ninguna operación.
        """
        if not self.write_queue:
            # Puede pasar si la cola está vacía cuando se borra la instancia, o cada 5 minutos si no hay escrituras.
            self.last_queue_flush = t.time_ns()
            return False

        size = len(self.write_queue)
        if result := self.client.write_points(self.write_queue, batch_size=DEFAULT_BATCH_SIZE):
            self.write_queue.clear()
            self.last_queue_flush = t.time_ns()
        print(f"Flushing queue of size {size}. Result: {result}")
        return result

    async def awrite_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]):
        return self.write_metric(measurement, tag_values, value)

    async def abatch_write_metric(self, measurement: str, tag_values: dict, value: typing.Union[int, float]):
        return self.batch_write_metric(measurement, tag_values, value)
