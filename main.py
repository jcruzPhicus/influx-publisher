import publisher
import publisherv2
import random
import time
measurement_names = ["check_txrx", "check_cpe", "check_cpe_docsis",
                     "check_bw", "check_uptime", "check_whatever", "idk_what_more_names_to_make"]
metric_names = ["upbw", "dnbw", "uptx", "dntx", "cpu", "time"]
unit = ["percent", "dbw"]
service = ["service"]


def create_fake_data():
    measurement = random.choice(measurement_names)
    tags = {}
    tags["metric"] = random.choice(metric_names)
    while (random.random() > 0.75):
        if not tags.setdefault("unit", random.choice(unit)):
            continue
        if not tags.setdefault("service", random.choice(service)):
            continue
    if unit_type := tags.get("unit") == "percent":
        value = random.random() * 100
    elif unit_type == "dbw":
        value = -(random.random() * 100)
    else:
        value = random.random()
    return measurement, tags, value


if __name__ == "__main__":
    # pub: publisherv2.AsyncInfluxDBPublisher = publisherv2.AsyncInfluxDBPublisher(
    #     "root", "root", "icinga2")
    pub: publisher.InfluxDBPublisher = publisher.InfluxDBPublisher(
        "root", "root", "icinga2")
    write_total = 1000 * 1000
    data = []
    for i in range(write_total):
        m, t, v = create_fake_data()
        data.append((m, t, v))
    start = time.time()
    for point in data:
        # pub.write_metric(point[0], point[1], point[2])
        pub.batch_write_metric(point[0], point[1], point[2])
    end = time.time()
    elapsed = end - start
    print(f"Total time for writing {write_total} metrics with batches of {publisher.DEFAULT_BATCH_SIZE} \
and a queue size of {publisher.DEFAULT_QUEUE_SIZE}:\n{elapsed}s")
