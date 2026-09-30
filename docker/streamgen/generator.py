"""Simulated live logistics events -> Kafka (one topic per raw table).

Every trip produces, over the next few seconds:
    raw.loads + raw.trips          at once
    raw.delivery_events            Pickup after ~3 s, Delivery after ~15 s
    raw.fuel_purchases             1-2 stops in between
    raw.safety_incidents           rarely
plus independent raw.maintenance_records.

Values are random within realistic ranges and ids carry an 'S' (streamed) marker
plus the generator's start time, e.g. TRIPS1790500000-000001, so they never collide
with historic rows. Foreign keys pick existing drivers/trucks/trailers/customers/
routes/facilities (read once from ClickHouse), so facts join to the dimensions.

Message = one raw-table row as JSON (column names = raw table columns),
key = the row's business key. sys_create_date is NOT in the message: the
ingest job stamps it with the Kafka record time.

Env: KAFKA_BOOTSTRAP, TRIPS_PER_SECOND, MAINTENANCE_PER_MINUTE,
     DWH_CH_URL / DWH_CH_USER / DWH_CH_PASSWORD / DWH_SOURCE_DB
"""
import base64
import heapq
import json
import os
import random
import signal
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TRIPS_PER_SECOND = float(os.environ.get("TRIPS_PER_SECOND", "1"))
MAINTENANCE_PER_MINUTE = float(os.environ.get("MAINTENANCE_PER_MINUTE", "3"))
SOURCE_DB = os.environ.get("DWH_SOURCE_DB", "default")

TOPICS = ["raw.loads", "raw.trips", "raw.delivery_events", "raw.fuel_purchases",
          "raw.maintenance_records", "raw.safety_incidents"]
RUN = int(time.time())  # makes ids unique across generator restarts
_seq = {}
_stop = False


def new_id(prefix: str) -> str:
    _seq[prefix] = _seq.get(prefix, 0) + 1
    return f"{prefix}S{RUN}-{_seq[prefix]:06d}"


def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def maybe_empty(value: str, p: float = 0.02) -> str:
    """~2% of historic trips/fuel rows have an empty driver/truck/trailer; keep that."""
    return "" if random.random() < p else value


# ------------------------------------------------------------- reference data

def ch_rows(sql: str) -> list:
    token = base64.b64encode(
        f"{os.environ.get('DWH_CH_USER', 'default')}:{os.environ['DWH_CH_PASSWORD']}".encode()).decode()
    req = urllib.request.Request(os.environ["DWH_CH_URL"].rstrip("/") + "/",
                                 data=f"{sql} FORMAT JSONEachRow".encode(),
                                 headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return [json.loads(line) for line in resp.read().decode().splitlines() if line]


def load_reference() -> dict:
    ids = lambda table, col: [r[col] for r in ch_rows(f"SELECT DISTINCT {col} FROM {SOURCE_DB}.{table}")]
    return {
        "drivers": ids("drivers", "driver_id"),
        "trucks": ids("trucks", "truck_id"),
        "trailers": ids("trailers", "trailer_id"),
        "customers": ids("customers", "customer_id"),
        "routes": ch_rows(f"SELECT route_id, destination_city, destination_state, typical_distance_miles, "
                          f"base_rate_per_mile, fuel_surcharge_rate FROM {SOURCE_DB}.routes"),
        "facilities": ch_rows(f"SELECT facility_id, city, state FROM {SOURCE_DB}.facilities"),
    }


# ------------------------------------------------------------------- events

def make_trip(ref: dict, now: datetime):
    route = random.choice(ref["routes"])
    distance = int(route["typical_distance_miles"])
    load_id, trip_id = new_id("LOAD"), new_id("TRIP")
    driver, truck = maybe_empty(random.choice(ref["drivers"])), maybe_empty(random.choice(ref["trucks"]))

    load = {
        "load_id": load_id, "customer_id": random.choice(ref["customers"]), "route_id": route["route_id"],
        "load_date": now.date().isoformat(), "load_type": random.choice(["Dry Van", "Refrigerated"]),
        "weight_lbs": random.randint(5000, 45000), "pieces": random.randint(1, 30),
        "revenue": round(distance * float(route["base_rate_per_mile"]) * random.uniform(0.9, 1.3), 2),
        "fuel_surcharge": round(distance * float(route["fuel_surcharge_rate"]), 2),
        "accessorial_charges": random.choice([0, 0, 0, 50, 100, 150, 250]),
        "load_status": "Completed", "booking_type": random.choice(["Spot", "Dedicated", "Contract"]),
    }
    miles = int(distance * random.uniform(0.95, 1.15))
    mpg = round(random.uniform(5.5, 7.5), 2)
    trip = {
        "trip_id": trip_id, "load_id": load_id, "driver_id": driver, "truck_id": truck,
        "trailer_id": maybe_empty(random.choice(ref["trailers"])), "dispatch_date": now.date().isoformat(),
        "actual_distance_miles": miles, "actual_duration_hours": round(miles / random.uniform(45, 60), 1),
        "fuel_gallons_used": round(miles / mpg, 1), "average_mpg": mpg,
        "idle_time_hours": round(random.uniform(0.5, 10), 1), "trip_status": "Completed",
    }
    return load, trip, route, driver, truck


def make_delivery_event(trip: dict, event_type: str, facility: dict, at: datetime) -> dict:
    delay = int(random.gauss(0, 90))                      # minutes late (negative = early)
    return {
        "event_id": new_id("EVT"), "load_id": trip["load_id"], "trip_id": trip["trip_id"],
        "event_type": event_type, "facility_id": facility["facility_id"],
        "scheduled_datetime": ts(at - timedelta(minutes=delay)), "actual_datetime": ts(at),
        "detention_minutes": 0 if random.random() < 0.2 else random.randint(0, 240),
        "on_time_flag": "True" if delay <= 15 else "False",
        "location_city": facility["city"], "location_state": facility["state"],
    }


def make_fuel(trip: dict, driver: str, truck: str, route: dict, at: datetime) -> dict:
    gallons, price = round(random.uniform(40, 200), 1), round(random.uniform(3.0, 4.5), 3)
    return {
        "fuel_purchase_id": new_id("FUEL"), "trip_id": trip["trip_id"], "truck_id": truck, "driver_id": driver,
        "purchase_date": ts(at), "location_city": route["destination_city"],
        "location_state": route["destination_state"], "gallons": gallons, "price_per_gallon": price,
        "total_cost": round(gallons * price, 2), "fuel_card_number": f"FC{random.randint(100000, 999999)}",
    }


def make_incident(trip: dict, driver: str, truck: str, route: dict, at: datetime) -> dict:
    vehicle, cargo = round(random.uniform(0, 20000), 2), round(random.choice([0, 0, random.uniform(0, 15000)]), 2)
    return {
        "incident_id": new_id("INC"), "trip_id": trip["trip_id"], "truck_id": truck, "driver_id": driver,
        "incident_date": ts(at), "incident_type": random.choice(
            ["Moving Violation", "Accident", "DOT Violation", "Customer Complaint", "Equipment Damage"]),
        "location_city": route["destination_city"], "location_state": route["destination_state"],
        "at_fault_flag": random.choice(["True", "False"]), "injury_flag": "True" if random.random() < 0.1 else "False",
        "vehicle_damage_cost": vehicle, "cargo_damage_cost": cargo, "claim_amount": round(vehicle + cargo, 2),
        "preventable_flag": random.choice(["True", "False"]), "description": "Streamed incident",
    }


def make_maintenance(ref: dict, at: datetime) -> dict:
    kind = random.choice(["Tire", "Repair", "Engine", "Brake", "Preventive", "Inspection", "Transmission"])
    labor_hours = round(random.uniform(0.5, 12), 1)
    labor, parts = round(labor_hours * random.uniform(80, 120), 2), round(random.uniform(0, 4000), 2)
    return {
        "maintenance_id": new_id("MAINT"), "truck_id": random.choice(ref["trucks"]),
        "maintenance_date": at.date().isoformat(), "maintenance_type": kind,
        "odometer_reading": random.randint(50000, 900000), "labor_hours": labor_hours,
        "labor_cost": labor, "parts_cost": parts, "total_cost": round(labor + parts, 2),
        "facility_location": random.choice(ref["facilities"])["city"],
        "downtime_hours": round(random.uniform(0, 48), 1), "service_description": f"Streamed {kind}",
    }


# --------------------------------------------------------------------- main

BUSINESS_KEY = {"raw.loads": "load_id", "raw.trips": "trip_id", "raw.delivery_events": "event_id",
                "raw.fuel_purchases": "fuel_purchase_id", "raw.maintenance_records": "maintenance_id",
                "raw.safety_incidents": "incident_id"}


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=30).topics
    missing = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS if t not in existing]
    for future in admin.create_topics(missing).values() if missing else []:
        future.result()

    ref = load_reference()
    print(f"reference data: {len(ref['drivers'])} drivers, {len(ref['trucks'])} trucks, "
          f"{len(ref['routes'])} routes, {len(ref['facilities'])} facilities", flush=True)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    pending = []            # heap of (due_epoch, n, topic, row): child events published later
    sent = {t: 0 for t in TOPICS}

    def request_stop(*_):          # docker stop sends SIGTERM: flush and exit cleanly
        global _stop
        _stop = True
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def publish(topic: str, row: dict):
        producer.produce(topic, key=row[BUSINESS_KEY[topic]], value=json.dumps(row))
        sent[topic] += 1

    n, next_trip, last_report = 0, time.time(), time.time()
    while not _stop:
        now_epoch = time.time()
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        if now_epoch >= next_trip:
            load, trip, route, driver, truck = make_trip(ref, now)
            publish("raw.loads", load)
            publish("raw.trips", trip)
            pickup, delivery = random.sample(ref["facilities"], 2)
            children = [(random.uniform(2, 5), "raw.delivery_events", ("Pickup", pickup)),
                        (random.uniform(8, 20), "raw.delivery_events", ("Delivery", delivery))]
            children += [(random.uniform(3, 15), "raw.fuel_purchases", None) for _ in range(random.randint(1, 2))]
            if random.random() < 0.005:
                children.append((random.uniform(5, 15), "raw.safety_incidents", None))
            for delay, topic, extra in children:
                n += 1
                heapq.heappush(pending, (now_epoch + delay, n, topic, (trip, driver, truck, route, extra)))
            next_trip += random.expovariate(TRIPS_PER_SECOND)   # Poisson arrivals

        if random.random() < MAINTENANCE_PER_MINUTE / 60 / 10:  # loop runs ~10x per second
            publish("raw.maintenance_records", make_maintenance(ref, now))

        while pending and pending[0][0] <= now_epoch:
            _, _, topic, (trip, driver, truck, route, extra) = heapq.heappop(pending)
            if topic == "raw.delivery_events":
                publish(topic, make_delivery_event(trip, extra[0], extra[1], now))
            elif topic == "raw.fuel_purchases":
                publish(topic, make_fuel(trip, driver, truck, route, now))
            else:
                publish(topic, make_incident(trip, driver, truck, route, now))

        producer.poll(0)
        if now_epoch - last_report >= 30:
            print(f"{now:%H:%M:%S} sent so far: {sent}", flush=True)
            last_report = now_epoch
        time.sleep(0.1)

    producer.flush(10)
    print(f"stopped; sent {sent}", flush=True)


if __name__ == "__main__":
    main()
