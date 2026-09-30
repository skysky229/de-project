"""Kafka message schemas, one per streamed raw table (topic raw.<table>).

Columns = the raw ClickHouse table's columns, without sys_create_date (read_raw_topic()
adds it from the Kafka record time). Dates/timestamps travel as strings
("2026-09-27" / "2026-09-27 10:15:00.123456") and are cast after parsing
(the CASTS map); from_json's own date parsing is format-sensitive.
"""
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def _schema(**columns) -> StructType:
    return StructType([StructField(name, dtype) for name, dtype in columns.items()])


S, L, D = StringType(), LongType(), DoubleType()

SCHEMAS = {
    "delivery_events": _schema(
        event_id=S, load_id=S, trip_id=S, event_type=S, facility_id=S, scheduled_datetime=S,
        actual_datetime=S, detention_minutes=L, on_time_flag=S, location_city=S, location_state=S),
    "trips": _schema(
        trip_id=S, load_id=S, driver_id=S, truck_id=S, trailer_id=S, dispatch_date=S,
        actual_distance_miles=L, actual_duration_hours=D, fuel_gallons_used=D, average_mpg=D,
        idle_time_hours=D, trip_status=S),
    "loads": _schema(
        load_id=S, customer_id=S, route_id=S, load_date=S, load_type=S, weight_lbs=L, pieces=L,
        revenue=D, fuel_surcharge=D, accessorial_charges=L, load_status=S, booking_type=S),
    "fuel_purchases": _schema(
        fuel_purchase_id=S, trip_id=S, truck_id=S, driver_id=S, purchase_date=S, location_city=S,
        location_state=S, gallons=D, price_per_gallon=D, total_cost=D, fuel_card_number=S),
    "maintenance_records": _schema(
        maintenance_id=S, truck_id=S, maintenance_date=S, maintenance_type=S, odometer_reading=L,
        labor_hours=D, labor_cost=D, parts_cost=D, total_cost=D, facility_location=S,
        downtime_hours=D, service_description=S),
    "safety_incidents": _schema(
        incident_id=S, trip_id=S, truck_id=S, driver_id=S, incident_date=S, incident_type=S,
        location_city=S, location_state=S, at_fault_flag=S, injury_flag=S, vehicle_damage_cost=D,
        cargo_damage_cost=D, claim_amount=D, preventable_flag=S, description=S),
}

# string columns to cast after parsing, per table
CASTS = {
    "delivery_events": {"scheduled_datetime": "timestamp", "actual_datetime": "timestamp"},
    "trips": {"dispatch_date": "date"},
    "loads": {"load_date": "date"},
    "fuel_purchases": {"purchase_date": "timestamp"},
    "maintenance_records": {"maintenance_date": "date"},
    "safety_incidents": {"incident_date": "timestamp"},
}
