"""What "good data" means for each fact and dimension: the single source of rules used by
both DQ runners. dq_batch.py applies them to dwh.<fact> (rows of the window just loaded),
dq_streaming.py to dwh.<fact>_streaming (rows of the last hour), so a rule is written once.

Severity convention:
    error  the pipeline produced something wrong or unusable (missing ids, broken keys,
           negative money, a total our transform computes that doesn't add up): fails the task
    warn   the *source* looks suspicious (implausible values, unknown categories, empty
           foreign ids, source totals that don't add up): recorded, the run carries on

Ranges were set from the bootstrap CSVs (dataset/) and the generator (docker/streamgen) with
generous margins; tighten them as the data is better understood.
"""

from dq_framework import (
    Check,
    accepted_values,
    in_range,
    key_mapping,
    non_negative,
    not_empty,
    relationship,
    rule,
    unknown_rate,
)

# share of rows allowed to point at the Unknown member (-1): ~2% of the source trips and fuel
# purchases have an empty driver/truck/trailer id
MAX_UNKNOWN_RATE = 0.05

# dimension -> (surrogate key, natural id, raw table in MinIO)
DIMS = {
    "dim_driver": ("driver_key", "driver_id", "drivers"),
    "dim_truck": ("truck_key", "truck_id", "trucks"),
    "dim_trailer": ("trailer_key", "trailer_id", "trailers"),
    "dim_customer": ("customer_key", "customer_id", "customers"),
    "dim_facility": ("facility_key", "facility_id", "facilities"),
    "dim_route": ("route_key", "route_id", "routes"),
}

FACTS = {
    "fact_trip": dict(
        id="trip_id", key="trip_key", raw="trips", streamed=True,
        fks={"driver_key": "dim_driver", "truck_key": "dim_truck", "trailer_key": "dim_trailer",
             "customer_key": "dim_customer", "route_key": "dim_route"},
        date_keys=["date_key"],
        non_negative=["weight_lbs", "pieces", "actual_miles", "trip_duration_hours", "revenue",
                      "fuel_surcharge", "accessorial_charges", "total_revenue",
                      "fuel_consumed_gallons", "idle_hours"],
        ranges={"weight_lbs": (1, 80000), "pieces": (1, 1000), "actual_miles": (1, 5000),
                "trip_duration_hours": (0.1, 200), "revenue": (1, 50000),
                "fuel_consumed_gallons": (0.1, 2000), "idle_hours": (0, 48)},
        accepted={"load_type": ["Dry Van", "Refrigerated"],
                  "booking_type": ["Spot", "Dedicated", "Contract"],
                  "load_status": ["Completed"]},
        rules=[
            ("total_revenue_is_sum",
             "abs(ifNull(total_revenue, 0) - (ifNull(revenue, 0) + ifNull(fuel_surcharge, 0)"
             " + ifNull(accessorial_charges, 0))) <= 0.02",
             "error", "total_revenue = revenue + fuel_surcharge + accessorial_charges (computed by the ETL)"),
            ("mpg_plausible",
             "ifNull(fuel_consumed_gallons, 0) = 0 OR actual_miles / fuel_consumed_gallons BETWEEN 3 AND 12",
             "warn", "actual_miles / fuel_consumed_gallons between 3 and 12 mpg"),
            ("actual_vs_planned_miles",
             "planned_miles IS NULL OR planned_miles = 0 OR actual_miles BETWEEN planned_miles * 0.5 AND planned_miles * 2",
             "warn", "actual_miles within 0.5x..2x of the route's planned_miles"),
        ],
        compare=["date_key", "driver_key", "truck_key", "trailer_key", "customer_key", "route_key",
                 "actual_miles", "total_revenue", "fuel_consumed_gallons"],
    ),
    "fact_delivery_event": dict(
        id="event_id", key="event_key", raw="delivery_events", streamed=True,
        fks={"facility_key": "dim_facility"},
        # an early event's scheduled date can be the next day, which dim_date only gets tomorrow
        date_keys=["date_key", "scheduled_date_key", "actual_date_key"],
        soft_date_keys=["scheduled_date_key"],
        trip_child=True,
        non_negative=["detention_minutes"],
        ranges={"detention_minutes": (0, 1440), "delay_minutes": (-10080, 10080)},
        accepted={"event_type": ["Pickup", "Delivery"]},
        rules=[
            ("on_time_count_consistent", "on_time_count = is_on_time AND event_count = 1", "error",
             "on_time_count = is_on_time and event_count = 1 (computed by the ETL)"),
        ],
        compare=["date_key", "facility_key", "trip_key", "event_type", "delay_minutes", "is_on_time"],
    ),
    "fact_fuel_purchase": dict(
        id="fuel_purchase_id", key="fuel_purchase_key", raw="fuel_purchases", streamed=True,
        fks={"driver_key": "dim_driver", "truck_key": "dim_truck"},
        date_keys=["date_key"],
        trip_child=True,
        non_negative=["gallons", "price_per_gallon", "total_cost"],
        ranges={"gallons": (1, 400), "price_per_gallon": (1, 10), "total_cost": (1, 4000)},
        accepted={},
        rules=[
            ("total_cost_is_gallons_x_price",
             "gallons IS NULL OR price_per_gallon IS NULL OR total_cost IS NULL"
             " OR abs(total_cost - gallons * price_per_gallon) <= 0.05",
             "warn", "total_cost = gallons * price_per_gallon (+-0.05)"),
        ],
        compare=["date_key", "driver_key", "truck_key", "trip_key", "gallons", "total_cost"],
    ),
    "fact_maintenance": dict(
        id="maintenance_id", key="maintenance_key", raw="maintenance_records", streamed=True,
        fks={"truck_key": "dim_truck"},
        date_keys=["date_key"],
        non_negative=["odometer_reading", "labor_hours", "labor_cost", "parts_cost", "total_cost",
                      "downtime_hours"],
        ranges={"labor_hours": (0, 24), "downtime_hours": (0, 720), "odometer_reading": (0, 2000000),
                "total_cost": (0, 50000)},
        accepted={"maintenance_type": ["Tire", "Repair", "Engine", "Brake", "Preventive",
                                       "Inspection", "Transmission"]},
        rules=[
            ("total_cost_is_labor_plus_parts",
             "total_cost IS NULL OR abs(total_cost - (ifNull(labor_cost, 0) + ifNull(parts_cost, 0))) <= 0.02",
             "warn", "total_cost = labor_cost + parts_cost (+-0.02)"),
        ],
        compare=["date_key", "truck_key", "total_cost", "downtime_hours"],
    ),
    "fact_safety_incident": dict(
        id="incident_id", key="incident_key", raw="safety_incidents", streamed=True,
        fks={"driver_key": "dim_driver", "truck_key": "dim_truck"},
        date_keys=["date_key"],
        trip_child=True,
        non_negative=["vehicle_damage_cost", "cargo_damage_cost", "claim_amount"],
        ranges={"claim_amount": (0, 500000)},
        accepted={"incident_type": ["Moving Violation", "Accident", "DOT Violation",
                                    "Customer Complaint", "Equipment Damage"]},
        rules=[
            ("flag_counts_consistent",
             "incident_count = 1 AND injury_count = injury_flag AND preventable_count = preventable_flag",
             "error", "incident/injury/preventable counts match their flags (computed by the ETL)"),
        ],
        compare=["date_key", "driver_key", "truck_key", "trip_key", "claim_amount"],
    ),
    "fact_driver_monthly": dict(
        id=None, key=None, raw=None, streamed=False,
        fks={"driver_key": "dim_driver"},
        date_keys=["month_date_key"],
        non_negative=["trips_completed", "total_miles", "total_revenue", "total_fuel_gallons",
                      "average_idle_hours"],
        ranges={"on_time_delivery_rate": (0, 1), "average_mpg": (3, 12)},
        accepted={},
        rules=[
            ("on_time_rate_is_ratio", "on_time_delivery_rate IS NULL OR on_time_delivery_rate BETWEEN 0 AND 1",
             "error", "on_time_delivery_rate is a ratio in [0, 1]"),
        ],
        compare=[],
    ),
    "fact_truck_monthly": dict(
        id=None, key=None, raw=None, streamed=False,
        fks={"truck_key": "dim_truck"},
        date_keys=["month_date_key"],
        non_negative=["trips_completed", "total_miles", "total_revenue", "maintenance_events",
                      "maintenance_cost", "downtime_hours", "utilization_rate"],
        ranges={"utilization_rate": (0, 2), "average_mpg": (3, 12)},
        accepted={},
        rules=[],
        compare=[],
    ),
}


def fact_row_checks(db: str, fact: str, table: str, where: str, trip_table: str,
                    check_dates: bool = True) -> list:
    """Row-level checks of one fact, on `db.table` rows matching `where`.

    table       fact (batch) or fact_streaming (streaming); `fact` names the rules
    trip_table  where child facts look up their trip_key (fact_trip / fact_trip_streaming)
    check_dates check date keys against dim_date (off for streaming: dim_date only gets today's
                date when the next daily run loads it)
    """
    spec = FACTS[fact]
    src = f"{db}.{table} FINAL WHERE {where}"
    checks = []
    if spec["id"]:
        checks.append(not_empty(table, src, spec["id"]))
    for date_col in spec["date_keys"]:
        checks.append(rule(table, src, f"{date_col}_valid", f"{date_col} > 19000101",
                           description=f"{date_col} is a real yyyyMMdd date"))
        if check_dates:
            severity = "warn" if date_col in spec.get("soft_date_keys", []) else "error"
            checks.append(relationship(table, src, date_col, db, "dim_date", "date_key", severity))
    for fk, dim in spec["fks"].items():
        checks.append(relationship(table, src, fk, db, dim, DIMS[dim][0]))
        checks.append(unknown_rate(table, src, fk, MAX_UNKNOWN_RATE))
    if spec.get("trip_child"):
        checks.append(relationship(table, src, "trip_key", db, trip_table, "trip_key",
                                   severity="warn", max_fail_rate=0.01))
    checks += [non_negative(table, src, c) for c in spec["non_negative"]]
    checks += [in_range(table, src, c, lo, hi) for c, (lo, hi) in spec["ranges"].items()]
    checks += [accepted_values(table, src, c, v) for c, v in spec["accepted"].items()]
    checks += [rule(table, src, n, cond, sev, desc) for n, cond, sev, desc in spec["rules"]]
    return checks


def dim_checks(db: str, dim: str) -> list:
    """Whole-table checks of a (small) dimension."""
    key, natural_id, _ = DIMS[dim]
    src = f"{db}.{dim} FINAL WHERE 1"
    return [
        not_empty(dim, src, natural_id),
        key_mapping(dim, db, key, natural_id),
        Check(
            dim, "unknown_member_present", "rule",
            f"SELECT count() = 0, 1 FROM {db}.{dim} WHERE {key} = -1",
            description="the Unknown member (key -1) exists, so facts with an empty id still join",
        ),
    ]
