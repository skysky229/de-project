# Data-quality test results — 2026-10-17

Full-pipeline test on Docker. Generator ran ~16 min at 1 trip/s with 1% deliberately broken messages, then was stopped.

- **Batch** = task `dq.dq_batch` of `logistics_dwh_daily` for 2026-10-17 (run `batch-20261017T072441-79a165`); checks rows loaded that day.
- **Streaming** = DAG `logistics_streaming_dq` (last run); checks `dwh.<fact>_streaming` rows of the last hour and `dwh.dq_dead_letter`.
- `failed / total` = rows breaking the rule / rows checked (freshness: 1 = too old). Severity **error** fails the task, **warn** is only recorded.

## Summary

- **batch**: 169 checks — 166 pass, 3 fail
- **streaming**: 102 checks — 6 fail, 96 pass

Failures:

- batch · `fact_delivery_event.trip_key_in_fact_trip` (warn): 35/1727 — trip_key references an existing fact_trip.trip_key
- batch · `fact_fuel_purchase.trip_key_in_fact_trip` (warn): 23/1303 — trip_key references an existing fact_trip.trip_key
- batch · `fact_trip.reconciles_with_raw_trips` (error): 9/875 — every trips.trip_id recorded in the window is in fact_trip
- streaming · `dq_dead_letter.dead_letter_any` (warn): 26/2973 — no Kafka message was rejected
- streaming · `fact_delivery_event_streaming.latency_p95` (warn): 557/1059 — 95% of rows reach dwh within 180 s of arriving in Kafka
- streaming · `fact_delivery_event_streaming.trip_key_in_fact_trip_streaming` (warn): 16/1059 — trip_key references an existing fact_trip_streaming.trip_key
- streaming · `fact_fuel_purchase_streaming.latency_p95` (warn): 387/794 — 95% of rows reach dwh within 180 s of arriving in Kafka
- streaming · `fact_maintenance_streaming.latency_p95` (warn): 14/24 — 95% of rows reach dwh within 180 s of arriving in Kafka
- streaming · `fact_trip_streaming.latency_p95` (warn): 434/534 — 95% of rows reach dwh within 180 s of arriving in Kafka

Why they failed: 9 loads were dead-lettered (broken messages injected on purpose), so their 9 trips were dropped by the `trips ⋈ loads` join (reconciliation, error), and those trips' delivery events / fuel purchases point at missing trips (warn). Streaming latency warnings come from the ~10 min catch-up after the app started; dead-letter warning is the injected 1%.

## Batch checks per table

### `dim_customer` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `customer_key_unique_per_customer_id` | key_mapping | error | ✅ pass | 0 / 201 | customer_key is unique per customer_id (no surrogate-key collision) |
| `customer_id_not_empty` | not_empty | error | ✅ pass | 0 / 201 | customer_id is never NULL or empty |
| `reconciles_with_raw_customers` | reconciliation | error | ✅ pass | 0 / 0 | every customers.customer_id recorded in the window is in dim_customer |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `dim_driver` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `driver_key_unique_per_driver_id` | key_mapping | error | ✅ pass | 0 / 151 | driver_key is unique per driver_id (no surrogate-key collision) |
| `driver_id_not_empty` | not_empty | error | ✅ pass | 0 / 151 | driver_id is never NULL or empty |
| `reconciles_with_raw_drivers` | reconciliation | error | ✅ pass | 0 / 0 | every drivers.driver_id recorded in the window is in dim_driver |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `dim_facility` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `facility_key_unique_per_facility_id` | key_mapping | error | ✅ pass | 0 / 51 | facility_key is unique per facility_id (no surrogate-key collision) |
| `facility_id_not_empty` | not_empty | error | ✅ pass | 0 / 51 | facility_id is never NULL or empty |
| `reconciles_with_raw_facilities` | reconciliation | error | ✅ pass | 0 / 0 | every facilities.facility_id recorded in the window is in dim_facility |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `dim_route` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `route_key_unique_per_route_id` | key_mapping | error | ✅ pass | 0 / 59 | route_key is unique per route_id (no surrogate-key collision) |
| `route_id_not_empty` | not_empty | error | ✅ pass | 0 / 59 | route_id is never NULL or empty |
| `reconciles_with_raw_routes` | reconciliation | error | ✅ pass | 0 / 0 | every routes.route_id recorded in the window is in dim_route |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `dim_trailer` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `trailer_key_unique_per_trailer_id` | key_mapping | error | ✅ pass | 0 / 181 | trailer_key is unique per trailer_id (no surrogate-key collision) |
| `trailer_id_not_empty` | not_empty | error | ✅ pass | 0 / 181 | trailer_id is never NULL or empty |
| `reconciles_with_raw_trailers` | reconciliation | error | ✅ pass | 0 / 0 | every trailers.trailer_id recorded in the window is in dim_trailer |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `dim_truck` (4 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `truck_key_unique_per_truck_id` | key_mapping | error | ✅ pass | 0 / 121 | truck_key is unique per truck_id (no surrogate-key collision) |
| `truck_id_not_empty` | not_empty | error | ✅ pass | 0 / 121 | truck_id is never NULL or empty |
| `reconciles_with_raw_trucks` | reconciliation | error | ✅ pass | 0 / 0 | every trucks.truck_id recorded in the window is in dim_truck |
| `unknown_member_present` | rule | error | ✅ pass | 0 / 1 | the Unknown member (key -1) exists, so facts with an empty id still join |

### `fact_delivery_event` (20 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `event_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 1727 | event_type in (\'Pickup\', \'Delivery\') |
| `matches_streaming` | consistency | warn | ✅ pass | 0 / 1727 | every fact_delivery_event_streaming row of the day is in fact_delivery_event with the same date_key, facility_key, trip_key, event_type, delay_minutes, is_on_time |
| `present_in_streaming` | consistency | warn | ✅ pass | 0 / 1727 | every fact_delivery_event row of the day was also streamed to fact_delivery_event_streaming |
| `event_key_unique_per_event_id` | key_mapping | error | ✅ pass | 0 / 172654 | event_key is unique per event_id (no surrogate-key collision) |
| `detention_minutes_non_negative` | non_negative | error | ✅ pass | 0 / 1727 | detention_minutes >= 0 |
| `event_id_not_empty` | not_empty | error | ✅ pass | 0 / 1727 | event_id is never NULL or empty |
| `delay_minutes_in_range` | range | warn | ✅ pass | 0 / 1727 | delay_minutes between -10080 and 10080 |
| `detention_minutes_in_range` | range | warn | ✅ pass | 0 / 1727 | detention_minutes between 0 and 1440 |
| `reconciles_with_raw_delivery_events` | reconciliation | error | ✅ pass | 0 / 1727 | every delivery_events.event_id recorded in the window is in fact_delivery_event |
| `actual_date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 1727 | actual_date_key references an existing dim_date.date_key |
| `date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 1727 | date_key references an existing dim_date.date_key |
| `facility_key_in_dim_facility` | relationship | error | ✅ pass | 0 / 1727 | facility_key references an existing dim_facility.facility_key |
| `scheduled_date_key_in_dim_date` | relationship | warn | ✅ pass | 0 / 1727 | scheduled_date_key references an existing dim_date.date_key |
| `trip_key_in_fact_trip` | relationship | warn | ❌ fail | 35 / 1727 | trip_key references an existing fact_trip.trip_key |
| `actual_date_key_valid` | rule | error | ✅ pass | 0 / 1727 | actual_date_key is a real yyyyMMdd date |
| `date_key_valid` | rule | error | ✅ pass | 0 / 1727 | date_key is a real yyyyMMdd date |
| `on_time_count_consistent` | rule | error | ✅ pass | 0 / 1727 | on_time_count = is_on_time and event_count = 1 (computed by the ETL) |
| `scheduled_date_key_valid` | rule | error | ✅ pass | 0 / 1727 | scheduled_date_key is a real yyyyMMdd date |
| `facility_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 1727 | at most 5% of rows have facility_key = -1 (Unknown) |
| `daily_volume` | volume | warn | ✅ pass | 0 / 1 | the day has rows, within 50% of the previous 7 days\' average |

### `fact_driver_monthly` (12 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `average_idle_hours_non_negative` | non_negative | error | ✅ pass | 0 / 0 | average_idle_hours >= 0 |
| `total_fuel_gallons_non_negative` | non_negative | error | ✅ pass | 0 / 0 | total_fuel_gallons >= 0 |
| `total_miles_non_negative` | non_negative | error | ✅ pass | 0 / 0 | total_miles >= 0 |
| `total_revenue_non_negative` | non_negative | error | ✅ pass | 0 / 0 | total_revenue >= 0 |
| `trips_completed_non_negative` | non_negative | error | ✅ pass | 0 / 0 | trips_completed >= 0 |
| `average_mpg_in_range` | range | warn | ✅ pass | 0 / 0 | average_mpg between 3 and 12 |
| `on_time_delivery_rate_in_range` | range | warn | ✅ pass | 0 / 0 | on_time_delivery_rate between 0 and 1 |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 0 | driver_key references an existing dim_driver.driver_key |
| `month_date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 0 | month_date_key references an existing dim_date.date_key |
| `month_date_key_valid` | rule | error | ✅ pass | 0 / 0 | month_date_key is a real yyyyMMdd date |
| `on_time_rate_is_ratio` | rule | error | ✅ pass | 0 / 0 | on_time_delivery_rate is a ratio in [0, 1] |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 0 | at most 5% of rows have driver_key = -1 (Unknown) |

### `fact_fuel_purchase` (20 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `matches_streaming` | consistency | warn | ✅ pass | 0 / 1303 | every fact_fuel_purchase_streaming row of the day is in fact_fuel_purchase with the same date_key, driver_key, truck_key, trip_key, gallons, total_cost |
| `present_in_streaming` | consistency | warn | ✅ pass | 0 / 1303 | every fact_fuel_purchase row of the day was also streamed to fact_fuel_purchase_streaming |
| `fuel_purchase_key_unique_per_fuel_purchase_id` | key_mapping | error | ✅ pass | 0 / 197745 | fuel_purchase_key is unique per fuel_purchase_id (no surrogate-key collision) |
| `gallons_non_negative` | non_negative | error | ✅ pass | 0 / 1303 | gallons >= 0 |
| `price_per_gallon_non_negative` | non_negative | error | ✅ pass | 0 / 1303 | price_per_gallon >= 0 |
| `total_cost_non_negative` | non_negative | error | ✅ pass | 0 / 1303 | total_cost >= 0 |
| `fuel_purchase_id_not_empty` | not_empty | error | ✅ pass | 0 / 1303 | fuel_purchase_id is never NULL or empty |
| `gallons_in_range` | range | warn | ✅ pass | 0 / 1303 | gallons between 1 and 400 |
| `price_per_gallon_in_range` | range | warn | ✅ pass | 0 / 1303 | price_per_gallon between 1 and 10 |
| `total_cost_in_range` | range | warn | ✅ pass | 0 / 1303 | total_cost between 1 and 4000 |
| `reconciles_with_raw_fuel_purchases` | reconciliation | error | ✅ pass | 0 / 1303 | every fuel_purchases.fuel_purchase_id recorded in the window is in fact_fuel_purchase |
| `date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 1303 | date_key references an existing dim_date.date_key |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 1303 | driver_key references an existing dim_driver.driver_key |
| `trip_key_in_fact_trip` | relationship | warn | ❌ fail | 23 / 1303 | trip_key references an existing fact_trip.trip_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 1303 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 1303 | date_key is a real yyyyMMdd date |
| `total_cost_is_gallons_x_price` | rule | warn | ✅ pass | 0 / 1303 | total_cost = gallons * price_per_gallon (+-0.05) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 36 / 1303 | at most 5% of rows have driver_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 38 / 1303 | at most 5% of rows have truck_key = -1 (Unknown) |
| `daily_volume` | volume | warn | ✅ pass | 0 / 1 | the day has rows, within 50% of the previous 7 days\' average |

### `fact_maintenance` (21 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `maintenance_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 38 | maintenance_type in (\'Tire\', \'Repair\', \'Engine\', \'Brake\', \'Preventive\', \'Inspection\', \'Transmission\') |
| `matches_streaming` | consistency | warn | ✅ pass | 0 / 38 | every fact_maintenance_streaming row of the day is in fact_maintenance with the same date_key, truck_key, total_cost, downtime_hours |
| `present_in_streaming` | consistency | warn | ✅ pass | 0 / 38 | every fact_maintenance row of the day was also streamed to fact_maintenance_streaming |
| `maintenance_key_unique_per_maintenance_id` | key_mapping | error | ✅ pass | 0 / 2958 | maintenance_key is unique per maintenance_id (no surrogate-key collision) |
| `downtime_hours_non_negative` | non_negative | error | ✅ pass | 0 / 38 | downtime_hours >= 0 |
| `labor_cost_non_negative` | non_negative | error | ✅ pass | 0 / 38 | labor_cost >= 0 |
| `labor_hours_non_negative` | non_negative | error | ✅ pass | 0 / 38 | labor_hours >= 0 |
| `odometer_reading_non_negative` | non_negative | error | ✅ pass | 0 / 38 | odometer_reading >= 0 |
| `parts_cost_non_negative` | non_negative | error | ✅ pass | 0 / 38 | parts_cost >= 0 |
| `total_cost_non_negative` | non_negative | error | ✅ pass | 0 / 38 | total_cost >= 0 |
| `maintenance_id_not_empty` | not_empty | error | ✅ pass | 0 / 38 | maintenance_id is never NULL or empty |
| `downtime_hours_in_range` | range | warn | ✅ pass | 0 / 38 | downtime_hours between 0 and 720 |
| `labor_hours_in_range` | range | warn | ✅ pass | 0 / 38 | labor_hours between 0 and 24 |
| `odometer_reading_in_range` | range | warn | ✅ pass | 0 / 38 | odometer_reading between 0 and 2000000 |
| `total_cost_in_range` | range | warn | ✅ pass | 0 / 38 | total_cost between 0 and 50000 |
| `reconciles_with_raw_maintenance_records` | reconciliation | error | ✅ pass | 0 / 38 | every maintenance_records.maintenance_id recorded in the window is in fact_maintenance |
| `date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 38 | date_key references an existing dim_date.date_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 38 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 38 | date_key is a real yyyyMMdd date |
| `total_cost_is_labor_plus_parts` | rule | warn | ✅ pass | 0 / 38 | total_cost = labor_cost + parts_cost (+-0.02) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 38 | at most 5% of rows have truck_key = -1 (Unknown) |

### `fact_safety_incident` (18 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `incident_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 4 | incident_type in (\'Moving Violation\', \'Accident\', \'DOT Violation\', \'Customer Complaint\', \'Equipment Damage\') |
| `matches_streaming` | consistency | warn | ✅ pass | 0 / 4 | every fact_safety_incident_streaming row of the day is in fact_safety_incident with the same date_key, driver_key, truck_key, trip_key, claim_amount |
| `present_in_streaming` | consistency | warn | ✅ pass | 0 / 4 | every fact_safety_incident row of the day was also streamed to fact_safety_incident_streaming |
| `incident_key_unique_per_incident_id` | key_mapping | error | ✅ pass | 0 / 174 | incident_key is unique per incident_id (no surrogate-key collision) |
| `cargo_damage_cost_non_negative` | non_negative | error | ✅ pass | 0 / 4 | cargo_damage_cost >= 0 |
| `claim_amount_non_negative` | non_negative | error | ✅ pass | 0 / 4 | claim_amount >= 0 |
| `vehicle_damage_cost_non_negative` | non_negative | error | ✅ pass | 0 / 4 | vehicle_damage_cost >= 0 |
| `incident_id_not_empty` | not_empty | error | ✅ pass | 0 / 4 | incident_id is never NULL or empty |
| `claim_amount_in_range` | range | warn | ✅ pass | 0 / 4 | claim_amount between 0 and 500000 |
| `reconciles_with_raw_safety_incidents` | reconciliation | error | ✅ pass | 0 / 4 | every safety_incidents.incident_id recorded in the window is in fact_safety_incident |
| `date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 4 | date_key references an existing dim_date.date_key |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 4 | driver_key references an existing dim_driver.driver_key |
| `trip_key_in_fact_trip` | relationship | warn | ✅ pass | 0 / 4 | trip_key references an existing fact_trip.trip_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 4 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 4 | date_key is a real yyyyMMdd date |
| `flag_counts_consistent` | rule | error | ✅ pass | 0 / 4 | incident/injury/preventable counts match their flags (computed by the ETL) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 4 | at most 5% of rows have driver_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 4 | at most 5% of rows have truck_key = -1 (Unknown) |

### `fact_trip` (41 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `booking_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 866 | booking_type in (\'Spot\', \'Dedicated\', \'Contract\') |
| `load_status_accepted_values` | accepted_values | warn | ✅ pass | 0 / 866 | load_status in (\'Completed\') |
| `load_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 866 | load_type in (\'Dry Van\', \'Refrigerated\') |
| `matches_streaming` | consistency | warn | ✅ pass | 0 / 866 | every fact_trip_streaming row of the day is in fact_trip with the same date_key, driver_key, truck_key, trailer_key, customer_key, route_key, actual_miles, total_revenue, fuel_consumed_gallons |
| `present_in_streaming` | consistency | warn | ✅ pass | 0 / 866 | every fact_trip row of the day was also streamed to fact_trip_streaming |
| `trip_key_unique_per_trip_id` | key_mapping | error | ✅ pass | 0 / 86342 | trip_key is unique per trip_id (no surrogate-key collision) |
| `accessorial_charges_non_negative` | non_negative | error | ✅ pass | 0 / 866 | accessorial_charges >= 0 |
| `actual_miles_non_negative` | non_negative | error | ✅ pass | 0 / 866 | actual_miles >= 0 |
| `fuel_consumed_gallons_non_negative` | non_negative | error | ✅ pass | 0 / 866 | fuel_consumed_gallons >= 0 |
| `fuel_surcharge_non_negative` | non_negative | error | ✅ pass | 0 / 866 | fuel_surcharge >= 0 |
| `idle_hours_non_negative` | non_negative | error | ✅ pass | 0 / 866 | idle_hours >= 0 |
| `pieces_non_negative` | non_negative | error | ✅ pass | 0 / 866 | pieces >= 0 |
| `revenue_non_negative` | non_negative | error | ✅ pass | 0 / 866 | revenue >= 0 |
| `total_revenue_non_negative` | non_negative | error | ✅ pass | 0 / 866 | total_revenue >= 0 |
| `trip_duration_hours_non_negative` | non_negative | error | ✅ pass | 0 / 866 | trip_duration_hours >= 0 |
| `weight_lbs_non_negative` | non_negative | error | ✅ pass | 0 / 866 | weight_lbs >= 0 |
| `trip_id_not_empty` | not_empty | error | ✅ pass | 0 / 866 | trip_id is never NULL or empty |
| `actual_miles_in_range` | range | warn | ✅ pass | 0 / 866 | actual_miles between 1 and 5000 |
| `fuel_consumed_gallons_in_range` | range | warn | ✅ pass | 0 / 866 | fuel_consumed_gallons between 0.1 and 2000 |
| `idle_hours_in_range` | range | warn | ✅ pass | 0 / 866 | idle_hours between 0 and 48 |
| `pieces_in_range` | range | warn | ✅ pass | 0 / 866 | pieces between 1 and 1000 |
| `revenue_in_range` | range | warn | ✅ pass | 0 / 866 | revenue between 1 and 50000 |
| `trip_duration_hours_in_range` | range | warn | ✅ pass | 0 / 866 | trip_duration_hours between 0.1 and 200 |
| `weight_lbs_in_range` | range | warn | ✅ pass | 0 / 866 | weight_lbs between 1 and 80000 |
| `reconciles_with_raw_trips` | reconciliation | error | ❌ fail | 9 / 875 | every trips.trip_id recorded in the window is in fact_trip |
| `customer_key_in_dim_customer` | relationship | error | ✅ pass | 0 / 866 | customer_key references an existing dim_customer.customer_key |
| `date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 866 | date_key references an existing dim_date.date_key |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 866 | driver_key references an existing dim_driver.driver_key |
| `route_key_in_dim_route` | relationship | error | ✅ pass | 0 / 866 | route_key references an existing dim_route.route_key |
| `trailer_key_in_dim_trailer` | relationship | error | ✅ pass | 0 / 866 | trailer_key references an existing dim_trailer.trailer_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 866 | truck_key references an existing dim_truck.truck_key |
| `actual_vs_planned_miles` | rule | warn | ✅ pass | 0 / 866 | actual_miles within 0.5x..2x of the route\'s planned_miles |
| `date_key_valid` | rule | error | ✅ pass | 0 / 866 | date_key is a real yyyyMMdd date |
| `mpg_plausible` | rule | warn | ✅ pass | 0 / 866 | actual_miles / fuel_consumed_gallons between 3 and 12 mpg |
| `total_revenue_is_sum` | rule | error | ✅ pass | 0 / 866 | total_revenue = revenue + fuel_surcharge + accessorial_charges (computed by the ETL) |
| `customer_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 866 | at most 5% of rows have customer_key = -1 (Unknown) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 22 / 866 | at most 5% of rows have driver_key = -1 (Unknown) |
| `route_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 866 | at most 5% of rows have route_key = -1 (Unknown) |
| `trailer_key_unknown_rate` | unknown_rate | warn | ✅ pass | 21 / 866 | at most 5% of rows have trailer_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 23 / 866 | at most 5% of rows have truck_key = -1 (Unknown) |
| `daily_volume` | volume | warn | ✅ pass | 0 / 1 | the day has rows, within 50% of the previous 7 days\' average |

### `fact_truck_monthly` (13 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `downtime_hours_non_negative` | non_negative | error | ✅ pass | 0 / 0 | downtime_hours >= 0 |
| `maintenance_cost_non_negative` | non_negative | error | ✅ pass | 0 / 0 | maintenance_cost >= 0 |
| `maintenance_events_non_negative` | non_negative | error | ✅ pass | 0 / 0 | maintenance_events >= 0 |
| `total_miles_non_negative` | non_negative | error | ✅ pass | 0 / 0 | total_miles >= 0 |
| `total_revenue_non_negative` | non_negative | error | ✅ pass | 0 / 0 | total_revenue >= 0 |
| `trips_completed_non_negative` | non_negative | error | ✅ pass | 0 / 0 | trips_completed >= 0 |
| `utilization_rate_non_negative` | non_negative | error | ✅ pass | 0 / 0 | utilization_rate >= 0 |
| `average_mpg_in_range` | range | warn | ✅ pass | 0 / 0 | average_mpg between 3 and 12 |
| `utilization_rate_in_range` | range | warn | ✅ pass | 0 / 0 | utilization_rate between 0 and 2 |
| `month_date_key_in_dim_date` | relationship | error | ✅ pass | 0 / 0 | month_date_key references an existing dim_date.date_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 0 | truck_key references an existing dim_truck.truck_key |
| `month_date_key_valid` | rule | error | ✅ pass | 0 / 0 | month_date_key is a real yyyyMMdd date |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 0 | at most 5% of rows have truck_key = -1 (Unknown) |

## Streaming checks per table

### `dq_dead_letter` (2 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `dead_letter_any` | dead_letter | warn | ❌ fail | 26 / 2973 | no Kafka message was rejected |
| `dead_letter_rate` | dead_letter | error | ✅ pass | 26 / 2973 | at most 1% of Kafka messages are rejected |

### `fact_delivery_event_streaming` (14 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `event_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 1059 | event_type in (\'Pickup\', \'Delivery\') |
| `freshness` | freshness | warn | ✅ pass | 0 / 1 | newest row is at most 300 s old (streaming is alive) |
| `latency_p95` | latency | warn | ❌ fail | 557 / 1059 | 95% of rows reach dwh within 180 s of arriving in Kafka |
| `detention_minutes_non_negative` | non_negative | error | ✅ pass | 0 / 1059 | detention_minutes >= 0 |
| `event_id_not_empty` | not_empty | error | ✅ pass | 0 / 1059 | event_id is never NULL or empty |
| `delay_minutes_in_range` | range | warn | ✅ pass | 0 / 1059 | delay_minutes between -10080 and 10080 |
| `detention_minutes_in_range` | range | warn | ✅ pass | 0 / 1059 | detention_minutes between 0 and 1440 |
| `facility_key_in_dim_facility` | relationship | error | ✅ pass | 0 / 1059 | facility_key references an existing dim_facility.facility_key |
| `trip_key_in_fact_trip_streaming` | relationship | warn | ❌ fail | 16 / 1059 | trip_key references an existing fact_trip_streaming.trip_key |
| `actual_date_key_valid` | rule | error | ✅ pass | 0 / 1059 | actual_date_key is a real yyyyMMdd date |
| `date_key_valid` | rule | error | ✅ pass | 0 / 1059 | date_key is a real yyyyMMdd date |
| `on_time_count_consistent` | rule | error | ✅ pass | 0 / 1059 | on_time_count = is_on_time and event_count = 1 (computed by the ETL) |
| `scheduled_date_key_valid` | rule | error | ✅ pass | 0 / 1059 | scheduled_date_key is a real yyyyMMdd date |
| `facility_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 1059 | at most 5% of rows have facility_key = -1 (Unknown) |

### `fact_fuel_purchase_streaming` (16 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `freshness` | freshness | warn | ✅ pass | 0 / 1 | newest row is at most 300 s old (streaming is alive) |
| `latency_p95` | latency | warn | ❌ fail | 387 / 794 | 95% of rows reach dwh within 180 s of arriving in Kafka |
| `gallons_non_negative` | non_negative | error | ✅ pass | 0 / 794 | gallons >= 0 |
| `price_per_gallon_non_negative` | non_negative | error | ✅ pass | 0 / 794 | price_per_gallon >= 0 |
| `total_cost_non_negative` | non_negative | error | ✅ pass | 0 / 794 | total_cost >= 0 |
| `fuel_purchase_id_not_empty` | not_empty | error | ✅ pass | 0 / 794 | fuel_purchase_id is never NULL or empty |
| `gallons_in_range` | range | warn | ✅ pass | 0 / 794 | gallons between 1 and 400 |
| `price_per_gallon_in_range` | range | warn | ✅ pass | 0 / 794 | price_per_gallon between 1 and 10 |
| `total_cost_in_range` | range | warn | ✅ pass | 0 / 794 | total_cost between 1 and 4000 |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 794 | driver_key references an existing dim_driver.driver_key |
| `trip_key_in_fact_trip_streaming` | relationship | warn | ✅ pass | 7 / 794 | trip_key references an existing fact_trip_streaming.trip_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 794 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 794 | date_key is a real yyyyMMdd date |
| `total_cost_is_gallons_x_price` | rule | warn | ✅ pass | 0 / 794 | total_cost = gallons * price_per_gallon (+-0.05) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 17 / 794 | at most 5% of rows have driver_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 20 / 794 | at most 5% of rows have truck_key = -1 (Unknown) |

### `fact_maintenance_streaming` (18 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `maintenance_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 24 | maintenance_type in (\'Tire\', \'Repair\', \'Engine\', \'Brake\', \'Preventive\', \'Inspection\', \'Transmission\') |
| `freshness` | freshness | warn | ✅ pass | 0 / 1 | newest row is at most 900 s old (streaming is alive) |
| `latency_p95` | latency | warn | ❌ fail | 14 / 24 | 95% of rows reach dwh within 180 s of arriving in Kafka |
| `downtime_hours_non_negative` | non_negative | error | ✅ pass | 0 / 24 | downtime_hours >= 0 |
| `labor_cost_non_negative` | non_negative | error | ✅ pass | 0 / 24 | labor_cost >= 0 |
| `labor_hours_non_negative` | non_negative | error | ✅ pass | 0 / 24 | labor_hours >= 0 |
| `odometer_reading_non_negative` | non_negative | error | ✅ pass | 0 / 24 | odometer_reading >= 0 |
| `parts_cost_non_negative` | non_negative | error | ✅ pass | 0 / 24 | parts_cost >= 0 |
| `total_cost_non_negative` | non_negative | error | ✅ pass | 0 / 24 | total_cost >= 0 |
| `maintenance_id_not_empty` | not_empty | error | ✅ pass | 0 / 24 | maintenance_id is never NULL or empty |
| `downtime_hours_in_range` | range | warn | ✅ pass | 0 / 24 | downtime_hours between 0 and 720 |
| `labor_hours_in_range` | range | warn | ✅ pass | 0 / 24 | labor_hours between 0 and 24 |
| `odometer_reading_in_range` | range | warn | ✅ pass | 0 / 24 | odometer_reading between 0 and 2000000 |
| `total_cost_in_range` | range | warn | ✅ pass | 0 / 24 | total_cost between 0 and 50000 |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 24 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 24 | date_key is a real yyyyMMdd date |
| `total_cost_is_labor_plus_parts` | rule | warn | ✅ pass | 0 / 24 | total_cost = labor_cost + parts_cost (+-0.02) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 24 | at most 5% of rows have truck_key = -1 (Unknown) |

### `fact_safety_incident_streaming` (15 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `incident_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 2 | incident_type in (\'Moving Violation\', \'Accident\', \'DOT Violation\', \'Customer Complaint\', \'Equipment Damage\') |
| `freshness` | freshness | warn | ✅ pass | 0 / 1 | newest row is at most 3600 s old (streaming is alive) |
| `latency_p95` | latency | warn | ✅ pass | 0 / 2 | 95% of rows reach dwh within 180 s of arriving in Kafka |
| `cargo_damage_cost_non_negative` | non_negative | error | ✅ pass | 0 / 2 | cargo_damage_cost >= 0 |
| `claim_amount_non_negative` | non_negative | error | ✅ pass | 0 / 2 | claim_amount >= 0 |
| `vehicle_damage_cost_non_negative` | non_negative | error | ✅ pass | 0 / 2 | vehicle_damage_cost >= 0 |
| `incident_id_not_empty` | not_empty | error | ✅ pass | 0 / 2 | incident_id is never NULL or empty |
| `claim_amount_in_range` | range | warn | ✅ pass | 0 / 2 | claim_amount between 0 and 500000 |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 2 | driver_key references an existing dim_driver.driver_key |
| `trip_key_in_fact_trip_streaming` | relationship | warn | ✅ pass | 0 / 2 | trip_key references an existing fact_trip_streaming.trip_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 2 | truck_key references an existing dim_truck.truck_key |
| `date_key_valid` | rule | error | ✅ pass | 0 / 2 | date_key is a real yyyyMMdd date |
| `flag_counts_consistent` | rule | error | ✅ pass | 0 / 2 | incident/injury/preventable counts match their flags (computed by the ETL) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 2 | at most 5% of rows have driver_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 2 | at most 5% of rows have truck_key = -1 (Unknown) |

### `fact_trip_streaming` (37 checks)

| Check | Type | Severity | Result | failed / total | What it tests |
|---|---|---|---|---|---|
| `booking_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 534 | booking_type in (\'Spot\', \'Dedicated\', \'Contract\') |
| `load_status_accepted_values` | accepted_values | warn | ✅ pass | 0 / 534 | load_status in (\'Completed\') |
| `load_type_accepted_values` | accepted_values | warn | ✅ pass | 0 / 534 | load_type in (\'Dry Van\', \'Refrigerated\') |
| `freshness` | freshness | error | ✅ pass | 0 / 1 | newest row is at most 300 s old (streaming is alive) |
| `latency_p95` | latency | warn | ❌ fail | 434 / 534 | 95% of rows reach dwh within 180 s of arriving in Kafka |
| `accessorial_charges_non_negative` | non_negative | error | ✅ pass | 0 / 534 | accessorial_charges >= 0 |
| `actual_miles_non_negative` | non_negative | error | ✅ pass | 0 / 534 | actual_miles >= 0 |
| `fuel_consumed_gallons_non_negative` | non_negative | error | ✅ pass | 0 / 534 | fuel_consumed_gallons >= 0 |
| `fuel_surcharge_non_negative` | non_negative | error | ✅ pass | 0 / 534 | fuel_surcharge >= 0 |
| `idle_hours_non_negative` | non_negative | error | ✅ pass | 0 / 534 | idle_hours >= 0 |
| `pieces_non_negative` | non_negative | error | ✅ pass | 0 / 534 | pieces >= 0 |
| `revenue_non_negative` | non_negative | error | ✅ pass | 0 / 534 | revenue >= 0 |
| `total_revenue_non_negative` | non_negative | error | ✅ pass | 0 / 534 | total_revenue >= 0 |
| `trip_duration_hours_non_negative` | non_negative | error | ✅ pass | 0 / 534 | trip_duration_hours >= 0 |
| `weight_lbs_non_negative` | non_negative | error | ✅ pass | 0 / 534 | weight_lbs >= 0 |
| `trip_id_not_empty` | not_empty | error | ✅ pass | 0 / 534 | trip_id is never NULL or empty |
| `actual_miles_in_range` | range | warn | ✅ pass | 0 / 534 | actual_miles between 1 and 5000 |
| `fuel_consumed_gallons_in_range` | range | warn | ✅ pass | 0 / 534 | fuel_consumed_gallons between 0.1 and 2000 |
| `idle_hours_in_range` | range | warn | ✅ pass | 0 / 534 | idle_hours between 0 and 48 |
| `pieces_in_range` | range | warn | ✅ pass | 0 / 534 | pieces between 1 and 1000 |
| `revenue_in_range` | range | warn | ✅ pass | 0 / 534 | revenue between 1 and 50000 |
| `trip_duration_hours_in_range` | range | warn | ✅ pass | 0 / 534 | trip_duration_hours between 0.1 and 200 |
| `weight_lbs_in_range` | range | warn | ✅ pass | 0 / 534 | weight_lbs between 1 and 80000 |
| `customer_key_in_dim_customer` | relationship | error | ✅ pass | 0 / 534 | customer_key references an existing dim_customer.customer_key |
| `driver_key_in_dim_driver` | relationship | error | ✅ pass | 0 / 534 | driver_key references an existing dim_driver.driver_key |
| `route_key_in_dim_route` | relationship | error | ✅ pass | 0 / 534 | route_key references an existing dim_route.route_key |
| `trailer_key_in_dim_trailer` | relationship | error | ✅ pass | 0 / 534 | trailer_key references an existing dim_trailer.trailer_key |
| `truck_key_in_dim_truck` | relationship | error | ✅ pass | 0 / 534 | truck_key references an existing dim_truck.truck_key |
| `actual_vs_planned_miles` | rule | warn | ✅ pass | 0 / 534 | actual_miles within 0.5x..2x of the route\'s planned_miles |
| `date_key_valid` | rule | error | ✅ pass | 0 / 534 | date_key is a real yyyyMMdd date |
| `mpg_plausible` | rule | warn | ✅ pass | 0 / 534 | actual_miles / fuel_consumed_gallons between 3 and 12 mpg |
| `total_revenue_is_sum` | rule | error | ✅ pass | 0 / 534 | total_revenue = revenue + fuel_surcharge + accessorial_charges (computed by the ETL) |
| `customer_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 534 | at most 5% of rows have customer_key = -1 (Unknown) |
| `driver_key_unknown_rate` | unknown_rate | warn | ✅ pass | 12 / 534 | at most 5% of rows have driver_key = -1 (Unknown) |
| `route_key_unknown_rate` | unknown_rate | warn | ✅ pass | 0 / 534 | at most 5% of rows have route_key = -1 (Unknown) |
| `trailer_key_unknown_rate` | unknown_rate | warn | ✅ pass | 15 / 534 | at most 5% of rows have trailer_key = -1 (Unknown) |
| `truck_key_unknown_rate` | unknown_rate | warn | ✅ pass | 12 / 534 | at most 5% of rows have truck_key = -1 (Unknown) |
