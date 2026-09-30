-- Serving views: one per fact, combining the batch layer and the streaming (speed) layer.
--
--   dwh.<fact>             batch table (daily DAG), loaded up to some day C
--   dwh.<fact>_streaming   streaming table, 72 h TTL
--   dwh.v_<fact>           batch rows with toDate(src_sys_create_date) <= C
--                          + streaming rows with toDate(src_sys_create_date) > C
--
-- C = the last day the batch table has loaded (max src_sys_create_date)
--

CREATE OR REPLACE VIEW dwh.v_fact_trip AS
SELECT
    trip_key,
    trip_id,
    load_id,
    date_key,
    driver_key,
    truck_key,
    trailer_key,
    customer_key,
    route_key,
    booking_type,
    load_type,
    load_status,
    weight,
    pieces,
    planned_miles,
    actual_miles,
    trip_duration_hours,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    total_revenue,
    fuel_consumed_gallons,
    idle_hours,
    src_sys_create_date,
    etl_loaded_date,
    'batch' AS source
FROM dwh.fact_trip FINAL
WHERE toDate(src_sys_create_date) <= (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_trip)
UNION ALL
SELECT
    trip_key,
    trip_id,
    load_id,
    date_key,
    driver_key,
    truck_key,
    trailer_key,
    customer_key,
    route_key,
    booking_type,
    load_type,
    load_status,
    weight,
    pieces,
    planned_miles,
    actual_miles,
    trip_duration_hours,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    total_revenue,
    fuel_consumed_gallons,
    idle_hours,
    src_sys_create_date,
    etl_loaded_date,
    'streaming' AS source
FROM dwh.fact_trip_streaming FINAL
WHERE toDate(src_sys_create_date) > (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_trip);

CREATE OR REPLACE VIEW dwh.v_fact_delivery_event AS
SELECT
    event_key,
    event_id,
    trip_id,
    load_id,
    facility_key,
    event_type,
    scheduled_date_key,
    actual_date_key,
    scheduled_datetime,
    actual_datetime,
    detention_minutes,
    delay_minutes,
    is_on_time,
    src_sys_create_date,
    etl_loaded_date,
    'batch' AS source
FROM dwh.fact_delivery_event FINAL
WHERE toDate(src_sys_create_date) <= (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_delivery_event)
UNION ALL
SELECT
    event_key,
    event_id,
    trip_id,
    load_id,
    facility_key,
    event_type,
    scheduled_date_key,
    actual_date_key,
    scheduled_datetime,
    actual_datetime,
    detention_minutes,
    delay_minutes,
    is_on_time,
    src_sys_create_date,
    etl_loaded_date,
    'streaming' AS source
FROM dwh.fact_delivery_event_streaming FINAL
WHERE toDate(src_sys_create_date) > (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_delivery_event);

CREATE OR REPLACE VIEW dwh.v_fact_fuel_purchase AS
SELECT
    fuel_purchase_key,
    fuel_purchase_id,
    date_key,
    driver_key,
    truck_key,
    trip_key,
    trip_id,
    purchase_datetime,
    purchase_city,
    purchase_state,
    fuel_card_number,
    gallons,
    price_per_gallon,
    total_cost,
    src_sys_create_date,
    etl_loaded_date,
    'batch' AS source
FROM dwh.fact_fuel_purchase FINAL
WHERE toDate(src_sys_create_date) <= (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_fuel_purchase)
UNION ALL
SELECT
    fuel_purchase_key,
    fuel_purchase_id,
    date_key,
    driver_key,
    truck_key,
    trip_key,
    trip_id,
    purchase_datetime,
    purchase_city,
    purchase_state,
    fuel_card_number,
    gallons,
    price_per_gallon,
    total_cost,
    src_sys_create_date,
    etl_loaded_date,
    'streaming' AS source
FROM dwh.fact_fuel_purchase_streaming FINAL
WHERE toDate(src_sys_create_date) > (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_fuel_purchase);

CREATE OR REPLACE VIEW dwh.v_fact_maintenance AS
SELECT
    maintenance_key,
    maintenance_id,
    date_key,
    truck_key,
    maintenance_type,
    odometer,
    labor_hours,
    labor_cost,
    parts_cost,
    total_cost,
    downtime_hours,
    facility_location,
    service_description,
    src_sys_create_date,
    etl_loaded_date,
    'batch' AS source
FROM dwh.fact_maintenance FINAL
WHERE toDate(src_sys_create_date) <= (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_maintenance)
UNION ALL
SELECT
    maintenance_key,
    maintenance_id,
    date_key,
    truck_key,
    maintenance_type,
    odometer,
    labor_hours,
    labor_cost,
    parts_cost,
    total_cost,
    downtime_hours,
    facility_location,
    service_description,
    src_sys_create_date,
    etl_loaded_date,
    'streaming' AS source
FROM dwh.fact_maintenance_streaming FINAL
WHERE toDate(src_sys_create_date) > (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_maintenance);

CREATE OR REPLACE VIEW dwh.v_fact_safety_incident AS
SELECT
    incident_key,
    incident_id,
    date_key,
    driver_key,
    truck_key,
    trip_key,
    trip_id,
    incident_datetime,
    incident_type,
    incident_city,
    incident_state,
    at_fault_flag,
    injury_flag,
    preventable_flag,
    vehicle_damage_cost,
    cargo_damage_cost,
    claim_cost,
    description,
    src_sys_create_date,
    etl_loaded_date,
    'batch' AS source
FROM dwh.fact_safety_incident FINAL
WHERE toDate(src_sys_create_date) <= (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_safety_incident)
UNION ALL
SELECT
    incident_key,
    incident_id,
    date_key,
    driver_key,
    truck_key,
    trip_key,
    trip_id,
    incident_datetime,
    incident_type,
    incident_city,
    incident_state,
    at_fault_flag,
    injury_flag,
    preventable_flag,
    vehicle_damage_cost,
    cargo_damage_cost,
    claim_cost,
    description,
    src_sys_create_date,
    etl_loaded_date,
    'streaming' AS source
FROM dwh.fact_safety_incident_streaming FINAL
WHERE toDate(src_sys_create_date) > (SELECT max(toDate(src_sys_create_date)) FROM dwh.fact_safety_incident);
