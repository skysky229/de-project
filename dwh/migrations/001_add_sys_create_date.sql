-- Adds sys_create_date (when the row was recorded) to every raw table in `default`.
--
-- New inserts get DEFAULT now(). Existing rows are backfilled from the table's
-- business timestamp: a plain DEFAULT would be evaluated at read time (the value
-- changes on every query), and MATERIALIZE would stamp all history with the same
-- "today" timestamp, so neither works for a daily incremental filter.
--
-- Safe to re-run (ADD COLUMN IF NOT EXISTS; the UPDATE rewrites the same values).
-- Undo: ALTER TABLE default.<t> DROP COLUMN sys_create_date;

-- facts: recorded when the business event happened
ALTER TABLE default.trips               ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.trips               UPDATE sys_create_date = toDateTime(dispatch_date, 'UTC')    WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.loads               ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.loads               UPDATE sys_create_date = toDateTime(load_date, 'UTC')        WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.delivery_events     ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.delivery_events     UPDATE sys_create_date = toDateTime(actual_datetime, 'UTC')  WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.fuel_purchases      ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.fuel_purchases      UPDATE sys_create_date = toDateTime(purchase_date, 'UTC')    WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.maintenance_records ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.maintenance_records UPDATE sys_create_date = toDateTime(maintenance_date, 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.safety_incidents    ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.safety_incidents    UPDATE sys_create_date = toDateTime(incident_date, 'UTC')    WHERE 1 SETTINGS mutations_sync = 2;

-- dimensions: recorded when the entity came into existence
ALTER TABLE default.drivers             ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.drivers             UPDATE sys_create_date = toDateTime(hire_date, 'UTC')           WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.trucks              ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.trucks              UPDATE sys_create_date = toDateTime(acquisition_date, 'UTC')    WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.trailers            ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.trailers            UPDATE sys_create_date = toDateTime(acquisition_date, 'UTC')    WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.customers           ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.customers           UPDATE sys_create_date = toDateTime(contract_start_date, 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

-- no business date at all: fixed initial-load time, before the first fact (2022-01-01)
ALTER TABLE default.facilities          ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.facilities          UPDATE sys_create_date = toDateTime('2021-12-31 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.routes              ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.routes              UPDATE sys_create_date = toDateTime('2021-12-31 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

-- monthly metrics: recorded once the month is over (first day of the next month)
ALTER TABLE default.driver_monthly_metrics    ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.driver_monthly_metrics    UPDATE sys_create_date = toDateTime(addMonths(month, 1), 'UTC') WHERE 1 SETTINGS mutations_sync = 2;

ALTER TABLE default.truck_utilization_metrics ADD COLUMN IF NOT EXISTS sys_create_date DateTime('UTC') DEFAULT now();
ALTER TABLE default.truck_utilization_metrics UPDATE sys_create_date = toDateTime(addMonths(month, 1), 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
