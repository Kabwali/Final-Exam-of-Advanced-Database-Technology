
-- parkingticketing_postgres.sql
-- Smart Parking Management & Ticketing System (PostgreSQL)
-- This script is organized into sections for Node_B (parking_b) and Node_A (parking_a).
-- Intended workflow (run as a superuser or a user with CREATE DATABASE and postgres_fdw rights):
-- 1. Run the "01-create-databases-and-schemas" section (or use manual DB creation) to create parking_a and parking_b.
-- 2. Run Node_B setup on the parking_b database (Section: Node_B).
-- 3. Run Node_A setup on the parking_a database (Section: Node_A).
-- 4. Follow the README for exact commands to run each test and capture outputs (counts, checksums, EXPLAIN ANALYZE, pg_locks, etc.).
-- NOTE: Some operations (e.g., cross-database FDW connection) require postgres_fdw extension and that the target server accepts local connections.
-- Adjust connection parameters (host, port, dbname, user, password) in the CREATE SERVER/USER MAPPING sections as needed for your environment.

-- A1. Create databases for parking_a (Node_A) and parking_b (Node_B)
  
-- Run these as a superuser once in the postgres maintenance DB or psql startup:
CREATE DATABASE parking_a;
CREATE DATABASE parking_b;

-- After creating databases, connect to each as instructed below.

-- Node_B (parking_b) -- remote node
-- Run: psql -d parking_b -v ON_ERROR_STOP=1 -f parkingticketing_postgres.sql
-- Execute only the Node_B section on this connection.
 
-- 1. Node_B: Schema and core tables
-- Connect to parking_b before running the below CREATE TABLE statements.

-- Create schemas (optional)
CREATE SCHEMA IF NOT EXISTS parking_b AUTHORIZATION current_user;

-- Core tables in parking_b (we keep all core tables on both sides for demo; Ticket_B will reside here)
SET search_path = parking_b, public;

-- ParkingLot
CREATE TABLE IF NOT EXISTS parking_lot (
  lot_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  location TEXT,
  capacity INT CHECK (capacity >= 0),
  status VARCHAR(20) CHECK (status IN ('Open','Closed','Maintenance')) DEFAULT 'Open'
);

-- Space
CREATE TABLE IF NOT EXISTS space (
  space_id SERIAL PRIMARY KEY,
  lot_id INT NOT NULL REFERENCES parking_lot(lot_id) ON DELETE CASCADE,
  space_no TEXT NOT NULL,
  status VARCHAR(20) CHECK (status IN ('Free','Occupied','Reserved')) DEFAULT 'Free',
  type VARCHAR(20) CHECK (type IN ('Compact','Regular','Handicapped','EV')) DEFAULT 'Regular'
);

-- Vehicle
CREATE TABLE IF NOT EXISTS vehicle (
  vehicle_id SERIAL PRIMARY KEY,
  plate_no VARCHAR(20) UNIQUE NOT NULL,
  type VARCHAR(20) CHECK (type IN ('Car','Motorcycle','Truck')) NOT NULL,
  owner_name TEXT,
  contact VARCHAR(20)
);

-- Staff
CREATE TABLE IF NOT EXISTS staff (
  staff_id SERIAL PRIMARY KEY,
  fullname TEXT NOT NULL,
  role VARCHAR(50),
  contact VARCHAR(20),
  shift VARCHAR(20)
);

-- Payment
CREATE TABLE IF NOT EXISTS payment (
  payment_id SERIAL PRIMARY KEY,
  ticket_id INT UNIQUE NOT NULL, -- 1:1 relation to ticket
  amount NUMERIC(12,2) CHECK (amount >= 0),
  payment_date TIMESTAMP NOT NULL DEFAULT now(),
  method VARCHAR(30) CHECK (method IN ('Cash','Card','Mobile')),
  CONSTRAINT fk_payment_ticket FOREIGN KEY (ticket_id) REFERENCES ticket(ticket_id) ON DELETE CASCADE
);

-- Ticket_B: fragment on Node_B (we create it after ticket base seen; we'll create stub and then populate)
-- To avoid circular FK when creating payment referencing ticket, we will create ticket table first.
CREATE TABLE IF NOT EXISTS ticket (
  ticket_id SERIAL PRIMARY KEY,
  space_id INT NOT NULL REFERENCES space(space_id) ON DELETE RESTRICT,
  vehicle_id INT NOT NULL REFERENCES vehicle(vehicle_id) ON DELETE RESTRICT,
  entry_time TIMESTAMP NOT NULL,
  exit_time TIMESTAMP,
  status VARCHAR(20) CHECK (status IN ('Active','Exited','Lost')) DEFAULT 'Active',
  staff_id INT REFERENCES staff(staff_id) ON DELETE SET NULL,
  total_amount NUMERIC(12,2) CHECK (total_amount >= 0) DEFAULT 0
);

-- Now create payment table with FK to ticket (moved here in correct order)
-- (If the earlier payment creation failed due to missing ticket, re-create or ALTER as needed.)

-- 2. Populate small sample data on Node_B (we will insert a few rows; total across both nodes must be <=10)
-- Insert parking lots
INSERT INTO parking_lot (name, location, capacity, status) VALUES
('Central Lot', 'Downtown', 100, 'Open') ON CONFLICT DO NOTHING;

-- Insert spaces
INSERT INTO space (lot_id, space_no, status, type)
VALUES
  (1, 'A1', 'Free', 'Regular'),
  (1, 'A2', 'Free', 'EV'),
  (1, 'A3', 'Free', 'Compact')
ON CONFLICT DO NOTHING;

-- Vehicles
INSERT INTO vehicle (plate_no, type, owner_name, contact) 
VALUES
('RAB123A', 'Car', 'Alice M', '0788000001'),
('RAB124B', 'Motorcycle', 'Bob K', '0788000002'),
('RAB125C', 'Car', 'Claire T', '0788000003') 
ON CONFLICT DO NOTHING;

-- Staff
INSERT INTO staff (fullname, role, contact, shift) 
VALUES
('John Guard','Attendant','0788000100','Day'),
('Martha','Manager','0788000101','Night') 
ON CONFLICT DO NOTHING;

-- Ticket_B inserts (we will insert 5 rows on Node_B)
INSERT INTO ticket (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount) VALUES
(1, 1, '2025-10-01 08:00:00', '2025-10-01 10:30:00', 'Exited', 1, 5.00),
(2, 2, '2025-10-01 09:15:00', NULL, 'Active', 1, 0.00),
(3, 3, '2025-10-02 07:05:00', '2025-10-02 08:00:00', 'Exited', 2, 2.00),
(1, 2, '2025-10-03 12:00:00', '2025-10-03 12:45:00', 'Exited', 1, 1.50),
(2, 1, '2025-10-04 18:00:00', NULL, 'Active', 2, 0.00);

-- Payments corresponding to some tickets (Ticket -> Payment must be ON DELETE CASCADE: we'll ensure by FK with ON DELETE CASCADE)
-- Note: payment.ticket_id references local ticket ids (these are created above).
INSERT INTO payment (ticket_id, amount, payment_date, method) 
VALUES
(1, 5.00, '2025-10-01 11:00:00', 'Card'),
(3, 2.00, '2025-10-02 08:15:00', 'Cash'),
(4, 1.50, '2025-10-03 13:00:00', 'Mobile') 
ON CONFLICT DO NOTHING;

-- Node_A (parking_a) -- local node
-- Run: psql -d parking_a -v ON_ERROR_STOP=1 -f parkingticketing_postgres.sql
-- Execute only the Node_A section on this connection.
  
-- === Node_A: Schema and core tables ===

-- Connect to parking_a and run the following.
CREATE SCHEMA IF NOT EXISTS parking_a AUTHORIZATION current_user;
SET search_path = parking_a, public;

-- Core tables (same definitions)
CREATE TABLE IF NOT EXISTS parking_lot (
  lot_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  location TEXT,
  capacity INT CHECK (capacity >= 0),
  status VARCHAR(20) CHECK (status IN ('Open','Closed','Maintenance')) DEFAULT 'Open'
);

CREATE TABLE IF NOT EXISTS space (
  space_id SERIAL PRIMARY KEY,
  lot_id INT NOT NULL REFERENCES parking_lot(lot_id) ON DELETE CASCADE,
  space_no TEXT NOT NULL,
  status VARCHAR(20) CHECK (status IN ('Free','Occupied','Reserved')) DEFAULT 'Free',
  type VARCHAR(20) CHECK (type IN ('Compact','Regular','Handicapped','EV')) DEFAULT 'Regular'
);

CREATE TABLE IF NOT EXISTS vehicle (
  vehicle_id SERIAL PRIMARY KEY,
  plate_no VARCHAR(20) UNIQUE NOT NULL,
  type VARCHAR(20) CHECK (type IN ('Car','Motorcycle','Truck')) NOT NULL,
  owner_name TEXT,
  contact VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS staff (
  staff_id SERIAL PRIMARY KEY,
  fullname TEXT NOT NULL,
  role VARCHAR(50),
  contact VARCHAR(20),
  shift VARCHAR(20)
);

-- Ticket_A (fragment table on Node_A)
CREATE TABLE IF NOT EXISTS ticket_a (
  ticket_id SERIAL PRIMARY KEY,
  space_id INT NOT NULL, -- might reference local space or remote, for demo we keep local spaces
  vehicle_id INT NOT NULL,
  entry_time TIMESTAMP NOT NULL,
  exit_time TIMESTAMP,
  status VARCHAR(20) CHECK (status IN ('Active','Exited','Lost')) DEFAULT 'Active',
  staff_id INT,
  total_amount NUMERIC(12,2) CHECK (total_amount >= 0) DEFAULT 0
);

-- Payment on Node_A referencing ticket_a via FK with CASCADE delete handled manually if cross-db
CREATE TABLE IF NOT EXISTS payment_a (
  payment_id SERIAL PRIMARY KEY,
  ticket_id INT UNIQUE NOT NULL,
  amount NUMERIC(12,2) CHECK (amount >= 0),
  payment_date TIMESTAMP NOT NULL DEFAULT now(),
  method VARCHAR(30) CHECK (method IN ('Cash','Card','Mobile')),
  CONSTRAINT fk_payment_ticket_a FOREIGN KEY (ticket_id) REFERENCES ticket_a(ticket_id) ON DELETE CASCADE
);

-- Populate Node_A sample data (5 rows to complement Node_B 5 rows -> total 10)
INSERT INTO parking_lot (name, location, capacity, status) VALUES ('North Lot','Uptown',50,'Open') ON CONFLICT DO NOTHING;
INSERT INTO space (lot_id, space_no, status, type) VALUES
    (1, 'B1', 'Free', 'Regular'),
    (1, 'B2', 'Free', 'Regular')
ON CONFLICT DO NOTHING;

INSERT INTO vehicle (plate_no, type, owner_name, contact) VALUES ('RAB200X','Car','David N','0788000004') ON CONFLICT DO NOTHING;
INSERT INTO staff (fullname, role, contact, shift) VALUES ('Peter','Attendant','0788000102','Day') ON CONFLICT DO NOTHING;

-- Ticket_A inserts (5 rows)
INSERT INTO ticket_a (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount) VALUES
(1, 1, '2025-10-05 09:00:00', '2025-10-05 10:00:00', 'Exited', 1, 3.00),
(2, 2, '2025-10-06 14:00:00', NULL, 'Active', 1, 0.00),
(1, 2, '2025-10-07 07:30:00', '2025-10-07 08:30:00', 'Exited', 1, 2.50),
(2, 1, '2025-10-08 18:15:00', NULL, 'Active', 1, 0.00),
(1, 1, '2025-10-09 11:05:00', '2025-10-09 12:00:00', 'Exited', 1, 1.25);

-- payments
INSERT INTO payment_a (ticket_id, amount, payment_date, method) VALUES
(1, 3.00, '2025-10-05 10:05:00', 'Cash'),
(3, 2.50, '2025-10-07 08:35:00', 'Card'),
(5, 1.25, '2025-10-09 12:10:00', 'Mobile') ON CONFLICT DO NOTHING;

-- 3. Create Ticket_ALL view on Node_A as UNION ALL of local Ticket_A and remote Ticket (ticket from Node_B via proj_link) ===
-- We refer to local ticket_a and foreign table parking_b.ticket
CREATE OR REPLACE VIEW ticket_all AS
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'A' AS source_node FROM ticket_a
UNION ALL
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'B' AS source_node FROM parking_b.ticket;

-- 4. Validation: counts and checksum (sum of mod primary key,97) ===
-- Run these queries on Node_A to validate fragments vs ticket_all
-- 1) Count fragments:
SELECT COUNT(*) AS cnt_a FROM ticket_a;
SELECT COUNT(*) AS cnt_b FROM parking_b.ticket;
SELECT COUNT(*) AS cnt_all FROM ticket_all;
-- 2) Checksum (sum(mod(pk,97)))
SELECT SUM((ticket_id % 97)) AS chk_a FROM ticket_a;
SELECT SUM((ticket_id % 97)) AS chk_b FROM parking_b.ticket;
SELECT SUM((ticket_id % 97)) AS chk_all FROM ticket_all;
-- Expected: cnt_a + cnt_b = cnt_all; chk_a + chk_b = chk_all


-- 1. On Node_B (parking_b) — Create main table ticket
-- Ensure base schema
CREATE TABLE IF NOT EXISTS ticket (
  ticket_id SERIAL PRIMARY KEY,
  space_id INT NOT NULL REFERENCES space(space_id) ON DELETE RESTRICT,
  vehicle_id INT NOT NULL REFERENCES vehicle(vehicle_id) ON DELETE RESTRICT,
  entry_time TIMESTAMP NOT NULL,
  exit_time TIMESTAMP,
  status VARCHAR(20) CHECK (status IN ('Active','Exited','Lost')) DEFAULT 'Active',
  staff_id INT REFERENCES staff(staff_id) ON DELETE SET NULL,
  total_amount NUMERIC(12,2) CHECK (total_amount >= 0) DEFAULT 0
);
-- Insert 5 rows (Node_B fragment)
INSERT INTO ticket (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount)
VALUES
(1, 101, '2025-10-01 08:00:00', '2025-10-01 09:00:00', 'Exited', 1, 2.50),
(2, 102, '2025-10-02 07:30:00', '2025-10-02 08:15:00', 'Exited', 1, 1.75),
(3, 103, '2025-10-03 09:00:00', '2025-10-03 10:00:00', 'Exited', 2, 2.00),
(4, 104, '2025-10-04 10:15:00', NULL, 'Active', 2, 0.00),
(5, 105, '2025-10-05 11:00:00', '2025-10-05 11:45:00', 'Exited', 3, 1.50);

-- 2. On Node_A (parking_a) — Create local fragment ticket_a

CREATE TABLE IF NOT EXISTS ticket_a (
  ticket_id SERIAL PRIMARY KEY,
  space_id INT NOT NULL, -- might reference local space or remote, for demo we keep local spaces
  vehicle_id INT NOT NULL,
  entry_time TIMESTAMP NOT NULL,
  exit_time TIMESTAMP,
  status VARCHAR(20) CHECK (status IN ('Active','Exited','Lost')) DEFAULT 'Active',
  staff_id INT,
  total_amount NUMERIC(12,2) CHECK (total_amount >= 0) DEFAULT 0
);

-- Insert 5 rows (Node_A fragment)
INSERT INTO ticket_a (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount)
VALUES
(6, 201, '2025-10-06 08:15:00', '2025-10-06 09:00:00', 'Exited', 2, 1.80),
(7, 202, '2025-10-07 07:45:00', '2025-10-07 08:30:00', 'Exited', 1, 2.20),
(8, 203, '2025-10-08 09:30:00', NULL, 'Active', 3, 0.00),
(9, 204, '2025-10-09 10:00:00', '2025-10-09 10:45:00', 'Exited', 2, 1.60),
(10, 205, '2025-10-10 11:15:00', '2025-10-10 12:00:00', 'Exited', 1, 2.00);

-- 3. Create a unified view ticket_all on Node_A
CREATE OR REPLACE VIEW ticket_all AS
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'A' AS source_node
FROM ticket_a 
UNION ALL
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'B' AS source_node
FROM ticket;
-----------------------------------------------------------------------------------------------------------------
-- OK --
CREATE OR REPLACE VIEW ticket_all AS
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'A' AS source_node
FROM parking_a.ticket_a
UNION ALL
SELECT ticket_id, space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount, 'B' AS source_node
FROM parking_b.ticket;
-----------------------------------------------------------------------------------------------------------------

-- 4. Validate fragmentation and recombination
-- Count rows per fragment and combined view
SELECT COUNT(*) AS cnt_a FROM ticket_a;
SELECT COUNT(*) AS cnt_b FROM ticket;
SELECT COUNT(*) AS cnt_all FROM ticket_all;

-- Checksum validation (based on modulo of PK)
SELECT SUM((ticket_id % 97)) AS chk_a FROM ticket_a;
SELECT SUM((ticket_id % 97)) AS chk_b FROM ticket;
SELECT SUM((ticket_id % 97)) AS chk_all FROM ticket_all;

----------------------------------------------------------------------------------------------------------------
-- 4. Validate fragmentation and recombination
-- Count rows per fragment and combined view
SELECT COUNT(*) AS cnt_a FROM parking_a.ticket_a;
SELECT COUNT(*) AS cnt_b FROM parking_a.ticket;
SELECT COUNT(*) AS cnt_all FROM parking_a.ticket_all;

-- Checksum validation (based on modulo of PK)
SELECT SUM((ticket_id % 97)) AS chk_a FROM parking_a.ticket_a;
SELECT SUM((ticket_id % 97)) AS chk_b FROM parking_a.ticket;
SELECT SUM((ticket_id % 97)) AS chk_all FROM parking_a.ticket_all;

----------------------------------------------------------------------------------------------------------------
-- A2. Create a database link from Node_A to Node_B
-- On Node_A
-- 1a. Create a foreign server pointing to Node_B
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS proj_link CASCADE;

CREATE SERVER proj_link
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', port '5432', dbname 'parking_b');

-- Database Link and Cross-Node Join (3–10 rows result)
-- 1b. Create a user mapping for access to Node_B
CREATE USER MAPPING FOR CURRENT_USER
  SERVER proj_link
  OPTIONS (user 'postgres', password 'mas098');

-- 1c. Import the remote schema or tables (optional)
IMPORT FOREIGN SCHEMA public
  LIMIT TO (space, vehicle)
  FROM SERVER proj_link
  INTO parking_b;
-- 2. Run a remote SELECT on Space@proj_link
-- Fetch first 5 rows from the remote table
SELECT *
FROM parking_b.space
FETCH FIRST 5 ROWS ONLY;

-- 3. Run a distributed join (local + remote)
-- Join local Ticket_A with remote Vehicle@proj_link
-- Include a selective predicate to limit the row count to 3–10 rows
SELECT t.ticket_id, t.space_id, t.vehicle_id, v.plate_no, t.entry_time, t.exit_time
FROM ticket_a t
JOIN parking_b.vehicle v
  ON t.vehicle_id = v.vehicle_id
WHERE t.entry_time >= '2025-10-05'
FETCH FIRST 10 ROWS ONLY;

---------------------------------------
SELECT t.ticket_id, t.space_id, t.vehicle_id, v.plate_no, t.entry_time, t.exit_time
FROM parking_a.ticket_a t
JOIN parking_b.vehicle v
  ON t.vehicle_id = v.vehicle_id
WHERE t.entry_time >= '2025-10-05'
FETCH FIRST 10 ROWS ONLY;
-- Note: Use appropriate WHERE to get between 3 and 10 rows.

-- A3: Serial vs Parallel Aggregation (EXPLAIN ANALYZE)

-- 1. SERIAL Aggregation (Force Single-Worker Plan)

-- Disable parallelism
SET max_parallel_workers_per_gather = 0;

-- SERIAL aggregation query
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT status,
       COUNT(*) AS cnt,
       SUM(total_amount) AS total_amt
FROM ticket_all
GROUP BY status;

-- 2. PARALLEL Aggregation (Allow Multiple Workers)

-- Enable parallelism (up to 8 workers)
SET max_parallel_workers_per_gather = 8;

-- PARALLEL aggregation query
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT status,
       COUNT(*) AS cnt,
       SUM(total_amount) AS total_amt
FROM ticket_all
GROUP BY status;

-- 3. Capture and Compare Execution Plans

-- Use \timing and \x in psql for clearer display
-- \x off
\timing on

-- Note: Check for "Parallel Seq Scan" or "Gather" in EXPLAIN output
--       to confirm parallel execution occurred.


-- 4: Create 2-Row Comparison Table (Summary)

-- Example simulated results for report (replace with your real timings)
CREATE TEMP TABLE agg_perf (
    mode TEXT,
    exec_ms NUMERIC,
    buffers INT,
    notes TEXT
);

INSERT INTO agg_perf VALUES
('Serial',   1.23, 12, 'Single-worker, Sequential Scan on ticket_all'),
('Parallel', 0.78, 12, 'Parallel Seq Scan with 2 workers via Gather');

-- View comparison
SELECT * FROM agg_perf;

-- Optional: Check the aggregated results directly (for verification)

SELECT status, COUNT(*) AS cnt, SUM(total_amount) AS total_amt
FROM ticket_all
GROUP BY status
ORDER BY status;

-- Notes:
-- - Use EXPLAIN ANALYZE output to extract execution time & buffer stats.
-- - The dataset is intentionally small (≤10 rows) so differences may be minor.
-- - For real analysis, increase data volume to highlight parallel efficiency.

-- A4: Two-Phase Commit & Recovery (PREPARE TRANSACTION)
  
-- Note: In PostgreSQL, two-phase commit can be demonstrated with PREPARE TRANSACTION and COMMIT PREPARED.
-- The example below should be run across two connections (one connected to parking_a and one to parking_b),
-- or can be emulated using remote transaction via postgres_fdw which participates in distributed transactions automatically
-- when using a transaction manager like pg_prepared_xacts listing PREPARED transactions in pg_prepared_xacts.

-- Example PL/pgSQL pseudo-block demonstrating 2PC from Node_A inserting into local and remote via FDW:
-- BEGIN;
-- INSERT INTO ticket_a (space_id, vehicle_id, entry_time, status, staff_id, total_amount) VALUES (1,1,now(), 'Active', 1, 0.00);
-- -- Insert remotely via foreign table (this will be part of same distributed transaction)
-- INSERT INTO parking_b.ticket (space_id, vehicle_id, entry_time, status, staff_id, total_amount) VALUES (1,1,now(), 'Active', 1, 0.00);
-- -- Commit; postgres_fdw coordinates 2PC behind the scenes (with PREPARE/COMMIT prepared transactions)
-- COMMIT;

-- To simulate an in-doubt transaction: use PREPARE TRANSACTION manually on Node_A after doing local changes, then don't commit on remote etc.
-- Or use explicit PREPARE TRANSACTION / COMMIT PREPARED:
-- On Node_A connection:
-- BEGIN;
-- INSERT INTO ticket_a (...) VALUES (...);
-- PREPARE TRANSACTION 'txn_demo_1';
-- -- Now the transaction is prepared and will show up in pg_prepared_xacts on the node.
-- -- From another session you can COMMIT PREPARED 'txn_demo_1' or ROLLBACK PREPARED 'txn_demo_1';

-- Query prepared transactions:
-- SELECT * FROM pg_prepared_xacts;

-- To force resolve:
-- COMMIT PREPARED 'txn_demo_1';
-- or
-- ROLLBACK PREPARED 'txn_demo_1';


-- A4: Two-Phase Commit & Recovery (PREPARE TRANSACTION)
-- SMART PARKING MANAGEMENT & TICKETING SYSTEM

-- 1. Insert ONE local row on Node_A and ONE remote row on Node_B, then COMMIT
BEGIN;
INSERT INTO ticket_a (space_id, vehicle_id, entry_time, status, staff_id, total_amount)
VALUES (1, 1, now(), 'Active', 1, 5.00);
INSERT INTO parking_b.payment (ticket_id, amount, payment_date, method)
VALUES (1, 5.00, now(), 'Card');
COMMIT;

-- 2. Induce failure to simulate in-doubt transaction
-- (Manually stop FDW link or emulate partial commit using PREPARE TRANSACTION)
BEGIN;
INSERT INTO ticket_a (space_id, vehicle_id, entry_time, status, staff_id, total_amount)
VALUES (2, 1, now(), 'Pending', 1, 0.00);
PREPARE TRANSACTION 'txn_demo_1';

-- 3. Query and force-resolve in-doubt transactions
SELECT * FROM pg_prepared_xacts;
COMMIT PREPARED 'txn_demo_1';

-- 4. Verify no pending transactions and consistency
SELECT * FROM pg_prepared_xacts;
SELECT ticket_id, status, total_amount FROM ticket_a ORDER BY ticket_id;
SELECT payment_id, amount, method FROM parking_b.payment ORDER BY payment_id;


-- A5: Distributed Lock Conflict & Diagnosis (pg_locks)
  
-- Steps to reproduce (do NOT run both updates in same session):
-- Session 1 (Node_A): BEGIN; UPDATE ticket_a SET total_amount = total_amount + 1 WHERE ticket_id = 1; -- keep transaction open
-- Session 2 (Node_A): -- attempt to update same logical row via foreign path (or local) e.g., UPDATE parking_b.ticket SET total_amount = total_amount + 2 WHERE ticket_id = 1;
-- On Node_A, inspect locks:
-- SELECT pid, locktype, relation::regclass, page, tuple, virtualtransaction, mode, granted FROM pg_locks l LEFT JOIN pg_class c ON l.relation = c.oid WHERE relation::regclass::text LIKE 'ticket%' OR pid IS NOT NULL;
-- Or a simpler diagnostic:
-- SELECT * FROM pg_locks WHERE NOT granted;
-- After releasing session1 (COMMIT or ROLLBACK), session2 proceeds.

-- 1. Open Session 1 on Node_A and lock a row
-- Connect to Node_A
\c yourdb Node_A_user
connection to server at "localhost" (::1), port 5432 failed: FATAL:  password authentication failed for user "Node_A_user"
Previous connection kept

-- Start a transaction and update a row (keep it open)
BEGIN;
-- Update a single row in ticket (do NOT commit yet)
UPDATE parking_a.ticket
SET total_amount = total_amount + 1
WHERE ticket_id = 1;
-- At this point, the row is locked by this transaction
-- Do NOT commit or rollback yet

-- 2. Open Session 2 on Node_B and attempt to update the same logical row
-- Connect to Node_B
\c yourdb Node_B_user

-- Start transaction
BEGIN;

-- Attempt to update the same logical row via FDW (foreign table)
UPDATE ticket@proj_link
SET total_amount = total_amount + 2
WHERE ticket_id = 1;

 -- Steps to complete 2.

-- Step 1: Create a foreign server (done once)
CREATE SERVER proj_link
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'yourdb', port '5432');

-- Step 2: Create a user mapping
CREATE USER MAPPING FOR postgres
  SERVER proj_link
  OPTIONS (user 'Node_A_user', password 'your_password');

-- Step 3: Create a foreign table
CREATE FOREIGN TABLE ticket_remote (
  ticket_id integer,
  total_amount numeric
)
SERVER proj_link
OPTIONS (schema_name 'public', table_name 'ticket');

-- Step 4: Run your update
UPDATE ticket_remote
SET total_amount = total_amount + 2
WHERE ticket_id = 1;


-- 3. Inspect locks from Node_A
-- Query all locks related to ticket tables
SELECT pid, locktype, relation::regclass, page, tuple,
       virtualtransaction, mode, granted
FROM pg_locks l
LEFT JOIN pg_class c ON l.relation = c.oid
WHERE relation::regclass::text LIKE 'ticket%'
   OR pid IS NOT NULL;

-- Simpler diagnostic: show only locks waiting
SELECT *
FROM pg_locks
WHERE NOT granted;

-- 4. Release the lock in Session 1
-- Back in Session 1
COMMIT;  -- or ROLLBACK;

-- Now Session 2 will automatically proceed and complete
-- 5. Verify Session 2 completed
-- From Session 2 (or after it unblocks)
SELECT * FROM parking_b.ticket WHERE ticket_id = 1;
-- total_amount should reflect both updates

-- B6: Declarative Rules Hardening (constraints and tests)

-- Add/verify NOT NULL and domain CHECK constraints on ticket_a and payment_a
ALTER TABLE ticket_a
  ALTER COLUMN entry_time SET NOT NULL,
  ALTER COLUMN status SET NOT NULL;

ALTER TABLE payment_a
  ALTER COLUMN amount SET NOT NULL,
  ALTER COLUMN payment_date SET NOT NULL,
  ALTER COLUMN method SET NOT NULL;

-- Add named CHECK constraints
ALTER TABLE ticket_a ADD CONSTRAINT chk_ticket_status_valid CHECK (status IN ('Active','Exited','Lost')) ;
ALTER TABLE payment_a ADD CONSTRAINT chk_payment_amount_positive CHECK (amount >= 0);

-- Test inserts (2 failing, 2 passing) - wrap failing ones in DO block to rollback
-- DO $$ BEGIN
--   -- Failing: negative amount
--   BEGIN INSERT INTO payment_a (ticket_id, amount, payment_date, method) VALUES (999, -5.00, now(), 'Cash'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'Expected failure: %', SQLERRM; END;
--   -- Failing: invalid status
--   BEGIN INSERT INTO ticket_a (space_id, vehicle_id, entry_time, status) VALUES (1,1,now(),'INVALID'); EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'Expected failure: %', SQLERRM; END;
-- END $$;
-- Passing inserts (commit)
-- INSERT INTO ticket_a (space_id, vehicle_id, entry_time, status, staff_id, total_amount) VALUES (1,1,'2025-10-10 09:00:00','Exited',1,2.25);
-- Verify committed rows count <=10 via:
-- SELECT COUNT(*) FROM ticket_a;
-- SELECT COUNT(*) FROM parking_b.ticket;
-- SELECT (SELECT COUNT(*) FROM ticket_a) + (SELECT COUNT(*) FROM parking_b.ticket) AS total_committed;

-- Steps of B6:

-- 1. Add/Verify NOT NULL and domain CHECK constraints
-- --- Ticket table ---
ALTER TABLE ticket
  ALTER COLUMN entry_time SET NOT NULL,
  ALTER COLUMN exit_time SET NOT NULL,
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN total_amount SET NOT NULL;
-----------------------OK----------------------
ALTER TABLE parking_a.ticket
  ALTER COLUMN entry_time SET NOT NULL,
  ALTER COLUMN exit_time SET NOT NULL,
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN total_amount SET NOT NULL;

-- Named CHECK constraints
ALTER TABLE parking_a.ticket
  ADD CONSTRAINT chk_ticket_status_valid
  CHECK (status IN ('Active','Exited','Lost'));

ALTER TABLE parking_a.ticket
  ADD CONSTRAINT chk_ticket_total_amount_positive
  CHECK (total_amount >= 0);

ALTER TABLE parking_a.ticket
  ADD CONSTRAINT chk_ticket_times_valid
  CHECK (exit_time >= entry_time);

-- --- Payment table ---
ALTER TABLE parking_b.payment
  ALTER COLUMN amount SET NOT NULL,
  ALTER COLUMN payment_date SET NOT NULL,
  ALTER COLUMN method SET NOT NULL;

-- Named CHECK constraints
ALTER TABLE parking_b.payment
  ADD CONSTRAINT chk_payment_amount_positive
  CHECK (amount >= 0);

ALTER TABLE parking_b.payment
  ADD CONSTRAINT chk_payment_date_not_future
  CHECK (payment_date <= now());

-- 2. Prepare failing inserts (wrapped in a DO block)
DO $$
BEGIN
  -- Failing ticket insert: invalid status
  BEGIN
    INSERT INTO parking_b.ticket (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount)
    VALUES (1, 1, '2025-10-10 08:00:00', '2025-10-10 09:00:00', 'INVALID', 1, 5.0);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected ticket failure: %', SQLERRM;
  END;

  -- Failing ticket insert: negative total_amount
  BEGIN
    INSERT INTO parking_b.ticket (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount)
    VALUES (2, 2, '2025-10-10 10:00:00', '2025-10-10 11:00:00', 'Active', 2, -10.0);
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected ticket failure: %', SQLERRM;
  END;

  -- Failing payment insert: negative amount
  BEGIN
    INSERT INTO parking_b.payment (ticket_id, amount, payment_date, method)
    VALUES (1, -20.0, now(), 'Cash');
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected payment failure: %', SQLERRM;
  END;

  -- Failing payment insert: payment_date in future
  BEGIN
    INSERT INTO parking_b.payment (ticket_id, amount, payment_date, method)
    VALUES (1, 10.0, '2099-01-01', 'Card');
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Expected payment failure: %', SQLERRM;
  END;
END $$;

-- 3. Prepare passing inserts (commit)
-- Passing tickets
INSERT INTO parking_b.ticket (space_id, vehicle_id, entry_time, exit_time, status, staff_id, total_amount)
VALUES
(1, 1, '2025-10-10 09:00:00', '2025-10-10 10:00:00', 'Exited', 1, 2.25),
(2, 2, '2025-10-11 08:30:00', '2025-10-11 09:30:00', 'Active', 2, 3.50);

-- Passing payments
INSERT INTO parking_b.payment (ticket_id, amount, payment_date, method)
VALUES
(1, 2.25, '2025-10-10 10:15:00', 'Cash'),
(2, 3.50, '2025-10-11 09:45:00', 'Card');

-- Verify total committed rows ≤10
SELECT COUNT(*) AS ticket_count FROM parking_b.ticket;
SELECT COUNT(*) AS payment_count FROM parking_b.payment;
SELECT (SELECT COUNT(*) FROM parking_b.ticket) + (SELECT COUNT(*) FROM parking_b.payment) AS total_committed;

-- The notices i am seeing:
NOTICE: Expected parking_b.ticket failure: new row for relation "ticket" violates check constraint "ticket_status_check"
NOTICE: Expected parking_b.ticket failure: new row for relation "ticket" violates check constraint "ticket_total_amount_check"
NOTICE: Expected parking_b.payment failure: new row for relation "payment" violates check constraint "chk_payment_amount_positive"
NOTICE: Expected parking_b.payment failure: new row for relation "payment" violates check constraint "chk_payment_date_not_future"


-- B7: Statement-level AFTER trigger to recompute denormalized totals in ticket
  
-- 1. Create an audit table
CREATE TABLE IF NOT EXISTS ticket_audit (
  audit_id SERIAL PRIMARY KEY,
  bef_total NUMERIC(12,2),
  aft_total NUMERIC(12,2),
  changed_at TIMESTAMP DEFAULT now(),
  key_col TEXT
);

-- 2. Statement-level trigger function (recomputes ticket.total_amount based on payment_a entries)
CREATE OR REPLACE FUNCTION trg_recompute_ticket_totals() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
  rec RECORD;
  before_sum NUMERIC;
  after_sum NUMERIC;
BEGIN
  -- For simplicity, recompute totals for all tickets affected by payments in the statement
  -- Get list of distinct ticket_ids from inserted/updated/deleted rows in payment_a via TG_OP logic.
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    FOR rec IN SELECT DISTINCT ticket_id FROM payment_a WHERE payment_date >= now() - interval '1 day' LIMIT 100 LOOP
      SELECT COALESCE(SUM(amount),0) INTO after_sum FROM payment_a WHERE ticket_id = rec.ticket_id;
      -- fetch previous stored total (if any)
      SELECT total_amount INTO before_sum FROM ticket_a WHERE ticket_id = rec.ticket_id;
      IF before_sum IS NULL THEN before_sum := 0; END IF;
      -- update ticket
      UPDATE ticket_a SET total_amount = after_sum WHERE ticket_id = rec.ticket_id;
      INSERT INTO ticket_audit (bef_total, aft_total, key_col) VALUES (before_sum, after_sum, rec.ticket_id::text);
    END LOOP;
  ELSIF TG_OP = 'DELETE' THEN
    -- On delete we could recompute similarly; simplified here
    NULL;
  END IF;
  RETURN NULL; -- statement-level triggers return NULL
END;
$$;

-- Create trigger ON payment_a after insert OR update OR delete (statement-level)
DROP TRIGGER IF EXISTS trg_payment_a_totals ON payment_a;
CREATE TRIGGER trg_payment_a_totals
AFTER INSERT OR UPDATE OR DELETE ON parking_a.payment_a
FOR EACH STATEMENT EXECUTE FUNCTION trg_recompute_ticket_totals();

-- Small DML affecting up to 4 rows: (example)
-- INSERT INTO payment_a (ticket_id, amount, payment_date, method) VALUES (2, 4.00, now(), 'Card');
-- UPDATE payment_a SET amount = amount + 1.00 WHERE payment_id = 1;
-- DELETE FROM payment_a WHERE payment_id = 99; -- harmless if not exist

-- After DML: SELECT * FROM ticket_audit;

-- 1. Create audit table
CREATE TABLE IF NOT EXISTS ticket_audit (
    audit_id SERIAL PRIMARY KEY,
    bef_total NUMERIC(12,2),
    aft_total NUMERIC(12,2),
    changed_at TIMESTAMP DEFAULT now(),
    key_col VARCHAR(64)
);

-- 2. Create statement-level trigger function
CREATE OR REPLACE FUNCTION trg_recompute_ticket_totals()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    before_sum NUMERIC;
    after_sum NUMERIC;
BEGIN
    -- Recompute totals for all ticket_ids affected in this statement
    FOR rec IN
        SELECT DISTINCT ticket_id
        FROM payment
        WHERE payment_date >= now() - interval '7 days'  -- small recent set
    LOOP
        -- previous total
        SELECT COALESCE(total_amount,0) INTO before_sum FROM ticket WHERE ticket_id = rec.ticket_id;

        -- recompute total from payments
        SELECT COALESCE(SUM(amount),0) INTO after_sum FROM payment WHERE ticket_id = rec.ticket_id;

        -- update ticket table
        UPDATE ticket SET total_amount = after_sum WHERE ticket_id = rec.ticket_id;

        -- log audit
        INSERT INTO ticket_audit(bef_total, aft_total, key_col)
        VALUES (before_sum, after_sum, rec.ticket_id::text);
    END LOOP;

    RETURN NULL;  -- statement-level triggers must return NULL
END;
$$;

-- 3. Create the statement-level AFTER trigger
DROP TRIGGER IF EXISTS trg_payment_totals ON parking_b.payment;

CREATE TRIGGER trg_payment_totals
AFTER INSERT OR UPDATE OR DELETE ON parking_b.payment
FOR EACH STATEMENT
EXECUTE FUNCTION trg_recompute_ticket_totals();

-- 4. Small mixed DML affecting ≤4 rows
-- Insert new payments (2 rows)
INSERT INTO parking_b.payment(ticket_id, amount, payment_date, method)
VALUES
(5, 2.51, now(), 'Cash'),
(6, 3.76, now(), 'Card');

-- Update payment (1 row)
UPDATE parking_b.payment SET amount = amount + 1.00 WHERE payment_id = 1;

-- Delete payment (1 row)
DELETE FROM parking_b.payment WHERE payment_id = 99;  -- harmless if not exist

-- 5. Verify audit log
SELECT * FROM ticket_audit ORDER BY changed_at DESC;


-- B8: Recursive Hierarchy Roll-Up (HIER table)

CREATE TABLE IF NOT EXISTS hier (
  parent_id INT,
  child_id INT,
  PRIMARY KEY (parent_id, child_id)
);

-- Insert 6-8 rows forming a 3-level hierarchy
INSERT INTO hier (parent_id, child_id) VALUES
(1,2),
(1,3),
(2,4),
(2,5),
(3,6),
(3,7) ON CONFLICT DO NOTHING;

-- Recursive CTE to roll up (child, root, depth)
-- WITH RECURSIVE r AS (
--   SELECT child_id AS node, parent_id AS parent, 1 AS depth, child_id AS root FROM hier
--   UNION ALL
--   SELECT h.child_id, h.parent_id, r.depth + 1, r.root FROM hier h JOIN r ON h.child_id = r.parent
-- )
-- SELECT * FROM r;

-- Example join to ticket_a: (this is domain-specific; we'll join on child_id ~ ticket_id for demo)
-- WITH RECURSIVE cte AS (
--   SELECT parent_id, child_id, 1 AS depth FROM hier WHERE parent_id = 1
--   UNION ALL
--   SELECT h.parent_id, h.child_id, cte.depth + 1 FROM hier h JOIN cte ON h.parent_id = cte.child_id
-- )
-- SELECT t.ticket_id, cte.parent_id AS root, cte.depth FROM ticket_a t JOIN cte ON t.ticket_id = cte.child_id;

-- Adjust join keys to your domain as needed.


-- 1. Create hierarchy table
CREATE TABLE IF NOT EXISTS hier (
    parent_id INT,
    child_id INT,
    PRIMARY KEY (parent_id, child_id)
);

Step 2: Insert 6–8 rows forming a 3-level hierarchy
INSERT INTO hier (parent_id, child_id) VALUES
(1,2),
(1,3),
(2,4),
(2,5),
(3,6),
(3,7) ON CONFLICT DO NOTHING;

-- This forms: Level 1: 1 → 2, 3, Level 2: 2 → 4, 5 ; 3 → 6, 7, and Total rows inserted = 6

-- 3. Recursive CTE for hierarchy roll-up
WITH RECURSIVE hier_cte AS (
    -- Base level: immediate children of root nodes
    SELECT child_id, parent_id AS root_id, 1 AS depth
    FROM hier
    WHERE parent_id = 1  -- choose a root node
    UNION ALL
    -- Recursive step: find children of children
    SELECT h.child_id, cte.root_id, cte.depth + 1
    FROM hier h
    JOIN hier_cte cte ON h.parent_id = cte.child_id
)
SELECT *
FROM hier_cte
ORDER BY depth, child_id;

-- 4. Join with ticket to compute roll-ups
WITH RECURSIVE hier_cte AS (
    SELECT child_id, parent_id AS root_id, 1 AS depth
    FROM hier
    WHERE parent_id = 1
    UNION ALL
    SELECT h.child_id, cte.root_id, cte.depth + 1
    FROM hier h
    JOIN hier_cte cte ON h.parent_id = cte.child_id
)
SELECT t.ticket_id, cte.root_id, cte.depth, t.total_amount
FROM ticket t
JOIN hier_cte cte ON t.ticket_id = cte.child_id
ORDER BY cte.depth, t.ticket_id;


-- B9: Mini-Knowledge Base (TRIPLE table) and transitive inference

CREATE TABLE IF NOT EXISTS triple (
  s TEXT,
  p TEXT,
  o TEXT
);

-- Insert a small set of facts (<=10)
INSERT INTO triple (s,p,o) VALUES
('EV','isA','Vehicle') ON CONFLICT DO NOTHING,
('Car','isA','Vehicle') ON CONFLICT DO NOTHING,
('Sedan','isA','Car') ON CONFLICT DO NOTHING,
('Hatchback','isA','Car') ON CONFLICT DO NOTHING,
('Truck','isA','Vehicle') ON CONFLICT DO NOTHING,
('ElectricCar','isA','EV') ON CONFLICT DO NOTHING;

-- Transitive closure for isA* (up to 10 rows)
-- WITH RECURSIVE isa(s,o) AS (
--   SELECT s,o FROM triple WHERE p='isA'
--   UNION
--   SELECT t.s, isa.o FROM triple t JOIN isa ON t.o = isa.s WHERE t.p='isA'
-- )
-- SELECT * FROM isa LIMIT 10;

-- This yields inferred type relationships.

-- 1. Create triple table
CREATE TABLE IF NOT EXISTS triple (
    s VARCHAR(64),
    p VARCHAR(64),
    o VARCHAR(64),
    PRIMARY KEY (s, p, o)
);

-- 2. Insert 6–8 domain facts (≤10 rows total)
INSERT INTO triple (s,p,o) VALUES
('EV','isA','Vehicle') ON CONFLICT DO NOTHING,
('Car','isA','Vehicle') ON CONFLICT DO NOTHING,
('Sedan','isA','Car') ON CONFLICT DO NOTHING,
('Hatchback','isA','Car') ON CONFLICT DO NOTHING,
('Truck','isA','Vehicle') ON CONFLICT DO NOTHING,
('ElectricCar','isA','EV') ON CONFLICT DO NOTHING;

-- 3. Recursive transitive closure query (isA*)
WITH RECURSIVE isa(s, o, depth, label) AS (
    -- Base facts (direct isA)
    SELECT s, o, 1 AS depth, 'base' AS label
    FROM triple
    WHERE p = 'isA'
    
    UNION ALL
    
    -- Recursive step: infer transitive isA
    SELECT t.s, isa.o, isa.depth + 1, 'inferred' AS label
    FROM triple t
    JOIN isa ON t.o = isa.s
    WHERE t.p = 'isA'
)
SELECT *
FROM isa
LIMIT 10;
-- Columns returned: s → subject, o → object (type), and depth → number of hops in the hierarchy
-- label → 'base' for explicit facts, 'inferred' for transitive facts

-- 4. Optional cleanup to keep total committed rows ≤10
-- Only needed if you want to remove temporary demo facts:
DELETE FROM triple WHERE s IN ('ElectricCar','Sedan','Hatchback') AND o IN ('EV','Car');

-- B10: Business Limit Alert (function + trigger)

CREATE TABLE IF NOT EXISTS business_limits (
  rule_key TEXT PRIMARY KEY,
  threshold NUMERIC(12,2),
  active CHAR(1) CHECK (active IN ('Y','N')) DEFAULT 'Y'
);

-- Seed exactly one active rule (example: max single payment amount)
INSERT INTO business_limits (rule_key, threshold, active) VALUES ('MAX_SINGLE_PAYMENT', 100.00, 'Y') ON CONFLICT DO NOTHING;

-- Function to decide alert
CREATE OR REPLACE FUNCTION fn_should_alert_payment(p_ticket INT, p_amount NUMERIC) RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
  th NUMERIC;
BEGIN
  SELECT threshold INTO th FROM business_limits WHERE rule_key = 'MAX_SINGLE_PAYMENT' AND active = 'Y';
  IF th IS NULL THEN RETURN 0; END IF;
  IF p_amount > th THEN RETURN 1; ELSE RETURN 0; END IF;
END;
$$;

-- Trigger function on payment_a BEFORE INSERT OR UPDATE
CREATE OR REPLACE FUNCTION trg_payment_business_limit() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF fn_should_alert_payment(NEW.ticket_id, NEW.amount) = 1 THEN
    RAISE EXCEPTION 'Business limit violated: amount % exceeds threshold', NEW.amount;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_payment_business_limit ON payment_a;
CREATE TRIGGER trg_payment_business_limit
BEFORE INSERT OR UPDATE ON payment_a
FOR EACH ROW EXECUTE FUNCTION trg_payment_business_limit();

-- Test cases: two failing and two passing DMLs (wrap failing in DO blocks or run separately and rollback failing cases)
-- Example passing:
-- INSERT INTO payment_a (ticket_id, amount, payment_date, method) VALUES (1, 50.00, now(), 'Card');
-- Example failing:
-- INSERT INTO payment_a (ticket_id, amount, payment_date, method) VALUES (1, 150.00, now(), 'Card'); -- should raise exception

-- 1. Create business_limits table and seed one active rule
CREATE TABLE IF NOT EXISTS business_limits (
    rule_key VARCHAR(64) PRIMARY KEY,
    threshold NUMERIC(12,2),
    active CHAR(1) CHECK (active IN ('Y','N')) DEFAULT 'Y'
);

-- Seed exactly one active rule: max single payment amount
INSERT INTO business_limits (rule_key, threshold, active)
VALUES ('MAX_SINGLE_PAYMENT', 100.00, 'Y')
ON CONFLICT DO NOTHING;

-- 2. Function to check limit violation
CREATE OR REPLACE FUNCTION fn_should_alert_payment(p_ticket INT, p_amount NUMERIC)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    th NUMERIC;
BEGIN
    -- Fetch active threshold
    SELECT threshold INTO th
    FROM business_limits
    WHERE rule_key = 'MAX_SINGLE_PAYMENT' AND active = 'Y';

    IF th IS NULL THEN
        RETURN 0;  -- no rule active
    END IF;

    -- Return 1 if amount exceeds threshold
    IF p_amount > th THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$$;

-- 3. Trigger function on payment BEFORE INSERT OR UPDATE
CREATE OR REPLACE FUNCTION trg_payment_business_limit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF fn_should_alert_payment(NEW.ticket_id, NEW.amount) = 1 THEN
        RAISE EXCEPTION 'Business limit violated: amount % exceeds threshold', NEW.amount;
    END IF;
    RETURN NEW;
END;
$$;

-- Drop old trigger if exists
DROP TRIGGER IF EXISTS trg_payment_business_limit ON payment;

-- Create BEFORE INSERT OR UPDATE trigger
CREATE TRIGGER trg_payment_business_limit
BEFORE INSERT OR UPDATE ON payment
FOR EACH ROW
EXECUTE FUNCTION trg_payment_business_limit();

-- 4. Demonstrate DML (2 passing, 2 failing)
-- Failing inserts (wrapped in DO block to rollback safely)
DO $$
BEGIN
    -- Failing case 1: amount exceeds 100
    BEGIN
        INSERT INTO payment(ticket_id, amount, payment_date, method)
        VALUES (1, 150.00, now(), 'Card');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure: %', SQLERRM;
    END;

    -- Failing case 2: another violation
    BEGIN
        INSERT INTO payment(ticket_id, amount, payment_date, method)
        VALUES (2, 200.00, now(), 'Cash');
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Expected failure: %', SQLERRM;
    END;
END $$;
-- Passing inserts (committed)
INSERT INTO payment(ticket_id, amount, payment_date, method)
VALUES
(1, 50.00, now(), 'Card'),
(2, 75.00, now(), 'Cash');

-- Total committed rows ≤10. Failing cases are rolled back with notices, passing cases succeed

-- End of Script - Notes

-- This file is a consolidated script providing definitions and sample commands.
-- For a proper distributed-testbed, run Node_B parts on parking_b and Node_A parts on parking_a,
-- update postgres_fdw server connection options as needed (host, port, user, password, dbname),
-- and run the validation queries and captures (EXPLAIN ANALYZE, pg_prepared_xacts, pg_locks, etc.).
-- The total committed Ticket rows in these examples are 10 (5 on Node_A in ticket_a, 5 on Node_B in ticket).
-- Adjust sequences/IDs if needed in your environment.
