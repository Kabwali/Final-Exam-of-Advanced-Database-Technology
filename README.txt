
## Module: Advanced Databased Technology
## Final exam: Parallel and distributed databases applied on smart parking management and ticketing system

## Overview
This project provides a comprehensive PostgreSQL-based implementation and demonstration for the SMART PARKING MANAGEMENT & TICKETING SYSTEM. 
It models a distributed database environment by splitting the dataset across two nodes: Node_A (parking_a) and Node_B (parking_b) and 
showcases advanced database management concepts inspired by Oracle-style distributed exercises. The implementation covers data fragmentation, 
cross-node joins, two-phase commit (2PC), distributed lock handling, declarative constraints, triggers for audit and automation, recursive 
hierarchy queries, and business-rule enforcement. Together, these scripts form a robust demonstration of how PostgreSQL can efficiently manage 
and synchronize distributed parking and ticketing data while maintaining consistency, integrity, and real-time responsiveness.

## Files included
The project package includes three key components designed to demonstrate and document the full PostgreSQL implementation of the SMART 
PARKING MANAGEMENT & TICKETING SYSTEM. The parkingticketing_postgres.sql file serves as the main SQL script, containing both Node_A and Node_B 
configurations along with detailed example commands for distributed operations, transactions, and integrity testing. The README.txt file provides 
clear documentation, setup instructions, and execution guidance for running and validating the scripts across the two database nodes. Lastly, the 
Screenshots folder captures the execution results and visual evidence of key tasks performed from the SQL script, ensuring transparency and 
verification of the implemented features.

## How to use
Preferred approach: Run Node_B parts on a session connected to the parking_b database, and Node_A parts on a session connected
to the parking_a database. The file is annotated with instructions where to run each section.

1. Create databases (run as a superuser):
   CREATE DATABASE parking_a;
   CREATE DATABASE parking_b;

2. Run Node_B section (connect to parking_b):
   psql -d parking_b -f parkingticketing_postgres.sql -- set ON_ERROR_STOP=1 and execute only the Node_B parts (or run full file but ignore 
   Node_A parts where appropriate)

3. Run Node_A section (connect to parking_a):
   psql -d parking_a -f parkingticketing_postgres.sql -- ensures postgres_fdw is available and can reach parking_b

## Important Notes and Adjustments

The script leverages postgres_fdw to enable seamless cross-database communication between nodes, with a server named proj_link users should 
update the host, port, database name, and user mappings to align with their local environment. It demonstrates two-phase commit (2PC) operations 
using PostgreSQL’s PREPARE TRANSACTION and COMMIT PREPARED mechanisms, showcasing distributed transaction handling when properly configured under 
a coordinated postgres_fdw setup. For performance analysis, the script contrasts parallel vs. serial aggregations by adjusting the 
max_parallel_workers_per_gather parameter and examining execution behavior through EXPLAIN (ANALYZE, BUFFERS). The dataset is intentionally 
compact, containing only 10 total Ticket rows (5 on Node_A and 5 on Node_B) to adhere to the specified row budget and simplify validation. 
Additionally, several SQL commands are provided as commented examples — users should follow the README’s step-by-step guidance to execute them, 
observe outputs, and capture screenshots as required for task evidence.


## Answers to questions might be asked next

Several enhancement options are available to extend and customize the SMART PARKING MANAGEMENT & TICKETING SYSTEM project. The SQL implementation 
can be split into two separate files—node_a.sql and node_b.sql—and packaged in a zip for easier distributed deployment. Detailed psql session commands 
and steps can be produced to guide users in executing queries and capturing screenshots for every evidence requirement. The solution can also be adapted 
to run within a single PostgreSQL database by using separate schemas instead of postgres_fdw links, simplifying setup for standalone environments. 
To support performance analysis, sample EXPLAIN ANALYZE outputs and a brief comparison report between serial and parallel runs can be generated. 
A step-by-step lab guide can be created to reproduce the full two-phase commit (2PC) in-doubt and recovery scenario using exact PostgreSQL commands. 
Additionally, the solution can be converted to Oracle PL/SQL syntax—leveraging DBLINK, DBMS_XPLAN, and DBMS_UTILITY—or exported as a JSON dataset 
for MongoDB migration, enabling flexible adaptation across database platforms.

## Tip
When validating distributed joins and cross-node work, use small datasets and explicit WHERE predicates so results and planner 
traces are easy to capture and analyze.
