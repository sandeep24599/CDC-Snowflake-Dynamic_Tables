# CDC-Snowflake-Dynamic_Tables
Demonstration of Change Data Capture (CDC) using Snowflake dynamic tables, including real-time order tracking with automated data aggregation
# Change Data Capture with Snowflake Dynamic Tables

This repository contains a project that demonstrates Change Data Capture (CDC) using Snowflake's dynamic tables. The project simulates an order processing system where orders are continuously inserted, updated, and deleted, with dynamic tables used to maintain derived and aggregated views of the data.

## Project Overview

The project consists of two main parts:

1. **Snowflake Setup**: A series of SQL scripts to create the necessary database, tables, and dynamic tables in Snowflake.
2. **Python Script**: A script that generates mock CDC events (inserts, updates, and deletes) and pushes them to the Snowflake database.

## Prerequisites

- **Snowflake Account**: Ensure you have access to a Snowflake account with the ability to create databases and tables.
- **Python 3.x**: Install Python on your local machine.
- **Snowflake Connector for Python**: Install the Snowflake connector using pip:

  ```bash
  pip install snowflake-connector-python
