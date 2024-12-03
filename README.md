# PostgreSQL Database Management with Python

## Description

This project demonstrates how to manage a PostgreSQL database using Python and psycopg2. It covers creating tables, inserting sample data, and performing CRUD operations (Create, Read, Update, Delete) with robust transaction handling. The project ensures security through parameterized queries, preventing SQL injection and maintaining data integrity through transaction management.

## Features

- Automatically creates and populates a PostgreSQL database with sample data.
- Manages three tables: Product, Depot, and Stock.
- Perform CRUD operations:
  - Insert new records.
  - Update existing records.
  - Delete records with cascading effects.
- Ensures transaction integrity with commit and rollback mechanisms.
- Prevents SQL injection using parameterized queries.

## Prerequisites

- Python 3.6 or later
- PostgreSQL installed and running
- Python packages:
  - psycopg2
- A dbConfig.py file with the following structure:
  ```python
  DB_CONFIG = {
      'user': 'your_username',
      'password': 'your_password',
      'host': 'your_host',
      'port': 'your_port',
      'dbname': 'your_database_name'
  }
  DB_NAME = "your_database_name"
