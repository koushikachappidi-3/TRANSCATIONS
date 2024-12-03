import psycopg2
from dbConfig import DB_CONFIG
from dbConfig import DB_NAME

# Function to run a database query (transaction)
def execute_transaction(query, params=None):
    try:
        # Establish connection
        connection = psycopg2.connect(**DB_CONFIG)
        connection.autocommit = False  # Disable auto-commit for transaction handling
        cursor = connection.cursor()

        # Execute query
        cursor.execute(query, params)

        # Commit transaction
        connection.commit()
        print("Transaction completed successfully.")
    except Exception as e:
        # Rollback in case of error
        connection.rollback()
        print(f"Transaction failed. Rolled back. Error: {e}")
    finally:
        # Close resources
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Define transactions
def create_and_populate_database():
    try:
        # Handle database creation separately
        connection = psycopg2.connect(
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"]
        )
        connection.autocommit = True
        cursor = connection.cursor()

        # Force disconnect all connections to the database
        cursor.execute(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{DB_NAME}'
                  AND pid <> pg_backend_pid();
                """)

        # Drop and create the database
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
        cursor.execute(f"CREATE DATABASE {DB_NAME};")
        print("Database created successfully!")

        cursor.close()
        connection.close()

        # Now create tables and populate data
        execute_transaction("""
        CREATE TABLE Product (
            prod VARCHAR(10) PRIMARY KEY,
            pname VARCHAR(50) NOT NULL,
            price NUMERIC(10, 2) NOT NULL
        );
        """)

        execute_transaction("""
        CREATE TABLE Depot (
            dep VARCHAR(10) PRIMARY KEY,
            addr VARCHAR(50) NOT NULL,
            volume INT NOT NULL
        );
        """)

        execute_transaction("""
        CREATE TABLE Stock (
            prod VARCHAR(10),
            dep VARCHAR(10),
            quantity INT NOT NULL,
            PRIMARY KEY (prod, dep),
            FOREIGN KEY (prod) REFERENCES Product (prod) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (dep) REFERENCES Depot (dep) ON DELETE CASCADE ON UPDATE CASCADE
        );
        """)

        execute_transaction("""
        INSERT INTO Product (prod, pname, price) VALUES
        ('p1', 'tape', 2.5),
        ('p2', 'tv', 250),
        ('p3', 'vcr', 80);
        """)

        execute_transaction("""
        INSERT INTO Depot (dep, addr, volume) VALUES
        ('d1', 'New York', 9000),
        ('d2', 'Syracuse', 6000),
        ('d4', 'New York', 2000);
        """)

        execute_transaction("""
        INSERT INTO Stock (prod, dep, quantity) VALUES
        ('p1', 'd1', 1000),
        ('p1', 'd2', -100),
        ('p1', 'd4', 1200),
        ('p3', 'd1', 3000),
        ('p3', 'd4', 2000),
        ('p2', 'd4', 1500),
        ('p2', 'd1', -400),
        ('p2', 'd2', 2000);
        """)

        print("Sample data inserted successfully!")

    except Exception as e:
        print(f"Error during database setup: {e}")

def delete_product(product_id):
    query = """
    BEGIN;
    DELETE FROM Stock WHERE prod = %s;
    DELETE FROM Product WHERE prod = %s;
    COMMIT;
    """
    execute_transaction(query, (product_id, product_id))

def delete_depot(depot_id):
    query = """
    BEGIN;
    DELETE FROM Stock WHERE dep = %s;
    DELETE FROM Depot WHERE dep = %s;
    COMMIT;
    """
    execute_transaction(query, (depot_id, depot_id))

def update_product_name(old_name, new_name):
    query = """
    BEGIN;
    UPDATE Product SET prod = %s WHERE prod = %s;
    UPDATE Stock SET prod = %s WHERE prod = %s;
    COMMIT;
    """
    execute_transaction(query, (new_name, old_name, new_name, old_name))

def update_depot_name(old_name, new_name):
    query = """
    BEGIN;
    UPDATE Depot SET dep = %s WHERE dep = %s;
    UPDATE Stock SET dep = %s WHERE dep = %s;
    COMMIT;
    """
    execute_transaction(query, (new_name, old_name, new_name, old_name))

def update_product_and_stock(prod, pname, price,dep, quantity):
    query = """
    BEGIN;
    INSERT INTO Product (prod, pname, price) VALUES (%s, %s, %s);
    INSERT INTO Stock (prod, dep, quantity) VALUES (%s, %s, %s);
    COMMIT;
    """
    execute_transaction(query, (prod, pname, price, prod, dep, quantity))

def update_depot_and_stock(dep, addr, volume, prod, quantity):
    query = """
    BEGIN;
    INSERT INTO Depot (dep, addr, volume) VALUES (%s, %s, %s);
    INSERT INTO Stock (prod, dep, quantity) VALUES (%s, %s, %s);
    COMMIT;
    """
    execute_transaction(query, (dep, addr, volume, prod, dep, quantity))


def print_table_records(table):
    try:
        # Use string interpolation for table name
        query = f"SELECT * FROM {table};"
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute(query)

        # Fetch and print all records
        rows = cursor.fetchall()
        print(f"Records in table {table}:")
        for row in rows:
            print(row)

        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error fetching records from {table}: {e}")

# Example Usage
# Example calls to functions

# create table and call delete product 1
create_and_populate_database()
print("===================Given input table=================================================")
print_table_records("Product")
print_table_records("Stock")
print_table_records("Depot")
print()


print("==================== Transaction 1: Delete product================================================")
delete_product("p1")
print_table_records("Product")
print_table_records("Stock")
print()

print("==================== Transaction 2: Delete depot================================================")
delete_depot("d1")
print_table_records("Depot")
print_table_records("Stock")

create_and_populate_database()
print("===================creating input table=================================================")
print_table_records("Product")
print_table_records("Stock")
print_table_records("Depot")
print()

print("==================== Transaction 3: Update product================================================")
update_product_name("p1", "pp1")
print_table_records("Product")
print_table_records("Stock")
print()

print("==================== Transaction 4: Update Depot================================================")
update_depot_name("d1", "dd1")
print_table_records("Depot")
print_table_records("Stock")
print()

create_and_populate_database()
print("===================Given input table=================================================")
print_table_records("Product")
print_table_records("Stock")
print_table_records("Depot")

print("=================== Transaction 5: Update product and stock=================================================")
update_product_and_stock("p100", "cd", 5, "d2", 50)
print_table_records("Product")
print_table_records("Stock")
print()
#
print("==================== Transaction 6: Update depot and stock================================================")
update_depot_and_stock("d100", "Chicago", 100, "p1", 100)
print_table_records("Stock")
print_table_records("Depot")
print()




