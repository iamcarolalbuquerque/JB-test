import unittest
import pandas as pd
import psycopg2
from psycopg2 import Error
import os

# Import functions/classes from your db_operations.py file
from lib.postgres_operations import PostgreSQLConnection, insert_user, get_dataframe_from_query

# --- Configuration for Test Database ---
TEST_DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "default_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "default_password"),
    "host": os.getenv("POSTGRES_HOST", "db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "default_test_db")
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20)
);
"""

TRUNCATE_TABLE_SQL = "TRUNCATE TABLE users RESTART IDENTITY CASCADE;"

class TestDatabaseOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        
        print("\nSetting up test database connection...")
        try:
            cls.setup_conn = psycopg2.connect(**TEST_DB_CONFIG)
            
            with cls.setup_conn.cursor() as cursor:
                cursor.execute(CREATE_TABLE_SQL)
                cls.setup_conn.commit()
            
            print("Test database setup complete: Table 'users' ensured.")
        
        except Error as e:
            print(f"ERROR: Could not set up test database: {e}")
            raise # Stop tests if setup fails

    @classmethod
    def tearDownClass(cls):
        if cls.setup_conn:
            cls.setup_conn.close()
            print("Test database connection closed.")

    def setUp(self):
        try:
            self.db_context = PostgreSQLConnection(**TEST_DB_CONFIG)
            self.conn = self.db_context.__enter__() 
            
            with self.conn.cursor() as cursor:
                cursor.execute(TRUNCATE_TABLE_SQL)
                self.conn.commit()
            print("Table truncated for new test.")
        
        except Exception as e:
            self.fail(f"Failed to set up test environment: {e}")

    def tearDown(self):
        if hasattr(self, 'db_context'):
            self.db_context.__exit__(None, None, None)
            print("Database connection closed after test.")


    # --- Unit Tests for Database Operations ---

    def test_postgre_sql_connection_success(self):
        try:
            with PostgreSQLConnection(**TEST_DB_CONFIG) as conn:
                self.assertIsNotNone(conn)
                self.assertFalse(conn.closed) # Connection should be open
        except Error as e:
            self.fail(f"PostgreSQLConnection failed unexpectedly: {e}")

    def test_postgre_sql_connection_failure(self):
        bad_config = TEST_DB_CONFIG.copy()
        bad_config["password"] = "wrong_password"
        with self.assertRaises(Error):
            with PostgreSQLConnection(**bad_config) as conn:
                pass # Connection should fail here

    def test_insert_user_success(self):
        success = insert_user(self.conn, "Test User", "Test City", "Test Country", "test@example.com", "123-456-7890")
        self.assertTrue(success)

        # Verify insertion by querying the database
        df = get_dataframe_from_query(self.conn, "SELECT * FROM users WHERE user_name = %s", ("Test User",))
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['user_name'], "Test User")
        self.assertEqual(df.iloc[0]['email'], "test@example.com")

    def test_get_dataframe_from_query_empty(self):
        df = get_dataframe_from_query(self.conn, "SELECT * FROM users;")
        self.assertTrue(df.empty)
        self.assertEqual(len(df.columns), 6) # user_id, user_name, city, country, email, phone

    def test_get_dataframe_from_query_with_data(self):
        insert_user(self.conn, "User1", "City1", "Country1", "u1@e.com")
        insert_user(self.conn, "User2", "City2", "Country2", "u2@e.com")

        df = get_dataframe_from_query(self.conn, "SELECT user_name, city FROM users;")
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)
        self.assertEqual(len(df.columns), 2)
        self.assertIn('user_name', df.columns)
        self.assertIn('city', df.columns)
        self.assertEqual(df.iloc[0]['user_name'], "User1")
        self.assertEqual(df.iloc[1]['city'], "City2")

    def test_get_dataframe_from_query_error_handling(self):
        df = get_dataframe_from_query(self.conn, "SELECT non_existent_column FROM users;")
        self.assertTrue(df.empty) # Should return empty DataFrame on error

    

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)