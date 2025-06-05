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


# --- Integration Tests for Data Analysis Logic (using loaded DataFrame) ---

    def test_distinct_users_count(self):
        """Test counting distinct user names."""
        insert_user(self.conn, "Alice", "NYC", "USA", "alice@example.com")
        insert_user(self.conn, "Bob", "LDN", "UK", "bob@example.com")
        insert_user(self.conn, "Alice", "PAR", "FRA", "alice2@example.com") # Duplicate name

        df = get_dataframe_from_query(self.conn, "SELECT * FROM users;")
        distinct_names_count = df['user_name'].nunique()
        self.assertEqual(distinct_names_count, 2) # Alice, Bob

    def test_null_phone_users_count(self):
        """Test counting users with null phone numbers."""
        insert_user(self.conn, "User A", "City X", "C1", "a@e.com", "123")
        insert_user(self.conn, "User B", "City Y", "C2", "b@e.com", None)
        insert_user(self.conn, "User C", "City Z", "C3", "c@e.com", "456")
        insert_user(self.conn, "User D", "City W", "C4", "d@e.com", None)

        df = get_dataframe_from_query(self.conn, "SELECT * FROM users;")
        null_phone_count = df['phone'].isnull().sum()
        self.assertEqual(null_phone_count, 2)

    def test_users_grouped_by_country(self):
        """Test grouping users by country."""
        insert_user(self.conn, "A", "C1", "USA", "a@e.com")
        insert_user(self.conn, "B", "C2", "UK", "b@e.com")
        insert_user(self.conn, "C", "C3", "USA", "c@e.com")
        insert_user(self.conn, "D", "C4", "France", "d@e.com")
        insert_user(self.conn, "E", "C5", "USA", "e@e.com")

        df = get_dataframe_from_query(self.conn, "SELECT * FROM users;")
        users_by_country = df.groupby('country')['user_id'].count().reset_index()
        users_by_country.columns = ['Country', 'User Count']

        expected_data = [
            {'Country': 'France', 'User Count': 1},
            {'Country': 'UK', 'User Count': 1},
            {'Country': 'USA', 'User Count': 3}
        ]
        expected_df = pd.DataFrame(expected_data).sort_values(by='Country').reset_index(drop=True)
        # Use .equals() for DataFrame comparison
        pd.testing.assert_frame_equal(users_by_country.sort_values(by='Country').reset_index(drop=True), expected_df)

    def test_city_users_list_dataframe_structure(self):
        """Test the transformation to city and list of users DataFrame."""
        insert_user(self.conn, "User1", "CityA", "C1", "u1@e.com")
        insert_user(self.conn, "User2", "CityB", "C2", "u2@e.com")
        insert_user(self.conn, "User3", "CityA", "C1", "u3@e.com")

        users_df = get_dataframe_from_query(self.conn, "SELECT * FROM users;")

        city_users_df = users_df.groupby('city').apply(
            lambda x: x[['user_id', 'user_name', 'email']].to_dict(orient='records')
        ).reset_index(name='users')

        self.assertFalse(city_users_df.empty)
        self.assertIn('city', city_users_df.columns)
        self.assertIn('users', city_users_df.columns)
        self.assertEqual(len(city_users_df), 2) # CityA, CityB

        # Check content for CityA
        city_a_row = city_users_df[city_users_df['city'] == 'CityA'].iloc[0]
        self.assertEqual(len(city_a_row['users']), 2)
        # Sort for consistent comparison as dict order is not guaranteed
        sorted_users_a = sorted(city_a_row['users'], key=lambda d: d['user_id'])
        self.assertEqual(sorted_users_a[0]['user_name'], "User1")
        self.assertEqual(sorted_users_a[1]['user_name'], "User3")
        self.assertIn('email', sorted_users_a[0]) 

    

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)