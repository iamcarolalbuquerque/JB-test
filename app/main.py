from lib.postgres_operations import PostgreSQLConnection
from lib.postgres_operations import insert_user, get_dataframe_from_query
from psycopg2 import Error
import os

if __name__ == "__main__":
    try:
        with PostgreSQLConnection(
            user=os.getenv("POSTGRES_USER", "default_user"),
            password=os.getenv("POSTGRES_PASSWORD", "default_password"),
            host=os.getenv("POSTGRES_HOST", "db"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "default_db")
        ) as db_connection:
            
            with db_connection.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE users RESTART IDENTITY;")
                db_connection.commit()
            
            insert_user(db_connection, "Alice", "New York", "USA", "alice@example.com", "111-222-3333")
            insert_user(db_connection, "Bob", "London", "UK", "bob@example.com", "444-555-6666")
            insert_user(db_connection, "Charlie", "New York", "USA", "charlie@example.com", None)
            insert_user(db_connection, "David", "Paris", "France", "david@example.com", "777-888-9999")
            insert_user(db_connection, "Eve", "London", "UK", "eve@example.com", None)
            insert_user(db_connection, "Frank", "New York", "USA", "frank@example.com", "000-111-2222")
            insert_user(db_connection, "Grace", "Berlin", "Germany", "grace@example.com", "333-444-5555")
            insert_user(db_connection, "Charlie", "London", "UK", "charlie_uk@example.com", "999-888-7777")
            insert_user(db_connection, "Alice", "Paris", "France", "alice_fr@example.com", "555-666-7777")

            users_df = get_dataframe_from_query(db_connection, "SELECT * FROM users;")

            if not users_df.empty:
                print("\nOriginal DataFrame Head:")
                print(users_df.head())
                print("\nOriginal DataFrame Info:")
                users_df.info()

                print("\n--- Performing Data Analysis Tests ---")

                distinct_user_names = users_df['user_name'].nunique()
                print(f"Number of distinct user_names: {distinct_user_names}")
                print(f"Total number of records (distinct user_ids): {users_df.shape[0]}")

                null_phone_users_count = users_df['phone'].isnull().sum()
                print(f"Number of users with null phone: {null_phone_users_count}")

                users_by_country = users_df.groupby('country')['user_id'].count().reset_index()
                users_by_country.columns = ['Country', 'User Count']
                print("\nUsers grouped by Country:")
                print(users_by_country)

                city_users_df = users_df.groupby('city').apply(
                    lambda x: x[['user_id', 'user_name', 'email']].to_dict(orient='records')
                ).reset_index(name='users')

                print("\nCity and Users List DataFrame:")
                print(city_users_df)

                if not city_users_df.empty:
                    print(f"\nType of 'users' column content in first row: {type(city_users_df.loc[0, 'users'])}")
                    print(f"Content of 'users' column in first row:\n{city_users_df.loc[0, 'users']}")
            else:
                print("No data retrieved to perform tests.")

    except Error as e:
        print(f"An error occurred during database operations: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    print("\nProgram finished.")