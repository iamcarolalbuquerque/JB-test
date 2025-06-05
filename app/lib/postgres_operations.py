import pandas as pd
import json

import psycopg2
from psycopg2 import Error


class PostgreSQLConnection:

    def __init__(self, user, password, host, port, database):
        self.conn = None
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            return self.conn
        
        except Error as e: 
            self.conn = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


def insert_user(conn, user_name, city, country, email, phone=None):
    if conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO users (user_name, city, country, email, phone) VALUES (%s, %s, %s, %s, %s);",
                    (user_name, city, country, email, phone)
                )
                conn.commit()
                return True
            
            except Error as e:
                print(f"Error inserting user data: {e}") 
                conn.rollback()
                return False
    return False

def get_dataframe_from_query(conn, query, params=None):

    if not conn:
        return pd.DataFrame()

    with conn.cursor() as cursor:
        try:
            cursor.execute(query, params)
            
            columns = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            
            df = pd.DataFrame(records, columns=columns)
            return df
        
        except Error as e:
            return pd.DataFrame()