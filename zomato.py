import mysql.connector
from mysql.connector import Error
from faker import Faker
import pandas as pd
import random
import streamlit as st
import matplotlib.pyplot as plt

class DatabaseHandler:
    def __init__(self, host, user, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password
            )
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()
                # Ensure database exists
                self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
                self.conn.database = database  # Switch to the specified database
            else:
                self.conn = None
                self.cursor = None
                st.error("Failed to establish a connection to the database.")
        except Error as e:
            self.conn = None
            self.cursor = None
            st.error(f"Error connecting to MySQL: {e}")

    def execute_query(self, query, params=None):
        if self.conn and self.cursor:
            try:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                self.conn.commit()
            except Error as e:
                st.error(f"Error executing query: {e}")
        else:
            st.error("Database connection is not established.")

    def fetch_all(self, query):
        if self.conn and self.cursor:
            try:
                self.cursor.execute(query)
                return self.cursor.fetchall()
            except Error as e:
                st.error(f"Error fetching data: {e}")
                return []
        else:
            st.error("Database connection is not established.")
            return []

    def close(self):
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()

# Data generation functions
def generate_customers(db_handler, count=100):
    fake = Faker()
    for _ in range(count):
        query = '''
            INSERT INTO Customers (name, email, phone, location, signup_date, is_premium, preferred_cuisine, total_orders, average_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        params = (
            fake.name(),
            fake.email(),
            fake.phone_number(),
            fake.address(),
            fake.date_this_decade(),
            fake.boolean(),
            random.choice(['Indian', 'Chinese', 'Italian', 'Mexican']),
            random.randint(0, 50),
            round(random.uniform(1, 5), 2)
        )
        db_handler.execute_query(query, params)

def generate_restaurants(db_handler, count=50):
    fake = Faker()
    for _ in range(count):
        query = '''
            INSERT INTO Restaurants (name, cuisine_type, location, owner_name, average_delivery_time, contact_number, rating, total_orders, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        params = (
            fake.company(),
            random.choice(['Indian', 'Chinese', 'Italian', 'Mexican']),
            fake.city(),
            fake.name(),
            random.randint(20, 60),
            fake.phone_number(),
            round(random.uniform(1, 5), 2),
            random.randint(0, 100),
            fake.boolean()
        )
        db_handler.execute_query(query, params)

def generate_orders(db_handler, count=200):
    fake = Faker()
    for _ in range(count):
        query = '''
            INSERT INTO Orders (customer_id, restaurant_id, order_date, delivery_time, status, total_amount, payment_mode, discount_applied, feedback_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        params = (
            random.randint(1, 100),
            random.randint(1, 50),
            fake.date_time_this_year(),
            fake.date_time_this_year(),
            random.choice(['Pending', 'Delivered', 'Cancelled']),
            round(random.uniform(10, 100), 2),
            random.choice(['Credit Card', 'Cash', 'UPI']),
            round(random.uniform(0, 10), 2),
            random.randint(1, 5)
        )
        db_handler.execute_query(query, params)

def generate_deliveries(db_handler, count=200):
    fake = Faker()
    for _ in range(count):
        query = '''
            INSERT INTO Deliveries (order_id, delivery_status, distance, delivery_time, estimated_time, delivery_fee, vehicle_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        params = (
            random.randint(1, 200),
            random.choice(['On the way', 'Delivered']),
            round(random.uniform(1, 20), 2),
            random.randint(10, 60),
            random.randint(10, 60),
            round(random.uniform(2, 10), 2),
            random.choice(['Bike', 'Car'])
        )
        db_handler.execute_query(query, params)

def init_db(db_handler):
    queries = [
        '''CREATE TABLE IF NOT EXISTS Customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(255),
            location TEXT,
            signup_date DATE,
            is_premium BOOLEAN,
            preferred_cuisine VARCHAR(50),
            total_orders INT,
            average_rating FLOAT
        )''',
        '''CREATE TABLE IF NOT EXISTS Restaurants (
            restaurant_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            cuisine_type VARCHAR(50),
            location TEXT,
            owner_name VARCHAR(255),
            average_delivery_time INT,
            contact_number VARCHAR(255),
            rating FLOAT,
            total_orders INT,
            is_active BOOLEAN
        )''',
        '''CREATE TABLE IF NOT EXISTS Orders (
            order_id INT AUTO_INCREMENT PRIMARY KEY,
            customer_id INT,
            restaurant_id INT,
            order_date DATETIME,
            delivery_time DATETIME,
            status VARCHAR(50),
            total_amount FLOAT,
            payment_mode VARCHAR(50),
            discount_applied FLOAT,
            feedback_rating INT,
            FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
            FOREIGN KEY (restaurant_id) REFERENCES Restaurants(restaurant_id)
        )''',
        '''CREATE TABLE IF NOT EXISTS Deliveries (
            delivery_id INT AUTO_INCREMENT PRIMARY KEY,
            order_id INT,
            delivery_status VARCHAR(50),
            distance FLOAT,
            delivery_time INT,
            estimated_time INT,
            delivery_fee FLOAT,
            vehicle_type VARCHAR(50),
            FOREIGN KEY (order_id) REFERENCES Orders(order_id)
        )'''
    ]
    for query in queries:
        db_handler.execute_query(query)

def run_app(db_handler):
    if not db_handler.conn or not db_handler.conn.is_connected():
        st.error("Database connection is not established. Please check your database settings.")
        return

    st.title("Zomato Data Insights")

    menu = ["View Data", "Add Data", "Update Data", "Delete Data", "Insights"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "View Data":
        table = st.selectbox("Select Table", ["Customers", "Restaurants", "Orders", "Deliveries"])
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table}", db_handler.conn)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error fetching data from {table}: {e}")

    elif choice == "Add Data":
        st.subheader("Add New Records")
        table = st.selectbox("Select Table", ["Customers", "Restaurants", "Orders", "Deliveries"])

        if table == "Customers":
            name = st.text_input("Name")
            email = st.text_input("Email")
            phone = st.text_input("Phone")
            location = st.text_input("Location")
            signup_date = st.date_input("Signup Date")
            is_premium = st.checkbox("Is Premium?")
            preferred_cuisine = st.text_input("Preferred Cuisine")
            if st.button("Add Customer"):
                query = '''
                    INSERT INTO Customers (name, email, phone, location, signup_date, is_premium, preferred_cuisine, total_orders, average_rating)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                params = (name, email, phone, location, signup_date, is_premium, preferred_cuisine, 0, 0.0)
                db_handler.execute_query(query, params)
                st.success("Customer added successfully!")

    elif choice == "Insights":
        st.subheader("Data Insights")
        options = ["Peak Ordering Hours", "Top Cuisines", "Delivery Times"]
        insight_choice = st.selectbox("Choose Insight", options)

        if insight_choice == "Peak Ordering Hours":
            query = '''
                SELECT HOUR(order_date) AS Hour, COUNT(*) AS OrderCount
                FROM Orders
                GROUP BY Hour
                ORDER BY OrderCount DESC
            '''
            df = pd.read_sql_query(query, db_handler.conn)
            st.bar_chart(df.set_index("Hour"))

        elif insight_choice == "Top Cuisines":
            query = '''
                SELECT cuisine_type AS Cuisine, COUNT(*) AS Orders
                FROM Restaurants
                JOIN Orders ON Restaurants.restaurant_id = Orders.restaurant_id
                GROUP BY Cuisine
                ORDER BY Orders DESC
            '''
            df = pd.read_sql_query(query, db_handler.conn)
            st.bar_chart(df.set_index("Cuisine"))

        elif insight_choice == "Delivery Times":
            query = '''
                SELECT delivery_status AS Status, AVG(delivery_time) AS AvgTime
                FROM Deliveries
                GROUP BY Status
            '''
            df = pd.read_sql_query(query, db_handler.conn)
            st.bar_chart(df.set_index("Status"))

if __name__ == '__main__':
    db_handler = DatabaseHandler(host='localhost', user='root', password='janllyn', database='zomato')
    init_db(db_handler)
    generate_customers(db_handler)
    generate_restaurants(db_handler)
    generate_orders(db_handler)
    generate_deliveries(db_handler)
    run_app(db_handler)
    db_handler.close()

