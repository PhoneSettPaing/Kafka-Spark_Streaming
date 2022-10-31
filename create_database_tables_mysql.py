import mysql.connector

# Connect to MySQL
connection = mysql.connector.connect(user='root',
                                     password='steven',
                                     host='localhost',
                                     database='mysql')
# Create cursor
cursor = connection.cursor()
# Create Database
cursor.execute('CREATE DATABASE IF NOT EXISTS sales_db')
print('sales_db database created in mysql')
# Close connection to default database in MySQL
connection.close()

# Connect to MySQL sales_db_test database
conn = mysql.connector.connect(user='root',
                               password='steven',
                               host='localhost',
                               database='sales_db')
# Create new cursor
cur = conn.cursor()

# Create Table sales
SQL = """CREATE TABLE IF NOT EXISTS sales(
Sale_ID int,
Product varchar(40),
Quantity_Sold int,
Each_Price float,
Sales float,
Date date,
Day int,
Month int,
Year int)"""
cur.execute(SQL)
print('sales table created')

# Create Table stocks
SQL = """CREATE TABLE IF NOT EXISTS stocks(
Product varchar(40),
Stock_Quantity int,
Total_Quantity_Sold int)"""
cur.execute(SQL)
print('stocks table created')
conn.close()
