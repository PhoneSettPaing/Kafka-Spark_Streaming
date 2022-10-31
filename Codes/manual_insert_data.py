from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# MySQL connection variables
mysql_host_name = 'localhost'
mysql_port_no = '3306'
mysql_database_name = 'sales_db'
mysql_driver_class = 'com.mysql.cj.jdbc.Driver'
mysql_table_name_1 = "sales"
mysql_table_name_2 = "stocks"
mysql_user_name = 'root'
mysql_password = 'steven'
mysql_jdbc_url = 'jdbc:mysql://' + mysql_host_name + ':' + mysql_port_no + '/' + mysql_database_name

if __name__ == '__main__':
    print('Data insertion Started ...')

    # Create spark session
    spark = SparkSession\
        .builder\
        .appName('PySpark Structured Streaming')\
        .master('local[*]') \
        .config('spark.jars', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                              'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar')\
        .config('spark.executor.extraClassPath', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                                 'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar')\
        .config('spark.executor.extraLibrary', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                               'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar')\
        .config('spark.driver.extraClassPath', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                               'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Import sales and stocks data
    filepath1 = "/home/steven/projects/pyspark/data/sales/Processed_Data.csv"
    sales_df = spark.read.csv(filepath1, header=True, inferSchema=True)
    filepath2 = "/home/steven/projects/pyspark/data/sales/Stock_Quantity.csv"
    stocks_df = spark.read.csv(filepath2, header=True, inferSchema=True)

    # Splitting Sale_Date Column
    sales_df_split = sales_df\
        .withColumn("Date", to_date(col("Sale_Date"), "yyyy-MM-dd"))\
        .withColumn("Day", split(col("Date"), "-").getItem(2))\
        .withColumn("Month", split(col("Date"), "-").getItem(1))\
        .withColumn("Year", split(col("Date"), "-").getItem(0))\
        .drop("Sale_Date")
    print('Printing Schema of sales_df_split: ')
    sales_df_split.show()

    # Dropping unnecessary column for stocks table
    stocks_df_drop = sales_df\
        .drop("Sale_ID")\
        .drop("Each_Price")\
        .drop("Sale_Date")\
        .drop("Sales")

    # Join with stocks_df
    stocks_df_join = stocks_df.join(stocks_df_drop, on="Product", how="inner")
    print('Printing Schema of stocks_df_join: ')
    stocks_df_join.show()

    # Grouping by Product and Stock_Quantity
    stocks_df_group = stocks_df_join\
        .groupBy("Product", "Stock_Quantity")\
        .agg({'Quantity_Sold': 'sum'})\
        .select('Product', 'Stock_Quantity', col('sum(Quantity_Sold)').alias('Total_Quantity_Sold'))
    print('Printing Schema of stocks_df_group: ')
    stocks_df_group.show()

    # Insert data into sales table
    sales_df_split.select('Sale_ID', 'Product', 'Quantity_Sold', 'Each_Price', 'Sales', 'Date', 'Day', 'Month', 'Year')\
        .write\
        .format('jdbc')\
        .mode('append')\
        .option('url', mysql_jdbc_url)\
        .option('driver', mysql_driver_class)\
        .option('dbtable', mysql_table_name_1)\
        .option('user', mysql_user_name)\
        .option('password', mysql_password)\
        .save()

    # Insert data into stocks table
    stocks_df_group.select('Product', 'Stock_Quantity', 'Total_Quantity_Sold')\
        .write\
        .format('jdbc')\
        .mode('append')\
        .option('url', mysql_jdbc_url)\
        .option('driver', mysql_driver_class)\
        .option('dbtable', mysql_table_name_2)\
        .option('user', mysql_user_name)\
        .option('password', mysql_password)\
        .save()
