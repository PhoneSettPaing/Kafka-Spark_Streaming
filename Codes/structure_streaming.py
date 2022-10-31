from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka connection variables
kafka_topic_name = "sales_topic"
kafka_bootstrap_servers = 'localhost:9092'

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

# Function to save into sales table
def save_to_sales(sales_df_save, epoc_id):
    db_credentials = {'user': mysql_user_name,
                      'password': mysql_password,
                      'driver': mysql_driver_class}
    sales_df_save \
        .write \
        .jdbc(url=mysql_jdbc_url,
              table=mysql_table_name_1,
              mode='append',
              properties=db_credentials)

# Function to save into stocks table
def save_to_stocks(stocks_df_save, epoc_id):
    db_credentials = {'user': mysql_user_name,
                      'password': mysql_password,
                      'driver': mysql_driver_class}
    stocks_df_save \
        .write \
        .option('truncate', 'true') \
        .jdbc(url=mysql_jdbc_url,
              table=mysql_table_name_2,
              mode='overwrite',
              properties=db_credentials)

if __name__ == '__main__':
    print('Spark Streaming Application Started ...')

    spark = SparkSession\
        .builder\
        .appName('PySpark Structured Streaming')\
        .master('local[*]') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.0') \
        .config('spark.jars', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                              'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar,'
                              'file:///home/steven/projects/pyspark/lib/spark-sql-kafka-0-10_2.12-3.2.0.jar,'
                              'file:///home/steven/projects/pyspark/lib/kafka-clients-3.2.0.jar,'
                              'file:///home/steven/projects/pyspark/lib/spark-token-provider-kafka-0-10_2.12-3.2.0.jar')\
        .config('spark.executor.extraClassPath', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                                 'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar,'
                                                 'file:///home/steven/projects/pyspark/lib/spark-sql-kafka-0-10_2.12-3.2.0.jar,'
                                                 'file:///home/steven/projects/pyspark/lib/kafka-clients-3.2.0.jar,'
                                                 'file:///home/steven/projects/pyspark/lib/spark-token-provider-kafka-0-10_2.12-3.2.0.jar') \
        .config('spark.executor.extraLibrary', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                               'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar,'
                                               'file:///home/steven/projects/pyspark/lib/spark-sql-kafka-0-10_2.12-3.2.0.jar,'
                                               'file:///home/steven/projects/pyspark/lib/kafka-clients-3.2.0.jar,'
                                               'file:///home/steven/projects/pyspark/lib/spark-token-provider-kafka-0-10_2.12-3.2.0.jar') \
        .config('spark.driver.extraClassPath', 'file:///home/steven/projects/pyspark/lib/jsr305-3.0.2.jar,'
                                               'file:///home/steven/projects/pyspark/lib/mysql-connector-java-8.0.30.jar,'
                                               'file:///home/steven/projects/pyspark/lib/spark-sql-kafka-0-10_2.12-3.2.0.jar,'
                                               'file:///home/steven/projects/pyspark/lib/kafka-clients-3.2.0.jar,'
                                               'file:///home/steven/projects/pyspark/lib/spark-token-provider-kafka-0-10_2.12-3.2.0.jar') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Construct a streaming DataFrame that reads from sales_topic
    sales_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Getting json format sales data from value pair of kafka message
    sales_df1 = sales_df.selectExpr('CAST(value AS STRING)')

    # Define a schema for the sales data
    sales_schema = StructType()\
        .add('Sale_ID', IntegerType())\
        .add('Product', StringType())\
        .add('Quantity_Sold', IntegerType())\
        .add('Each_Price', FloatType())\
        .add('Sale_Date', TimestampType())\
        .add('Sales', FloatType())

    # Putting json sales data into sales_schema structure
    sales_df2 = sales_df1 \
        .select(from_json(col('value'), sales_schema).alias('sales_data'))
    sales_df3 = sales_df2.select('sales_data.*')
    print('Printing Schema of sales_df3: ')
    sales_df3.printSchema()

    # Splitting Sale_Date Column
    sales_df_split = sales_df3\
        .withColumn("Date", to_date(col("Sale_Date"), "yyyy-MM-dd"))\
        .withColumn("Day", split(col("Date"), "-").getItem(2))\
        .withColumn("Month", split(col("Date"), "-").getItem(1))\
        .withColumn("Year", split(col("Date"), "-").getItem(0))\
        .drop("Sale_Date")
    print('Printing Schema of sales_df_split: ')
    sales_df_split.printSchema()

    # Importing stock data
    filepath = "/home/steven/projects/pyspark/data/sales/Stock_Quantity.csv"
    stocks_df = spark.read.csv(filepath, header=True, inferSchema=True)

    # Dropping unnecessary column for stocks table
    stocks_df_drop = sales_df3\
        .drop("Sale_ID")\
        .drop("Each_Price")\
        .drop("Sale_Date")\
        .drop("Sales")

    # Join with stocks_df
    stocks_df_join = stocks_df.join(stocks_df_drop, on="Product", how="inner")
    print('Printing Schema of stocks_df_join: ')
    stocks_df_join.printSchema()

    # Grouping by Product
    stocks_df_group = stocks_df_join\
        .groupBy("Product", "Stock_Quantity")\
        .agg({'Quantity_Sold': 'sum'})\
        .select('Product', 'Stock_Quantity', col('sum(Quantity_Sold)').alias('Total_Quantity_Sold'))
    print('Printing Schema of stocks_df_group: ')
    stocks_df_group.printSchema()

    # Write final result into console for debugging purpose #stocks_df_group #outputmode:'complete'
    sales_data_write_stream = sales_df_split \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode('update') \
        .option('truncate', 'false') \
        .format('console') \
        .start()

    # Write sales data to mongodb as backup
    sales_df3\
        .writeStream \
        .format("mongodb")\
        .option('checkpointLocation', '/tmp/pyspark7/')\
        .option('forceDeleteTempCheckpointLocation', 'true')\
        .option('spark.mongodb.connection.uri', 'mongodb://localhost:27017')\
        .option('spark.mongodb.database', 'sales_db')\
        .option('spark.mongodb.collection', 'sales_backup')\
        .trigger(processingTime='5 seconds') \
        .outputMode('append')\
        .start()

    # Write sales data to sales table in mysql
    sales_df_split\
        .writeStream\
        .trigger(processingTime='5 seconds')\
        .outputMode('update')\
        .foreachBatch(save_to_sales)\
        .start()

    # Write stocks data to stocks table in mysql
    stocks_df_group\
        .writeStream\
        .trigger(processingTime='5 seconds')\
        .outputMode('complete')\
        .option('truncate', 'true')\
        .foreachBatch(save_to_stocks)\
        .start()

    sales_data_write_stream.awaitTermination()
    print('Pyspark Structured Streaming with Kafka Application Completed.')

