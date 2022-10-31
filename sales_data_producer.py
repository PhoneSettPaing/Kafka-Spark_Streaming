from kafka import KafkaProducer
import time
from json import dumps
import pandas as pd

KAFKA_TOPIC_NAME_CONS = "sales_topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ...")

    # value_serializer = lambda x: dumps(x).encode('utf-8'):
    # This parameter functions on the serialization of the data before sending it to the broker.
    # Here, we transform the data into a JSON file and encode it to UTF-8.
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    file_path = "/home/steven/projects/pyspark/data/sales/Processed_Data.csv"

    sales_df = pd.read_csv(file_path)
    # print(sales_df.head(1))

    sales_list = sales_df.to_dict(orient="records")
    # print(sales_list[0])

    for sale in sales_list:
        message = sale
        print("Message to be sent: ", message)

        # The value serializer will automatically transform data into json and encode the data.
        # Where key will be key of the topic and value will be sale in json format.
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)

        time.sleep(1)
