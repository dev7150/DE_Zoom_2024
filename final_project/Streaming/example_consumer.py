import json 
from kafka import KafkaConsumer
import psycopg2
from sqlalchemy import create_engine

if __name__ == '__main__':
    conn = psycopg2.connect(
                    host="localhost",
                    database="ny_taxi",
                    user="root",
                    password="root"
                )

    # Create a cursor object
    cur = conn.cursor()
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'mec-xdr',
        bootstrap_servers='localhost:9092',
        max_poll_records = 100,
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        auto_offset_reset='earliest'#,'smallest'
    )
    for message in consumer:
        print(json.loads(message.value))
        cur.execute("INSERT INTO public.raw_data (raw) VALUES (%s)", (str(json.loads(message.value)),))
        conn.commit()