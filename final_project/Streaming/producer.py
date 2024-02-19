import json as json
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from kafka import KafkaProducer

# Set up Kafka producer
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)


#  {
#             "location_id": 3,
#             "sensing_datetime": "2024-02-08T02:55:00+00:00",
#             "sensing_date": "2024-02-08",
#             "sensing_time": "13:55",
#             "direction_1": 13,
#             "direction_2": 22,
#             "total_of_directions": 35
#         },
def get_push_reords():
    # Define function to flatten client records
    def flatten_record(server, client):
        client.pop('location_id', None)
        client.pop('sensing_datetime', None)
        client.pop('sensing_date', None)
        client.pop('sensing_time', None)
        client.pop('direction_1', None)
        if 'ip' in client:
            client['sensing_date'] = client.pop('ip')
        client['direction_2'] = ip_to_name.get(server['ip'], '')
        client['total_of_directions'] = int(time.time() * 1000)
        flat_record = json.dumps({"location_id": client['location_id'], "sensing_datetime": client['sensing_datetime'], "sensing_date": client['sensing_date'], "sensing_time": server['sensing_time'], "direction_1":server['direction_2'],**client})
        return flat_record


    # Fetch nested JSON record from Dropbox link
    response = requests.get("https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-past-hour-counts-per-minute/records?order_by=sensing_datetime%20asc&limit=20")
    json_data = json.loads(response.content)

    # # Extract servers and services arrays
    # servers = json_data['servers']
    # services = json_data['services']

    # Create dictionary mapping IP addresses to service names
    # ip_to_name = {server['ip']: service['name'] for service in services for server in service['servers']}

    # Output flattened JSON record for each client record using threads
    debug = True  # Set to True to enable debug output
    future = executor.submit(flatten_record, server, client)
    future_results.append(future)

    for future in future_results:
        flat_record = future.result()
        if debug:
            print(f"Sending record to kafka Stream: {json.loads(flat_record)}")
            data = json.loads(flat_record)
            print(data["location_id"])
            
            producer.send('mec-xdr', flat_record)
            producer.flush()    # Wait for messages to be delivered

def main():
    while True:
        get_push_reords()
        sleep(2)

if __name__ == "__main__":
    main()