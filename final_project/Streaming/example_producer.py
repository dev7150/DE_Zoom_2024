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

def get_push_reords():
    # Define function to flatten client records
    def flatten_record(server, client):
        client.pop('mac', None)
        client.pop('deviceHash', None)
        client.pop('lon', None)
        client.pop('lat', None)
        client.pop('ip4', None)
        if 'ip' in client:
            client['client_ip'] = client.pop('ip')
        client['application'] = ip_to_name.get(server['ip'], '')
        client['timestamp'] = int(time.time() * 1000)
        flat_record = json.dumps({"timestamp": client['timestamp'], "application": client['application'], "client_ip": client['client_ip'], "server_ip": server['ip'], "server_latlon":server['latlon'],**client})
        return flat_record


    # Fetch nested JSON record from Dropbox link
    response = requests.get("https://www.dropbox.com/s/iwcpg1oo59i4yrn/exfoCustosTest%20%283%29.json?dl=1")
    json_data = json.loads(response.content)

    # Extract servers and services arrays
    servers = json_data['servers']
    services = json_data['services']

    # Create dictionary mapping IP addresses to service names
    ip_to_name = {server['ip']: service['name'] for service in services for server in service['servers']}

    # Output flattened JSON record for each client record using threads
    debug = True  # Set to True to enable debug output
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_results = []
        for server in servers:
            for client in server['clients']:
                future = executor.submit(flatten_record, server, client)
                future_results.append(future)

        for future in future_results:
            flat_record = future.result()
            if debug:
                print(f"Sending record to kafka Stream: {json.loads(flat_record)}")
                data = json.loads(flat_record)
                print(data["timestamp"])
                
                producer.send('mec-xdr', flat_record)
                producer.flush()    # Wait for messages to be delivered

def main():
    while True:
        get_push_reords()
        sleep(2)

if __name__ == "__main__":
    main()