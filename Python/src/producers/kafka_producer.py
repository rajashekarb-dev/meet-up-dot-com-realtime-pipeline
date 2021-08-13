import time
import requests
from kafka import KafkaProducer
from json import dumps
import json

# Globals
meetup_dot_com_rsvp_stream_api_url = "https://stream.meetup.com/2/rsvps"
kafka_topic_name = "meetuprsvptopic"
kafka_bootstrap_server = "localhost:9092"

if __name__ == "__main__":
    print("***** Kafka Producer Application Started *****")

    try:
        kafka_producer_obj = KafkaProducer(bootstrap_servers=kafka_bootstrap_server, value_serializer=lambda x: dumps(x).encode('utf-8'))
    except Exception as ex:
        print("Please run your Kafka zookeeper and server to get started to publishes messages to cluster")

    while True:
        try:
            stream_api_response = requests.get(meetup_dot_com_rsvp_stream_api_url, stream=True)
            if stream_api_response.status_code == 200:
                for api_response_message in stream_api_response.iter_lines():
                    # RAW string from the response
                    print("Message received: ")
                    print(api_response_message)
                    print(type(api_response_message))

                    # Converting response string to JSON object
                    api_response_message = json.loads(api_response_message)
                    print("Message to be sent: ")
                    print(api_response_message)
                    print(type(api_response_message))

                    kafka_producer_obj.send(kafka_topic_name, api_response_message)
                    time.sleep(1)
            else:
                print("Unable to fetch data")
        except Exception as ex:
            print("Connection to meetup stream api could not be established...")