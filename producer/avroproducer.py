import socket
import sys
from uuid import uuid4
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# python dosyaismi.py pub-topic


class Image:
    def __init__(self, img_data, img_name):
        self.img_data = img_data
        self.img_name = img_name

# callback function : Returns a dict representation of a Image instance for serialization.


def image_to_dict(img_obj, ctx):
    return dict(img_data=img_obj.img_data, img_name=img_obj.img_name)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    args = sys.argv[1:]
    schema_str = """
    {
        "namespace": "confluent.io.examples.serialization.avro",
        "name": "Image",
        "type": "record",
        "fields": [
            {"name": "img_data", "type": "bytes"},
            {"name": "img_name", "type": "string"}
        ]
    }
    """

    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_str=schema_str,
                                     schema_registry_client=schema_registry_client,
                                     to_dict=image_to_dict)
    producer_conf = {'bootstrap.servers': 'localhost:9092,localhost:9092',
                     'client.id': socket.gethostname(),
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}
    producer = SerializingProducer(producer_conf)

    while True:
        producer.poll(0.0)
        try:
            img_file = open('image.jpg', 'rb')
            img_data = img_file.read()
            img_name = input("input image name")
            img_obj = Image(img_data, img_name)
            producer.produce(topic=args[0], key=str(
                uuid4()), value=img_obj, on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


main()
