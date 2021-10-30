import sys
import socket
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

# python dosyaismi.py group-id sub-topic pub-topic


class Image:
    def __init__(self, img_data=None, img_name=None):
        self.img_data = img_data
        self.img_name = img_name

    def export_here(self):
        if not(self.img_data is None):
            new_image_file = open('received_img.jpg', 'xb')
            new_image_file.write(self.img_data)


def dict_to_image(dict_obj, ctx):
    if dict_obj is None:
        return None
    return Image(dict_obj['img_data'], dict_obj['img_name'])


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

    avro_deserializer = AvroDeserializer(schema_str=schema_str,
                                         schema_registry_client=schema_registry_client,
                                         from_dict=dict_to_image)

    avro_serializer = AvroSerializer(schema_str=schema_str,
                                     schema_registry_client=schema_registry_client,
                                     to_dict=image_to_dict)

    string_deserializer = StringDeserializer('utf_8')
    string_serializer = StringSerializer('utf_8')
    producer_conf = {'bootstrap.servers': 'localhost:9092,localhost:9092',
                     'client.id': socket.gethostname(),
                     'key.serializer': string_serializer,
                     'value.serializer': avro_serializer}

    consumer_conf = {'bootstrap.servers': 'localhost:9092,localhost:9092',
                     'key.deserializer': string_deserializer,
                     'value.deserializer': avro_deserializer,
                     'group.id': args[0],
                     'auto.offset.reset': "earliest"}
    producer = SerializingProducer(producer_conf)
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([args[1]])
    while True:
        producer.poll(0.0)
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            image_obj = msg.value()
            if image_obj is not None:
                print("image record {}: imageDataType: {}\n"
                      "\imageName: {}\n"
                      .format(msg.key(), type(image_obj.img_data),
                              image_obj.img_name,))
                # image_obj.export_here() bu kod dosyayı bu klasöre kopyalar
                producer.produce(topic=args[2], key=str(
                    msg.key()), value=image_obj, on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue
    consumer.close()
    producer.flush()


main()
