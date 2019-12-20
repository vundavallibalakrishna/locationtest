import io
import struct

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

# Please adjust your server and url

# KAFKA BROKER URL
consumer = Consumer({
    'bootstrap.servers': '192.168.25.163:19092',
    'group.id': 'abcd'
})

# SCHEMA URL 
register_client = CachedSchemaRegistryClient(url="http://192.168.25.163:7070")
consumer.subscribe(['job_entity'])

MAGIC_BYTES = 0


def unpack(payload):
    magic, schema_id = struct.unpack('>bi', payload[:5])

    # Get Schema registry
    # Avro value format
    if magic == MAGIC_BYTES:
        schema = register_client.get_by_id(schema_id)
        reader = DatumReader(schema)
        output = BinaryDecoder(io.BytesIO(payload[5:]))
        abc = reader.read(output)
        return abc
    # String key
    else:
        # Timestamp is inside my key
        return payload[:-8].decode()


def get_data():
    while True:
        try:
            msg = consumer.poll(10)
        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            raise SerializerError

        if msg:
            if msg.error():
                print("AvroConsumer error: {}".format(msg.error()))
                return

            key, value = unpack(msg.key()), unpack(msg.value())
            print(key, value)
        else:
            print("No Message!!")

if __name__ == '__main__':
    get_data()