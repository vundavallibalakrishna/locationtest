import io
import avro.schema
import avro.io
from kafka import KafkaConsumer
import logging
import json


logging.basicConfig(level=logging.DEBUG)

# To consume messages
CONSUMER = KafkaConsumer(client_id='my_client',
                         group_id='job4',
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         bootstrap_servers='127.0.0.1:19092',
                         api_version=(0, 10),
                         consumer_timeout_ms=10000,
                         value_deserializer=json.loads)

# SCHEMA_PATH = "JobKafkaMessage.avsc"
# SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

# print(CONSUMER.assign([0, 1, 2]))
print(CONSUMER.subscribe(["job_entity"]))

for msg in CONSUMER:
    print("printing")
    print(msg.value)
try:
    running = True
    # print(SCHEMA_PATH)
    # print(SCHEMA)
    while running:
        print("polling")
        msg = CONSUMER.poll(timeout_ms=1.0)
        print("post polling")
        if msg is None:
            print("None")
        print(msg)
finally:
        # Close down consumer to commit final offsets.
    print("closing")
    CONSUMER.close()
##
# for msg in CONSUMER:
#     print("printing")
#     print(msg.value)
#     print(" ")
#     bytes_reader = io.BytesIO(msg.value)
#     print(bytes_reader)
#     print(" ")
#     decoder = avro.io.BinaryDecoder(bytes_reader)
#     print(decoder)
#     print(" ")
#     reader = avro.io.DatumReader(SCHEMA)
#     print(reader)
#     print(" ")
#     user1 = reader.read(decoder)
#     print(user1)
#     print(" ")
#     # print user1
