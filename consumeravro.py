from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
# from confluent_kafka.cimpl import TopicPartition

c = AvroConsumer(
    {'bootstrap.servers': '192.168.25.163:19092',
     'group.id': 'cgroudid-4',
     'schema.registry.url': 'http://192.168.25.163:7070',
     "api.version.request": True})
c.subscribe(['job_entity'])
running = True
while running:
    msg = None
    try:
        msg = c.poll(10)
        print(msg);
        if msg:
            if not msg.error():
                print(msg.value())
                print(msg.key())
                print(msg.partition())
                print(msg.offset())
                c.commit(msg)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
        else:
            print("No Message!! Happily trying again!!")
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False
c.commit()
c.close()
