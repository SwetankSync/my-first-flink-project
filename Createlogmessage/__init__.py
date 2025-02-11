import random
import threading
import time
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka import Consumer

def produce_message(producer,topic):
    names=["Apple","Banana","Cherry","Date","Elderberry","Fig","Grape","Honeydew","Jackfruit","Kiwi","Lemon","Mango","Nectarine","Orange","Papaya","Quince","Raspberry","Strawberry","Tangerine","Ugli","Vanilla","Watermelon","Xigua","Yellow","Zucchini"]
    while True:
        data={
            'id':random.randint(1,100),
            'name':random.choice(names),
            'timestamp':time.time(),
        }
        producer.produce(topic,str(data))
        producer.flush()



        time.sleep(1)

def consume_messages(consumer, topic):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__=="__main__":
    producer=Producer({'bootstrap.servers':'localhost:9092'})
    topic='commodity'

    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    produce_message(producer,topic)





    # producer_thread=threading.Thread(target=generate_produce_message,args=(producer,topic))
    # consumer_thread=threading.Thread(target=consume_messages,args=(consumer,topic))
    #
    # producer_thread.start()
    # consumer_thread.start()
    #
    # producer_thread.join()
    # consumer_thread.join()

