import pika
import json
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
with open("../linkeind", 'r') as myfile:
    data=myfile.read()
data=json.loads(data) 
channel.basic_publish(exchange='', routing_key='hello', body=json.dumps(data))
print("send data")
connection.close()