#!/usr/bin/env python
import pika, sys, os
import json
from helpers.dbconnect import Dbconnect


def get_freelance_from_db(_id):
    with Dbconnect() as client:
        data = client.get_freelance_data(_id)
    return data

def main():  
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    def callback(ch, method, properties, body):
        for queue_data in json.loads(body):
            print("mq",queue_data["_id"])
            data=get_freelance_from_db(queue_data["_id"]["$oid"])
            print(data)
        # print(" [x] Received %r" % (json.loads(body)[0]))

    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)