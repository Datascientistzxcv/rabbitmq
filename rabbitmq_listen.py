#!/usr/bin/env python
import pika, sys, os
import json
from helpers.dbconnect import Dbconnect
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from itertools import product
import re 

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
            # print("mq",queue_data["_id"])
            data=get_freelance_from_db(queue_data["_id"]["$oid"])
            if len(data)!=0:
                # print(queue_data.keys())
                name_ratio=fuzz.token_sort_ratio(data[0]["name"],queue_data["Name"])
                # skills_ratio=fuzz.token_sort_ratio(data[0]["skills"],queue_data["Skillset"])
                # projects_ratio=fuzz.token_sort_ratio(data[0]["projects"],queue_data["projects"])
                location_ratio=fuzz.token_sort_ratio(data[0]["location"],queue_data["Location"])
                skills_dict={}
                for first, second in product(data[0]["skills"], queue_data["Skillset"]):
                    skills_ratio=fuzz.token_sort_ratio(first, second)
                    if skills_ratio>=80:
                        skills_dict.update({f"{first}_{second}":skills_ratio})
                # print("skills:",skills_dict)
                data[0]["projects"].append({"company":data[0]["company"],"position":data[0]["title"]})
                company_name_ratio=0
                for first, second in product(data[0]["projects"],queue_data["experience"]):
                    company_name_ratio=fuzz.token_sort_ratio(first["company"], second["company_name"])
                    try:
                        position_ratio=fuzz.token_sort_ratio(first["position"], second["designation"])
                        description_ratio=fuzz.token_sort_ratio(first["description"],second["description"])
                        duration_ratio=fuzz.token_sort_ratio(first["duration"],second["duration"][0])
                    except:pass

                    if company_name_ratio>=70 and name_ratio>=70:
                        with Dbconnect() as client:
                            client.update_freelanceMap(data[0]["_id"],queue_data["Profile Url"])
                            break
                    else:
                        if (position_ratio>=70 or description_ratio>=70 or duration_ratio>=70) and (company_name_ratio>=70 or name_ratio>=70):
                            with Dbconnect() as client:
                                client.update_freelanceMap(data[0]["_id"],queue_data["Profile Url"])
                                break
                
                for first, second in product(re.split(",|;",data[0]["education"]),queue_data["Education"]):
                    education_ratio=fuzz.token_sort_ratio(first,second["degree_name"])
                    if education_ratio>=70 and (company_name_ratio>=70 or name_ratio>=70):
                        with Dbconnect() as client:
                            client.update_freelanceMap(data[0]["_id"],queue_data["Profile Url"])
                            break
                for first, second in product(data[0]["social_urls"],queue_data["contacts"][0]["website"]):
                    social_url_ratio=fuzz.token_sort_ratio(first,second)
                    if social_url_ratio>=70 and (company_name_ratio>=70 or name_ratio>=70):
                        with Dbconnect() as client:
                            client.update_freelanceMap(data[0]["_id"],queue_data["Profile Url"])
                            break
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