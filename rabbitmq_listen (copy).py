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
            data=get_freelance_from_db(queue_data["_id"]["$oid"])
            

            if len(data)!=0:
                data[0]["projects"].append({"company":data[0]["company"],"position":data[0]["title"]})
                name_ratio=fuzz.token_sort_ratio(data[0]["name"],queue_data["Name"])
                location_ratio=fuzz.token_sort_ratio(data[0]["location"],queue_data["Location"])
                company_ratio,position_ratio,description_ratio,duration_ratio=experience_ratios(data,queue_data)
                print(company_ratio)
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    def skills_ratios(mongo_data:list,queue_data:dict):
        data=mongo_data
        skills_dict={}
        for first, second in product(data[0]["skills"], queue_data["Skillset"]):
            skills_ratio=fuzz.token_sort_ratio(first, second)
            if skills_ratio>=70:
                skills_dict.update({f"{first}_{second}":skills_ratio})
        return skills_dict
    def experience_ratios(mongo_data:list,queue_data:list):
        data=mongo_data
        company_name_dict={}
        position_dict={}
        description_dict={}
        duration_dict={}
        for first, second in product(data[0]["projects"],queue_data["experience"]):
            company_name_ratio=fuzz.token_sort_ratio(first["company"], second["company_name"])
            if company_name_ratio>=70:
                company_name_dict.update({f'{first["company"]}_{second["company_name"]}':company_name_ratio})
            try:
                position_ratio=fuzz.token_sort_ratio(first["position"], second["designation"])
                if position_ratio>=70:
                    position_dict.update({f'{first["position"]}_{second["designation"]}':position_ratio})
                
                description_ratio=fuzz.token_sort_ratio(first["description"],second["description"])
                if description_ratio >=70:
                    description_dict.update({f'{first["description"]}_{second["description"]}':description_ratio})
                duration_ratio=fuzz.token_sort_ratio(first["duration"],second["duration"][0])
                if duration_ratio>=70:
                    duration_dict.update({f'{first["duration"]}_{second["duration"][0]}':duration_ratio})
                
            except:pass
            return company_name_dict,position_dict,description_dict,duration_dict
    def education_ratios(mongo_data:list,queue_data:dict):
        education_dict={}
        for first, second in product(re.split(",|;",data[0]["education"]),queue_data["Education"]):
            education_ratio=fuzz.token_sort_ratio(first,second["degree_name"])
        if education_ratio>=70:
            education_dict.update({f'{first}_{second["degree_name"]}':education_ratio})
        return education_dict
    def social_url_ratios(mongo_data:list,queue_data:dict):
        social_dict={}
        data=mongo_data
        for first, second in product(data[0]["social_urls"],queue_data["contacts"][0]["website"]):
            social_url_ratio=fuzz.token_sort_ratio(first,second)
            if social_url_ratio>=70:
                social_dict.update({f'{first}_{second}':social_url_ratio})
        return social_dict
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)