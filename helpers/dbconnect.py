from pymongo import MongoClient
import os
from pathlib import Path
from dotenv import load_dotenv
from bson.objectid import ObjectId


class Dbconnect():
    def __init__(self):
        env_file = os.path.join(Path(__file__).parent.parent.parent, ".env")
        load_dotenv(env_file)
        self.mongo_address = os.getenv("MONGO_REMOTE")
        # env_file = os.path.join(Path(__file__).parent.parent.parent, ".env")
        self.mongo_address = "mongodb://dev:testdev@54.164.182.183:27017"
        # self.mongo_address = os.getenv("MONGO_REMOTE")

    def __enter__(self):
        self.mongo_client = MongoClient(self.mongo_address)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.mongo_client.close()

    def SaveToDB(self, data):
        self.mongo_client.assignments.companies.insert_one(data)
        print("Record Inserted")

    def get_freelance_data(self,_id):
        print("db.py",_id)
        return list(self.mongo_client.assignments.FreelanceMap.find({"_id":ObjectId(_id)}))
    def update_freelanceMap(self,_id,linkedinURL):
        print(linkedinURL)
        self.mongo_client.assignments.FreelanceMap.update_one({"_id":ObjectId(_id)}, {'$set': {'linkedin_url':linkedinURL}})