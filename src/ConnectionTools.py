from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json


def mongodb_connect(config_path="./config.json"):
    with open(config_path) as f:
        conf = json.load(f)
    cluster = conf['mongodb']['cluster']
    username = conf['mongodb']['username']
    password = conf['mongodb']['password']
    connection_uri = f"mongodb+srv://{username}:{password}@{cluster}.goq6qwm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    # Create a new client and connect to the server
    client = MongoClient(connection_uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client


def get_hf_token(config_path="./config.json"):
    with open(config_path) as f:
        conf = json.load(f)
    hf_token = conf['huggingface']['hf_token']
    return hf_token


def get_hf_api_url(config_path="./config.json"):
    with open(config_path) as f:
        conf = json.load(f)
    hf_token = conf['huggingface']['api_url']
    return hf_token
