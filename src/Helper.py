import requests


def find_items(collection, limit=5):
    items = collection.find().limit(limit)
    for item in items:
        print(item)


def gather_doc_list():
    return


def query(payload, API_URL, headers):
    response = requests.post(API_URL, headers=headers, json=payload)
    # trying to avoid crash
    response.raise_for_status()  # raises exception when not a 2xx response
    if response.status_code != 204:
        return response.json()
