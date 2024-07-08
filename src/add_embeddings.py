from pymongo import UpdateOne, ReplaceOne
import time
import pickle

from src.ConnectionTools import mongodb_connect, get_hf_token, get_hf_api_url
from src.Helper import query


def get_list_of_docs_to_update(collection, check_if_embedding_exists=False):
    CHUNK_SIZE = 32
    if check_if_embedding_exists:
        # find documents with a plot field but no plot_embedding_hf field
        cursor = collection.find({'plot': {"$exists": True}, 'plot_embedding_hf': {"$exists": False}}, batch_size=CHUNK_SIZE)
    else:
        # find documents with a plot field
        cursor = collection.find({'plot': {"$exists": True}}, batch_size=CHUNK_SIZE)

    doc_list = []
    for i, row in enumerate(cursor):
        doc_list.append(row)

    cursor.close()

    return doc_list


def add_embeddings_to_docs(doc_list, API_URL, headers):
    st = time.time()
    batch_size = 32
    doc_count = 0
    embedded_doc_batches = []
    for i in range(0, len(doc_list), batch_size):
        doc_batch = doc_list[i:i + batch_size]
        plots_list = [x['plot'] for x in doc_batch]
        payload = {"inputs": plots_list}
        embedding_list = query(payload, API_URL, headers)
        for j, doc in enumerate(doc_batch):
            doc_batch[j]['plot_embedding_hf'] = embedding_list[j]
        embedded_doc_batches.append(doc_batch)
        doc_count += len(doc_batch)
        print(f"embeddings for {doc_count} docs generated")
        pass

    end = time.time()
    print(f"embedding runtime: {end - st}")
    print()

    # flatten the embedded chunklist
    embedded_docs = [x for batch_sublist in embedded_doc_batches for x in batch_sublist]

    with open('./data/embedded_docs.pkl', 'wb') as file:
        pickle.dump(embedded_docs, file)

    return embedded_docs


def replace_docs_in_collection(embedded_docs, collection):
    count = 0
    mapping = {}
    for doc in embedded_docs:
        mapping[doc['_id']] = doc
        count += 1
        if count % 100 == 0:
            print(f"{count} doc mappings complete")

    updates = []
    for key, value in mapping.items():
        updates.append(ReplaceOne({'_id': key}, value))

    update_count = 0
    for i in range(0, len(updates), 100):
        update = updates[i:i + 100]
        collection.bulk_write(update)
        update_count += len(update)
        print(f"updated {update_count} docs on database")


if __name__ == "__main__":

    client = mongodb_connect()
    db = client.sample_mflix
    collection = db.movies

    # get list of documents to update from the collection
    doc_list = get_list_of_docs_to_update(collection, check_if_embedding_exists=False)

    # create embeddings for
    API_URL = get_hf_api_url()
    headers = {"Authorization": f"Bearer {get_hf_token()}"}
    embedded_docs = add_embeddings_to_docs(doc_list, API_URL, headers)

    # replace docs updated with embeddings in the collection
    replace_docs_in_collection(embedded_docs, collection)

    print()
