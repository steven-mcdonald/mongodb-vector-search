from src.ConnectionTools import mongodb_connect, get_hf_token, get_hf_api_url
from src.Helper import query


def run_query(query_text, response_limit=20):
    client = mongodb_connect()
    db = client.sample_mflix
    collection = db.movies

    # HF embedding creation endpoint
    API_URL = get_hf_api_url()
    headers = {"Authorization": f"Bearer {get_hf_token()}"}

    results = collection.aggregate([
        {"$vectorSearch": {
            "queryVector": query({"inputs": query_text}, API_URL, headers)[0],
            "path": "plot_embedding_hf",
            "numCandidates": 100,
            "limit": response_limit,
            "index": "PlotSemanticSearch",
        }}
    ])

    for document in results:
        print(f'Movie Name: {document["title"]},\nMovie Plot: {document["plot"]}\n')


if __name__ == "__main__":

    run_query("imaginary characters from outer space at war")

    print()

