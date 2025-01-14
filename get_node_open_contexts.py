import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv, dotenv_values
import time
from datetime import datetime, timezone

config = dotenv_values(".env")
elastic_cloud_id = config['ELASTIC_CLOUD_ID']
elastic_api_key  = config['ELASTIC_API_KEY']

es = Elasticsearch(cloud_id=elastic_cloud_id, api_key=elastic_api_key)

try:
    print("Timestamp,Node,OpenContexts")
    while True:
        node_stats = es.nodes.stats()
        roles = ('data_content', 'data_hot')
        valid_nodes = [node for node, data in node_stats['nodes'].items() if any(role in roles for role in data['roles'])]
        for node in valid_nodes:
            if node_stats['nodes'][node].get('indices', {}).get('search', {}).get('open_contexts', {}) is not None:
                print(f"{datetime.now(timezone.utc).isoformat()} {node},{node_stats['nodes'][node]['indices']['search']['open_contexts']}")
        time.sleep(1)
        print()
except KeyboardInterrupt:
    print("\nKeyboardInterrupt detected. Exiting gracefully.")