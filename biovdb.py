import shelve
import os
import itertools
import tqdm
import sys

import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
import numpy as np
from qdrant_client.models import PointStruct

CACHE_PATH = "~/.local/share/biovdb"
QDRANT_URL = "http://localhost:6333"

def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch

def read_dataset(path):
    it = pd.read_csv(path, chunksize=100)
    chunk = next(it)
    n_meta = 8
    N = chunk.shape[1] - n_meta
    yield chunk.columns[n_meta:]

    it = itertools.chain([chunk], it)
    for chunk in it:
        metadata = chunk.iloc[:, :n_meta]
        data = chunk.iloc[:, n_meta:].fillna(-1)
        for i in range(metadata.shape[0]):
            md = metadata.iloc[i, :].to_dict()
            md = {k: (v if not pd.isnull(v) else None) for k, v in md.items()}
            x = data.iloc[i, :].to_list()
            yield md, x

class Client(object):
    def __init__(self):
        os.makedirs(os.path.expanduser(CACHE_PATH), exist_ok=True)
        self._cnames = shelve.open(os.path.join(os.path.expanduser(CACHE_PATH), "cnames"))
        self._cx = QdrantClient(url=QDRANT_URL)

    def create_collection(self, key: str, path: str):
        it = read_dataset(path)
        columns = next(it)
        N = len(columns)
        self._cnames[key] = columns

        # Check if the collection exists
        try:
            self._cx.describe_collection(collection_name=key)
            collection_exists = True
        except Exception as e:
            # Handle specific exception if possible
            collection_exists = False

        if not collection_exists:
            self._cx.create_collection(
                collection_name=key,
                vectors_config=VectorParams(size=N, distance=Distance.EUCLID),
            )

        with tqdm.tqdm(desc=key) as pbar:
            for chunk in batched(it, 10):
                points = [
                    PointStruct(
                        id=int(metadata["GSM"][3:]),
                        vector=data,
                        payload=metadata
                    )
                    for metadata, data in chunk
                ]

                self._cx.upsert(
                    collection_name=key,
                    points=points
                )

                pbar.update(len(points))

if __name__ == "__main__":
    paths = sys.argv[1:]
    
    client = Client()

    for path in paths:
        platform_name = os.path.splitext(os.path.splitext(os.path.basename(path))[0])[0]
        client.create_collection(platform_name, path)

