#!/usr/bin/env bash

docker pull qdrant/qdrant
docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/data:/qdrant/storage:z qdrant/qdrant
