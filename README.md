# BioVDB

![Alt text](https://www.frontiersin.org/files/Articles/1366273/frai-07-1366273-HTML-r1/image_m/frai-07-1366273-g004.jpg)

## Step 1 - Download data

Download data from the platform of choice (.csv.gz file) from https://huggingface.co/collections/mwinn99/biovdb-658daf0c3ceccd00f3ad63a9

GPL570: https://huggingface.co/datasets/mwinn99/GPL570

## Step 2 Start the server

Run `./start-server.sh`

## Step 3 Populate vector storage

Run `python biovdb.py /path/to/your/data/GPLxx.csv.gz /path/to/your/data/GPLyy.csv.gz`

### Please note that you can load one or more files at the same time, but code may take several hours to run depending on files size.

Examples of basic query and similarity search are available as jupyter notebooks in `biovdb\examples` directory

For information on running queries, similarity search and other features, see the official Qdrant documentation.


https://qdrant.tech/documentation/

## Citation

Winnicki MJ, Brown CA, Porter HL, Giles CB, Wren JD, **BioVDB: biological vector database for high-throughput gene expression meta-analysis**, Frontiers in Artificial Intelligence 7 (2024)

https://www.frontiersin.org/articles/10.3389/frai.2024.1366273