# Dense Vectors in KGs

This repository contains an experimental fork of `Qlever` that has built-in vector search.

## Setup

Prerequisites:
* `uv`
* `docker`

### Install Dependencies (Ubuntu/Debian)

```sh
apt install -y libboost1.88-dev libboost-iostreams1.88-dev libboost-random1.88-dev libboost-program-options1.88-dev libboost-url1.88-dev libboost-container1.88-dev libopenblas-dev
```


### Setup environment


```sh
git submodule init
git submodule update
git submodule sync
uv sync # assumes uv is installed
source .venv/bin/activate
cd faiss
sh ./install_with_uv.sh --cpu
```


### Generating the Benchmark Datasets

Please refer to the specific [instructions](benchmarks/data/README.md).