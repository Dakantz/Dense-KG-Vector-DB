# Dense Vectors in KGs

This repository contains an experimental fork of `Qlever` that has built-in vector search.

## Setup

Prerequisites:
* `uv`
* `docker`

```sh
git submodule init
git submodule update
uv sync # assumes uv is installed
source .venc/bin/activate.fish
cd faiss
sh ./install_with_uv.sh 
```