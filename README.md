# Dense Vectors in KGs

This repository contains an experimental fork of `Qlever` that has built-in vector search.

## Setup

Prerequisites:
* `uv`
* `docker`
* `gcc-13`/`cmake`/`Ninja

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

### Compiling qlever

Ensure that you have a recent version of `gcc`+ `cmake`installed!

```sh
cd qlever
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DLOGLEVEL=INFO -DUSE_DENSE_TENSOR_INDEX=true -DQLEVER_USE_TENSOR_BLAS=true -DUSE_PARALLEL=true -D_NO_TIMING_TESTS=ON -DCOMPILER_SUPPORTS_MARCH_NATIVE=FALSE -GNinja ..
cmake --build . -j --target qlever-index qlever-server
```
Add the executable's path to your `PATH`:
```sh
echo "\nexport PATH = \"$PWD:\$PATH\"" >> ~/.zshrc
```


### Generating the Benchmark Datasets

Please refer to the specific [instructions](benchmarks/data/README.md).


### Getting Neo4j

We also benchmark against Neo4j by converting the graph to a 


### Running the benchmarks

Now that everything is (hopefully) set up, we can run the benchmarks:

```sh
cd benchmarks
python run_bsbm_bench_full.py
python run_dbpedia_bench_full.py
```

You can use `--help` to inspect possible options w.r.t paths and other possible options. Once the benchmarks have successfully completed, the figures can be generated using the notebook in [benchmarks/eval_timings.ipynb](benchmarks/eval_timings.ipynb).


