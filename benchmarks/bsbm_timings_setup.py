from utils.datasets import BerlinSparqlBenchmark
from utils.dbs import FusekiDB, QleverDB, BaseDB
from utils.datasets.base_dataset import QUERY_DIFFICULTY, QUERY_TYPE
from pathlib import Path
import numpy as np
from utils.datasets.base_dataset import DataTensor
from sentence_transformers import SentenceTransformer
import argparse
import json

argsc = argparse.ArgumentParser(description="Run BDSDM timings")
argsc.add_argument(
    "--max-power", type=int, default=6, help="Maximum power of 10 for dataset sizes"
)
argsc.add_argument(
    "--base-dir",
    type=str,
    default="./data",
    help="Base directory for datasets",
)
argsc.add_argument(
    "--bsbm-dir",
    type=str,
    default="./data/bsbmtools-0.2",
    help="Directory containing BSBM tools for dataset generation",
)
argsc.add_argument(
    "--use-docker",
    action="store_true",
    help="Whether to use Docker for dataset generation",
    default=False,
)
args = argsc.parse_args()

if __name__ == "__main__":
    # %%
    args = argsc.parse_args()
    powers = np.arange(0, args.max_power + 1)  # extend on a more powerful machine
    sizes = 10**powers

    # %%
    datasets: dict[int, BerlinSparqlBenchmark] = {}
    raw_sizes: dict[int, int] = {}
    for power, size in zip(powers, sizes):
        print(f"Running BDSDM generation for size {size}...")
        dataset = BerlinSparqlBenchmark(
            base_dir=Path(args.base_dir) / f"bsbm_{power}",
            n=size,
            bsbm_directory=Path(args.bsbm_dir),
            use_docker=args.use_docker,
        )
        dataset.setup()
        datasets[power] = dataset
        # raw_sizes[power] = dataset.get_triple_count()

    # %%

    encoding_model = SentenceTransformer("all-MiniLM-L6-v2")

    # %%

    encoded_sizes: dict[int, int] = {}
    for power, size in zip(powers, sizes):
        print(f"Encoding dataset of size {size}...")
        dataset = datasets[power]
        encoded_sizes[power] = dataset.encode_streaming(encoding_model)
        #  dataset.get_triple_count(encoded=True)
    with open(Path(args.base_dir) / "bsbm_encoding_sizes.json", "w") as f:
        json.dump(encoded_sizes, f)
