from utils.dbs.neo4j import Neo4JDB
from utils.datasets import BerlinSparqlBenchmark
from utils.datasets.data_tensor import DataTensor
from utils.dbs.fuseki_native import FusekiDBNative
from utils.dbs.qlever_native import QleverDBNative
from utils.datasets.base_dataset import QUERY_DIFFICULTY, QUERY_TYPE

import pandas as pd

from utils.dbs.base_docker import BaseDocker
from utils.bench import BenchmarkRunner
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer
import argparse
import logging
import os

os.environ["OPENBLAS_NUM_THREADS"] = "4"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
argsc = argparse.ArgumentParser(description="Run BSBM timings")
argsc.add_argument(
    "--max-power",
    type=int,
    default=5,
    help="Power of 10 for dataset size (e.g., 2 for 100 triples)",
)
argsc.add_argument(
    "--base-dir",
    type=str,
    default="./data",
    help="Base directory for BSBM datasets",
)
argsc.add_argument(
    "--repetitions",
    type=int,
    default=128,
    help="Number of repetitions for each benchmark",
)
argsc.add_argument(
    "--scratch-dir",
    type=str,
    default="./scratch",
    help="Scratch directory for benchmark artifacts",
)
argsc.add_argument(
    "--out-dir",
    type=str,
    default="./scratch/results/bsbm",
    help="Directory for benchmark results",
)
args = argsc.parse_args()

if __name__ == "__main__":
    # %%
    args = argsc.parse_args()
    powers = np.arange(0, args.max_power + 1)  # extend on a more powerful machine
    sizes = 10**powers
    datasets: dict[int, BerlinSparqlBenchmark] = {}

    encoding_model = SentenceTransformer("all-MiniLM-L6-v2")
    for power, size in zip(powers, sizes):
        print(f"Running BDSDM generation for size {size}...")
        dataset = BerlinSparqlBenchmark(base_dir=Path(f"./data/bsbm_{power}"), n=size)
        dataset.setup()
        dataset.encode(encoding_model)
        datasets[power] = dataset

    timings_df = pd.DataFrame()
    stats_df = pd.DataFrame()
    offset = 0

    for power, size in zip(powers, sizes):
        difficulties = [
            QUERY_DIFFICULTY.EASY,
            QUERY_DIFFICULTY.HARD,
        ]
        print(f"Running BDSDM timing for size {size}...")
        dataset = datasets[power]
        dbs: list[BaseDocker] = [
            Neo4JDB(
                id="timing-neo4j",
                base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
                dataset=dataset,
                use_encoded_ttl=True,
                name="Neo4j",
            ),
            QleverDBNative(
                id="timing-qlever",
                base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
                dataset=dataset,
                use_encoded_ttl=True,
                enable_tensor_index=True,
                name="QLever",
                port_offset=offset + 1,
            ),
            QleverDBNative(
                id="timing-qlever",
                base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
                dataset=dataset,
                use_encoded_ttl=True,
                enable_tensor_index=False,
                name="QLever (no Tensor Vocabulary)",
                port_offset=offset + 2,
            ),
            FusekiDBNative(
                id="timing-fuseki",
                base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
                dataset=dataset,
                use_encoded_ttl=True,
                port_offset=offset,
            ),
        ]
        reference_db = dbs[0]
        reference_results: dict[QUERY_DIFFICULTY, pd.DataFrame] = {}

        test_label = "house furniture storage container"
        test_tensor = DataTensor.from_numpy(encoding_model.encode(test_label))
        # for difficulty in difficulties:
        # try:
        #     with reference_db:
        #         reference_result = reference_db.query_auto(
        #             test_tensor,
        #             query_difficulty=difficulty,
        #             query_type=QUERY_TYPE.EMBEDDED,
        #         )
        #         reference_results[difficulty] = reference_result
        # except Exception as e:
        #     print(
        #         f"Error occurred while querying reference DB for difficulty {difficulty}: {e}"
        #     )
        bench = BenchmarkRunner(
            dbs=dbs,
            test_tensor=test_tensor,
            difficulties=difficulties,
            types=[
                QUERY_TYPE.CYPHER_EMBEDDED,
                QUERY_TYPE.CYPHER_INDEX,
                QUERY_TYPE.EMBEDDED,
                QUERY_TYPE.INDEX,
                QUERY_TYPE.TWO_STAGE,
            ],
            dataset=dataset,
            reference_results=reference_results,
        )
        result = bench.run_benchmark(repetitions=args.repetitions)
        timings_df = pd.concat([timings_df, result.timings], ignore_index=True)
        stats_df = pd.concat([stats_df, result.stats], ignore_index=True)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timings_df.to_csv(out_dir / "bsbm_timings_all.csv", index=False)
    stats_df.to_csv(out_dir / "bsbm_stats_all.csv", index=False)
