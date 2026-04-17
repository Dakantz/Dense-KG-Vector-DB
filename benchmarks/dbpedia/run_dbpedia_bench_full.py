import os
import sys

sys.path.append("..")
sys.path.append(".")

from utils.datasets import DBPedia
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


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
argsc = argparse.ArgumentParser(description="Run DBPedia timings")
argsc.add_argument(
    "--max-power",
    type=int,
    default=5,
    help="Power of 10 for dataset size (e.g., 2 for 100 triples)",
)
argsc.add_argument(
    "--base-dir",
    type=str,
    default="./data/dbpedia",
    help="Base directory for DBPedia datasets",
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
    default="./scratch/results/dbpedia",
    help="Directory for benchmark results",
)
args = argsc.parse_args()

os.environ["OPENBLAS_NUM_THREADS"] = "4"
if __name__ == "__main__":
    # %%
    args = argsc.parse_args()
    dataset = DBPedia(base_dir=Path(args.base_dir))
    encoding_model = SentenceTransformer("clip-ViT-B-32")

    timings_df = pd.DataFrame()
    stats_df = pd.DataFrame()
    offset = 0
    difficulties = [
        QUERY_DIFFICULTY.HARD,
        QUERY_DIFFICULTY.EASY,
    ]

    dbs: list[BaseDocker] = [
        QleverDBNative(
            id="timing-qlever",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            enable_tensor_index=True,
            name="Qlever",
            port_offset=-100,
        ),
        QleverDBNative(
            id="timing-qlever",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            enable_tensor_index=False,
            name="Qlever (no Tensor Vocabulary)",
            port_offset=-200,
        ),
        FusekiDBNative(
            id="timing-fuseki",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            name="Fuseki",
            port_offset=-300,
        ),
    ]
    reference_db = dbs[0]
    test_label = "horse on a bow"
    test_tensor = DataTensor.from_numpy(encoding_model.encode(test_label))
    possible_queries = reference_db.get_queries(test_tensor)
    print(possible_queries)

    indices = ["index-encoded", "index-encoded-no-tidx", "fuseki-encoded"]
    ids = ["dbpedia-encoded-tidx", "dbpedia-encoded", None]
    for db, index, id in zip(dbs, indices, ids):
        db.db_dir = Path(args.base_dir) / index
        db.id = id if id is not None else db.id
    reference_results: dict[QUERY_DIFFICULTY, pd.DataFrame] = {}

    full_triple_count = -1
    with reference_db:
        full_triple_count = reference_db.get_triple_count()
        logger.info(f"Full triple count for DBPedia: {full_triple_count}")
        for difficulty in difficulties:
            try:
                reference_result = reference_db.query_auto(
                    test_tensor,
                    query_difficulty=difficulty,
                    query_type=QUERY_TYPE.EMBEDDED,
                )
                reference_results[difficulty] = reference_result
            except Exception as e:
                print(
                    f"Error occurred while querying reference DB for difficulty {difficulty}: {e}"
                )
    bench = BenchmarkRunner(
        dbs=dbs,
        test_tensor=test_tensor,
        difficulties=difficulties,
        types=[
            QUERY_TYPE.EMBEDDED,
            QUERY_TYPE.INDEX,
        ],
        dataset=dataset,
        reference_results=reference_results,
        full_triple_count=full_triple_count,
    )
    result = bench.run_benchmark(repetitions=args.repetitions)
    timings_df = pd.concat([timings_df, result.timings], ignore_index=True)
    stats_df = pd.concat([stats_df, result.stats], ignore_index=True)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    timings_df.to_csv(out_dir / "dbpedia_timings_all.csv", index=False)
    stats_df.to_csv(out_dir / "dbpedia_stats_all.csv", index=False)
