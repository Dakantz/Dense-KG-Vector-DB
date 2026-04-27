from utils.datasets import BerlinSparqlBenchmark
from utils.dbs.fuseki_native import FusekiDBNative
from utils.dbs.qlever_native import QleverDBNative
from utils.dbs.base_db import BaseDB
from utils.datasets.base_dataset import QUERY_DIFFICULTY, QUERY_TYPE
from pathlib import Path
import numpy as np
from utils.datasets.base_dataset import DataTensor
from sentence_transformers import SentenceTransformer
import argparse
import json
import logging
import pandas as pd
from utils.bench import BenchmarkRunner

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
argsc = argparse.ArgumentParser(description="Run BSBM timings")
argsc.add_argument(
    "--power",
    type=int,
    default=2,
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
    "--db",
    type=str,
    default="qlever",
    choices=["qlever", "qlever-tidx", "fuseki"],
    help="Database to use for benchmarks",
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
    default="./scratch/results",
    help="Directory for benchmark results",
)
argsc.add_argument(
    "--id",
    type=int,
    default=0,
    help="ID for the benchmark run",
)

args = argsc.parse_args()

if __name__ == "__main__":
    # %%
    args = argsc.parse_args()

    dataset = BerlinSparqlBenchmark = BerlinSparqlBenchmark(
        base_dir=Path(args.base_dir) / f"bsbm_{args.power}",
        n=10**args.power,
    )
    dataset.setup()

    encoding_model = SentenceTransformer("all-MiniLM-L6-v2")
    dataset.encode(encoding_model)
    db: BaseDB = None
    base_db_dir = Path(args.scratch_dir) / "bsbm" / dataset.base_dir.name
    reference_db = FusekiDBNative(
        id="timing-fuseki",
        base_dir=base_db_dir,
        dataset=dataset,
        use_encoded_ttl=True,
        port_offset=args.id,
    )
    match args.db:
        case "fuseki":
            db = reference_db

        case "qlever-tidx":
            db = QleverDBNative(
                id="timing-qlever",
                base_dir=base_db_dir,
                dataset=dataset,
                use_encoded_ttl=True,
                enable_tensor_index=True,
                name="QLever",
                port_offset=args.id,
            )

        case "qlever":
            db = QleverDBNative(
                id="timing-qlever",
                base_dir=base_db_dir,
                dataset=dataset,
                use_encoded_ttl=True,
                enable_tensor_index=False,
                name="QLever (no Tensor Vocabulary)",
                port_offset=args.id,
            )
    logger.info(f"Using database: {db.id}")
    logger.info(f"Setting up database {db.id} for dataset {dataset.name}...")
    difficulties = [
        QUERY_DIFFICULTY.HARD,
        QUERY_DIFFICULTY.EASY,
    ]

    test_label = "house furniture storage container"
    test_tensor = DataTensor.from_numpy(encoding_model.encode(test_label))

    reference_results: dict[QUERY_DIFFICULTY, pd.DataFrame] = {}
    for difficulty in difficulties:
        try:
            with reference_db:
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
        dbs=[db],
        test_tensor=test_tensor,
        difficulties=difficulties,
        types=[
            QUERY_TYPE.EMBEDDED,
            QUERY_TYPE.INDEX,
            QUERY_TYPE.TWO_STAGE,
        ],
        dataset=dataset,
        reference_results=reference_results,
    )
    result = bench.run_benchmark(repetitions=16)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result.timings.to_csv(
        out_dir / f"bsbm_timings_{db.id}_power_{args.power}.csv", index=False
    )
    result.stats.to_csv(
        out_dir / f"bsbm_stats_{db.id}_power_{args.power}.csv", index=False
    )
