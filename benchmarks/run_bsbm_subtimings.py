import argparse
import logging
import os
import time

# %%
from pathlib import Path

import dotenv
import numpy as np
import pandas as pd
import tqdm
from sentence_transformers import SentenceTransformer
from utils.datasets import BerlinSparqlBenchmark
from utils.datasets.base_dataset import QUERY_DIFFICULTY, QUERY_TYPE, DataTensor
from utils.dbs.executable_db import ExecutableDB
from utils.dbs.fuseki_native import FusekiDBNative
from utils.dbs.qlever_native import QleverDBNative
from utils.format import map_df_readable


def map_group_stats(df: pd.DataFrame, value_col: str = "timings"):
    stat_mapping = {
        "totalExecution": "Total (ms / ct.)",
        "readTensorData": "Read Tensors (ms / ct.)",
        "readWordFromDisk": "Read RDF (ms / ct.)",
        "tensorCosineSimilarity": "Cosine Similarity (ms / ct.)",
        "tensorFromBuffer": "Parse from Buffer (ms / ct.)",
        "tensorFromString": "Parse from String (ms / ct.)",
        "tensorIndexComputeResult": "Index Lookup (ms / ct.)",
    }
    df_cp = df.copy()
    df_cp["timing_key"] = df_cp["timing_key"].map(stat_mapping)

    df_cp = df_cp[df_cp["timing_key"].notna()]

    # timing mappings to columns

    df_grouped = (
        df_cp.groupby(["engine", "query_type", "timing_key"])[
            [("timings", "sum"), ("timings", "count")]
        ]
        .median()
        .reset_index()
        .sort_values(by=["engine", "query_type", "timing_key"])
    )
    df_grouped[("timings", "sum")] = (
        df_grouped[("timings", "sum")] / 1e6
    )  # convert to ms
    return df_grouped


# %%
def multileve_timings_to_str(df: pd.DataFrame, col="timings"):
    df_cp = df.copy()

    def combine_multi_level(row):
        # print(row)
        summed = f"{row['sum']:.2f}" if not pd.isna(row["sum"]) else "-"
        counts = int(row["count"]) if not pd.isna(row["count"]) else "-"
        return f"{summed} ({counts})"

    df_cp["combined"] = df_cp[col].apply(combine_multi_level, axis=1)
    return df_cp


def run_queries(
    db: ExecutableDB,
    q_type: QUERY_TYPE,
    q_difficulty: QUERY_DIFFICULTY,
    test_tensor: DataTensor,
    repetitions=128,
):
    query_timings_ns = []
    available_qs = db.get_available_query_types()
    if q_type not in available_qs:
        print(f"Query type {q_type} not available for database {db.name}. Skipping.")
        return query_timings_ns
    g = tqdm.tqdm(range(repetitions), total=repetitions, unit="query", leave=False)
    g.set_description(f"Running queries on {db.name}/{q_type}: ")
    for _ in g:
        noised_tensor = test_tensor.to_numpy() + np.random.normal(
            scale=0.01, size=test_tensor.to_numpy().shape
        )
        noised_tensor = DataTensor.from_numpy(noised_tensor)
        test_queries = db.get_queries(embedding=noised_tensor)
        queries_for_difficulty = test_queries.get(q_difficulty, {})
        q = queries_for_difficulty.get(q_type, None)
        if q is None:
            print(
                f"No query found for difficulty {q_difficulty} and type {q_type} in database {db.name}. Skipping."
            )
            continue
        start_time = time.time()
        db.raw_query(q)
        elapsed_time = time.time() - start_time
        query_timings_ns.append(elapsed_time * 1e9)
    return query_timings_ns


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
argsc = argparse.ArgumentParser(description="Run BSBM timings")
argsc.add_argument(
    "--power",
    type=int,
    default=3,
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
    default="./scratch/results",
    help="Directory for benchmark results",
)

args = argsc.parse_args()

if __name__ == "__main__":
    encoding_model = SentenceTransformer("all-MiniLM-L6-v2")

    figures_dir = Path("scratch") / "figures"
    figures_dir.mkdir(exist_ok=True, parents=True)

    config = dotenv.dotenv_values(".env")
    replace_keys = ["JAVA_HOME", "FUSEKI_HOME"]
    append_keys = ["PATH"]
    for key, value in config.items():
        # append to os.environ
        if key in append_keys:
            os.environ[key] = f"{os.environ.get(key, '')}:{value}"
        elif key in replace_keys:
            os.environ[key] = value
        print(f"{key}={os.environ.get(key)}")

    # check and compare values in fuseki log
    os.environ["OPENBLAS_NUM_THREADS"] = "4"

    # %%
    powers = np.arange(1, 4)  # extend on a more powerful machine
    sizes = 10**powers
    test_label = "house furniture storage container"
    test_tensor = DataTensor.from_numpy(encoding_model.encode(test_label))

    # %%
    datasets: dict[int, BerlinSparqlBenchmark] = {}
    raw_sizes: dict[int, int] = {}

    repetitions = args.repetitions
    all_timings = None
    qtypes = [QUERY_TYPE.EMBEDDED, QUERY_TYPE.INDEX]
    qdifficulties = [QUERY_DIFFICULTY.EASY, QUERY_DIFFICULTY.HARD]
    combinations = [
        (q_type, q_difficulty) for q_type in qtypes for q_difficulty in qdifficulties
    ]
    total_runs = len(powers) * 3 * len(combinations)  # powers * dbs * query_types

    progress = tqdm.tqdm(total=total_runs, unit="run", desc="Total Progress")
    for power, size in zip(powers, sizes):
        print(f"Running BDSDM generation for size {size}...")
        dataset = BerlinSparqlBenchmark(base_dir=Path(f"./data/bsbm_{power}"), n=size)
        dataset.setup()
        dataset.encode(encoding_model)
        datasets[power] = dataset
        full_triple_count = dataset.get_triple_count(encoded=True)
        estimated_triple_count = dataset.get_estimated_size()

        # %%

        db_with_tensor_idx = QleverDBNative(
            id="test",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            name="QLever",
        )
        db_no_tensor_idx = QleverDBNative(
            id="test",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            name="QLever (no Tensor Vocabulary)",
            enable_tensor_index=False,
        )
        db_fuseki = FusekiDBNative(
            id="test_fuseki",
            base_dir=Path(f"./scratch/bsbm/{dataset.base_dir.name}"),
            dataset=dataset,
            use_encoded_ttl=True,
            name="Fuseki + RDFTensor",
        )
        possible_queries = db_with_tensor_idx.get_queries(test_tensor)

        dbs: list[ExecutableDB] = [db_with_tensor_idx, db_no_tensor_idx, db_fuseki]

        # %%
        for q_type, q_difficulty in combinations:
            for db in dbs:
                db.clear_log()
                with db as db_instance:
                    progress.set_description(
                        f"Running queries on {db_instance.name}/{q_type}: "
                    )
                    try:
                        query_timings_ns = run_queries(
                            db_instance,
                            q_type,
                            q_difficulty,
                            test_tensor,
                            repetitions=1,
                        )
                    except TimeoutError as te:
                        print(f"TimeoutError for {db_instance.name}/{q_type}: {te}")
                        continue
                    if len(query_timings_ns) == 0:
                        continue
                    subtimings = db_instance.get_timings_per_query()
                    subtiming_records = []
                    metadata = {
                        "dataset": dataset.base_dir.name,
                        "size": full_triple_count,
                        "estimated_size": estimated_triple_count,
                        "engine": db_instance.name,
                        "query_type": q_type.name.lower(),
                        "difficulty": q_difficulty.name.lower(),
                    }
                    for i, qt in enumerate(subtimings):
                        for key, ts in qt.items():
                            for t in ts:
                                subtiming_records.append(
                                    {
                                        "query_id": i - 1,
                                        "timing_key": key,
                                        "timings": t,
                                    }
                                    | metadata
                                )
                    df_subtimings = pd.DataFrame(subtiming_records)
                    df_timings = pd.DataFrame(
                        [
                            {
                                "query_id": i,
                                "timing_key": "totalExecution",
                                "timings": t,
                            }
                            | metadata
                            for i, t in enumerate(query_timings_ns)
                        ]
                    )
                    full_timings = pd.concat(
                        [df_subtimings, df_timings], ignore_index=True
                    )

                    # full_timings_sums = (
                    #     full_timings.groupby(
                    #         ["query_id", "timing_key", "engine", "query_type", "dataset", "size", "estimated_size"]
                    #     )[["timings"]]
                    #     .agg(["sum", "count"])
                    #     .reset_index()
                    #     .sort_values(by=["query_id", "timing_key"])
                    # )
                    all_timings = (
                        pd.concat([all_timings, full_timings], ignore_index=True)
                        if all_timings is not None
                        else full_timings
                    )
                    progress.update(1)

    all_timings.to_csv(Path(args.out_dir) / "sub_bsbm_timings.csv", index=False)
