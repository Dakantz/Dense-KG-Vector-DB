from dataclasses import dataclass
import timeit
from numpy.random import f
import pandas as pd
import tqdm

from utils.dbs.base_db import QUERY_DIFFICULTY, QUERY_TYPE, BaseDB
from sklearn.metrics import ndcg_score
from utils.datasets.base_dataset import BaseDataset, DataTensor
import logging
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class TestResult:
    timings: pd.DataFrame
    stats: pd.DataFrame


class BenchmarkRunner:
    def __init__(
        self,
        dbs: list[BaseDB],
        difficulties: list[QUERY_DIFFICULTY],
        types: list[QUERY_TYPE],
        dataset: BaseDataset,
        test_tensor: DataTensor,
        reference_results: dict[QUERY_DIFFICULTY, pd.DataFrame] | None = None,
    ):
        self.dbs = dbs
        self.difficulties = difficulties
        self.query_types = types
        self.dataset = dataset
        self.test_tensor = test_tensor
        self.reference_results = reference_results

    def run_repetition(
        self,
        db: BaseDB,
        difficulty: QUERY_DIFFICULTY,
        query_type: QUERY_TYPE,
    ):
        noised_tensor = DataTensor.from_numpy(
            self.test_tensor.data
            + np.random.normal(scale=0.001, size=self.test_tensor.shape)
        )
        test_queries = db.get_queries(embedding=noised_tensor)
        q = test_queries[difficulty][query_type]
        start_time = timeit.default_timer()
        end_time = None
        score = -1
        try:
            results = db.raw_query(q)
            # score the results if reference results are provided
            end_time = timeit.default_timer()
            results_df = db.q_to_df_values(results)
            if self.reference_results is not None:
                # compute ndcg
                reference_result = (
                    self.reference_results[difficulty]
                    if difficulty in self.reference_results
                    else pd.DataFrame()
                )
                if reference_result.empty:
                    return end_time - start_time, -1
                reference_scores = np.ones(len(reference_result))
                result_scores = np.zeros(len(results_df))
                # set to one if in the reference results
                for i, row in results_df.iterrows():
                    if results_df.iloc[i, 0] in reference_result.iloc[:, 0].values:
                        result_scores[i] = 1
                score = ndcg_score([reference_scores], [result_scores])
            assert results is not None and len(results.bindings) > 0, (
                "Query returned no results"
            )
        except Exception as e:
            print(f"Error occurred while querying {db.name}: {e}")
            raise e
        elapsed_time = end_time - start_time if end_time is not None else np.inf
        if elapsed_time > 30:
            print(
                f"Warning: Query took {elapsed_time:.2f} seconds on {db.name} for difficulty {difficulty.value}, query type {query_type.value}. Will cancel."
            )
        return elapsed_time, score

    def test_db(
        self,
        db: BaseDB,
        difficulty=QUERY_DIFFICULTY.EASY,
        query_type=QUERY_TYPE.EMBEDDED,
        repetitions=128,
    ) -> TestResult:
        with db:
            full_triple_count = db.get_triple_count()
            estimated_size = self.dataset.get_estimated_size()
            timings = []
            if query_type not in db.get_available_query_types():
                print(f"Query type {query_type} not available for {db.name}, skipping.")
                return TestResult(timings=pd.DataFrame(), stats=pd.DataFrame())
            db.start_record_stats()
            for i in tqdm.tqdm(
                range(repetitions),
                desc=f"Timing for size {estimated_size}, difficulty {difficulty.value}, query type {query_type.value}, db {db.name}",
            ):
                # add a small amount of noise to the test tensor to avoid caching effects
                elapsed_time, score = self.run_repetition(db, difficulty, query_type)
                timings.append(
                    {
                        "size": full_triple_count,
                        "power": int(np.log10(full_triple_count)),
                        "estimated_size": estimated_size,
                        "elapsed_time": elapsed_time,
                        "rep": i,
                        "engine": db.name,
                        "difficulty": difficulty.value,
                        "query_type": query_type.value,
                        "ndcg_score": score,
                    }
                )

            db.stop_record_stats()
            stats = db.stat_recorder.get_stats()
            stats["size"] = full_triple_count
            stats["power"] = int(np.log10(full_triple_count))
            stats["difficulty"] = difficulty.value
            stats["query_type"] = query_type.value
            stats["estimated_size"] = estimated_size
            stats["engine"] = db.name

            db.stat_recorder.clear_stats()

            return TestResult(
                timings=pd.DataFrame(timings),
                stats=stats,
            )

    def run_benchmark(self, repetitions=128) -> TestResult:
        all_timings = []
        all_stats = []
        for db in self.dbs:
            for difficulty in self.difficulties:
                for query_type in self.query_types:
                    print(
                        f"Running benchmark for {db.name}, difficulty {difficulty.value}, query type {query_type.value}"
                    )
                    result = self.test_db(
                        db,
                        difficulty=difficulty,
                        query_type=query_type,
                        repetitions=repetitions,
                    )
                    all_timings.append(result.timings)
                    all_stats.append(result.stats)
        return TestResult(
            timings=pd.concat(all_timings, ignore_index=True),
            stats=pd.concat(all_stats, ignore_index=True),
        )
