from dataclasses import dataclass
import timeit
import traceback
from unittest import result
from numpy.random import f
import pandas as pd
import tqdm

from utils.dbs.base_db import QUERY_DIFFICULTY, QUERY_TYPE, BaseDB

from utils.datasets.base_dataset import BaseDataset, DataTensor
import logging
import numpy as np
from utils.helpers import recall_at_k, precision_at_k, ndcgscore_query

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
        full_triple_count: int | None = None,
    ):
        self.dbs = dbs
        self.difficulties = difficulties
        self.query_types = types
        self.dataset = dataset
        self.test_tensor = test_tensor
        self.reference_results = reference_results
        self.full_triple_count = full_triple_count

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
        end_time = None
        wall_time_end = None
        results_df = None
        result = {
            "elapsed_time": np.inf,
            "ndcg_score": -1,
            "recall_score": -1,
            "wall_time": None,
        }
        wall_time_start = db.stat_recorder.get_wall_time() if db.stat_recorder else None
        start_time = timeit.default_timer()
        try:
            if query_type != QUERY_TYPE.TWO_STAGE:
                results = db.raw_query(q)
            else:
                results_df = db.query_auto(
                    noised_tensor, query_difficulty=difficulty, query_type=query_type
                )
            # score the results if reference results are provided
            end_time = timeit.default_timer()
            wall_time_end = (
                db.stat_recorder.get_wall_time() if db.stat_recorder else None
            )
            results_df = (
                db.q_to_df_values(results) if results_df is None else results_df
            )
            if self.reference_results is not None:
                # compute ndcg
                reference_result = (
                    self.reference_results[difficulty]
                    if difficulty in self.reference_results
                    else pd.DataFrame()
                )
                result["ndcg_score"] = ndcgscore_query(results_df, reference_result)
                result["recall_score"] = recall_at_k(results_df, reference_result, k=10)
            assert results is not None and len(results.bindings) > 0, (
                "Query returned no results"
            )
        except Exception as e:
            logger.error(f"Error occurred while querying {db.name}: {e}")
            traceback.print_exc()
        elapsed_time = end_time - start_time if end_time is not None else np.inf
        run_wall_time = (
            (wall_time_end - wall_time_start)
            if (wall_time_start is not None and wall_time_end is not None)
            else None
        )
        result["elapsed_time"] = elapsed_time
        result["wall_time"] = run_wall_time
        return result

    def test_db(
        self,
        db: BaseDB,
        difficulty=QUERY_DIFFICULTY.EASY,
        query_type=QUERY_TYPE.EMBEDDED,
        repetitions=128,
    ) -> TestResult:
        with db:
            full_triple_count = (
                self.full_triple_count
                if self.full_triple_count is not None
                else db.get_triple_count()
            )
            estimated_size = self.dataset.get_estimated_size()
            timings = []
            if query_type not in db.get_available_query_types():
                print(f"Query type {query_type} not available for {db.name}, skipping.")
                return TestResult(timings=pd.DataFrame(), stats=pd.DataFrame())
            db.start_record_stats()
            allowed_timeouts = 2
            for i in tqdm.tqdm(
                range(repetitions),
                desc=f"Timing for size {estimated_size}, difficulty {difficulty.value}, query type {query_type.value}, db {db.name}",
            ):
                # add a small amount of noise to the test tensor to avoid caching effects
                result = self.run_repetition(db, difficulty, query_type)
                if result["elapsed_time"] > 30:
                    logger.info(
                        f"Query took {result['elapsed_time']:.2f} seconds on {db.name} for difficulty {difficulty.value}, query type {query_type.value}. Stopping benchmark for this configuration."
                    )
                    allowed_timeouts -= 1
                    if allowed_timeouts <= 0:
                        logger.info(
                            f"Too many timeouts for {db.name} on difficulty {difficulty.value}, query type {query_type.value}. Skipping remaining repetitions."
                        )
                        break
                timings.append(
                    {
                        "size": full_triple_count,
                        "power": int(np.log10(full_triple_count))
                        if full_triple_count > 0
                        else 0,
                        "estimated_size": estimated_size,
                        "rep": i,
                        "engine": db.name,
                        "difficulty": difficulty.value,
                        "query_type": query_type.value,
                    }
                    | result
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
                    logger.info(
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
        logger.info(
            "Benchmark completed got: %d timings and %d stats",
            len(all_timings),
            len(all_stats),
        )
        return TestResult(
            timings=pd.concat(all_timings, ignore_index=True),
            stats=pd.concat(all_stats, ignore_index=True),
        )
