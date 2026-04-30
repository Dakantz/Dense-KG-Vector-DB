from re import search

try:
    import faiss

    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    print(
        "Warning: Faiss library is not available. Dense index caching will be disabled."
    )

    class faiss:
        class IndexFlatIP:
            def __init__(self, dim):
                raise NotImplementedError("Faiss library is not available.")

        class IndexIVFFlat:
            def __init__(self, quantizer, dim, trees):
                raise NotImplementedError("Faiss library is not available.")

        class IndexHNSWFlat:
            def __init__(self, dim, M):
                raise NotImplementedError("Faiss library is not available.")


import logging
import pandas as pd
import numpy as np
from pathlib import Path

from ..datasets.data_tensor import DataTensor

logger = logging.getLogger(__name__)


class DenseIndexCacheEntry:
    def __init__(self, index: faiss.IndexHNSWFlat):

        self.index = index


class DenseIndexCache:
    def __init__(self):
        self.indices: dict[str, DenseIndexCacheEntry] = {}

    def clear_cache(self):
        self.indices.clear()

    def get_or_build_index(
        self, index_name: str, vectors: pd.Series
    ) -> faiss.IndexHNSWFlat:
        if index_name in self.indices:
            return self.indices[index_name].index
        logger.info(
            f"Start building dense index for {index_name} with {len(vectors)} vectors..."
        )
        datatensors = vectors.apply(lambda x: DataTensor.from_literal(x)).tolist()
        array = np.array([dt.to_numpy() for dt in datatensors], dtype=np.float32)
        dim = array.shape[1]
        logger.info(f"Vector dimension is {dim}")

        trees = min(
            int(np.sqrt(array.shape[0]) + 1), array.shape[0] - 1
        )  # std::min(std::sqrt(restable.size()) + 1, restable.size() - 1)
        index = faiss.IndexHNSWFlat(dim, 32, faiss.METRIC_INNER_PRODUCT)  # M = 32
        logger.info(
            f"Building dense index for {index_name} with {len(vectors)} vectors, number of trees: {trees}..."
        )
        index.train(array)
        index.add(array)
        logger.info(f"Finished building dense index for {index_name}")
        self.indices[index_name] = DenseIndexCacheEntry(index)
        return index

    def find_knn(
        self,
        index_name: str,
        query_vector: DataTensor,
        k: int,
        vectors: pd.Series,
        n_probe: int = 1,
    ) -> tuple[list[float], list[float]]:
        index = self.get_or_build_index(index_name, vectors)
        query_array = query_vector.to_numpy().reshape(1, -1).astype(np.float32)

        #   size_t search_probe = config_.searchK_.value_or(
        #       std::min((size_t)std::sqrt(tensorIndexToRow_.size()),
        #                tensorIndexToRow_.size() - 1));
        search_probe = (
            min(int(np.sqrt(index.ntotal) + 1), index.ntotal - 1)
            if n_probe is None
            else n_probe
        )
        index.hnsw.efSearch = search_probe
        distances, indices = index.search(query_array, k)  # returns two lists
        return distances[0], indices[0]
