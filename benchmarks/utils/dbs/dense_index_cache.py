from re import search

import faiss
import logging
import pandas as pd
import numpy as np
from pathlib import Path

from ..datasets.data_tensor import DataTensor

logger = logging.getLogger(__name__)


class DenseIndexCacheEntry:
    def __init__(self, quantizer: faiss.IndexFlatIP, index: faiss.IndexIVFFlat):
        self.quantizer = quantizer
        self.index = index


class DenseIndexCache:
    def __init__(self):
        self.indices: dict[str, DenseIndexCacheEntry] = {}

    def get_or_build_index(
        self, index_name: str, vectors: pd.Series
    ) -> faiss.IndexIVFFlat:
        if index_name in self.indices:
            return self.indices[index_name].index
        logger.info(
            f"Start building dense index for {index_name} with {len(vectors)} vectors..."
        )
        datatensors = vectors.apply(lambda x: DataTensor.from_literal(x)).tolist()
        array = np.array([dt.to_numpy() for dt in datatensors], dtype=np.float32)
        dim = array.shape[1]
        logger.info(f"Vector dimension is {dim}")
        quantizer = faiss.IndexFlatIP(dim)
        trees = min(
            int(np.sqrt(array.shape[0]) + 1), array.shape[0] - 1
        )  # std::min(std::sqrt(restable.size()) + 1, restable.size() - 1)
        index = faiss.IndexIVFFlat(quantizer, dim, trees)
        logger.info(
            f"Building dense index for {index_name} with {len(vectors)} vectors, number of trees: {trees}..."
        )
        index.train(array)
        index.add(array)
        logger.info(f"Finished building dense index for {index_name}")
        self.indices[index_name] = DenseIndexCacheEntry(quantizer, index)
        return index

    def find_knn(
        self, index_name: str, query_vector: DataTensor, k: int, vectors: pd.Series
    ) -> tuple[list[float], list[float]]:
        index = self.get_or_build_index(index_name, vectors)
        query_array = query_vector.to_numpy().reshape(1, -1).astype(np.float32)

        #   size_t search_probe = config_.searchK_.value_or(
        #       std::min((size_t)std::sqrt(tensorIndexToRow_.size()),
        #                tensorIndexToRow_.size() - 1));
        search_probe = min(int(np.sqrt(index.ntotal) + 1), index.ntotal - 1)
        index.nprobe = search_probe
        distances, indices = index.search(query_array, k)  # returns two lists
        return distances[0], indices[0]
