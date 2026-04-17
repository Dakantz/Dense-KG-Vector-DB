import pandas as pd
import numpy as np
from sklearn.metrics import ndcg_score


def score_query(
    result: pd.DataFrame | None, reference: pd.DataFrame, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_scores = np.ones(len(reference))
    result_scores = np.zeros(len(result))
    # set to one if in the reference results
    for i, row in result.iterrows():
        if result.iloc[i, col] in reference.iloc[:, col].values:
            result_scores[i] = 1
    result_scores = result_scores[: len(reference_scores)]
    score = ndcg_score([reference_scores], [result_scores])
    return score
