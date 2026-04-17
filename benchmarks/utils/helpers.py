import pandas as pd
import numpy as np
from sklearn.metrics import ndcg_score


import pandas as pd


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


def pretty_print_counts(counts_df: pd.DataFrame, out_path: str) -> pd.DataFrame:
    column_mapping = {
        "power": "Power",
        "size": "Generation $t$",
        "full_size": "$n$",
        "full_number_of_tensors": "$n_{tensors}$",
    }
    selection = list(column_mapping.keys())
    counts_df = counts_df[selection].astype(int)
    with open(out_path, "w") as f:

        def to_pow_10(x):
            base_10 = int(np.log10(x))
            extra = x / (10**base_10)
            if extra == 1:
                return f"$10^{{{base_10}}}$"

            return f"${extra:.1f} \\cdot 10^{{{base_10}}}$"

        counts_df = counts_df.rename(columns=column_mapping)

        counts_df.style.format(
            {
                column_mapping["power"]: lambda x: x,
                column_mapping["size"]: to_pow_10,
                column_mapping["full_size"]: to_pow_10,
                column_mapping["full_number_of_tensors"]: to_pow_10,
            }
        ).to_latex(buf=f)
    return counts_df
