import pandas as pd
import numpy as np
from sklearn.metrics import ndcg_score


import pandas as pd


def recall_at_k(
    result: pd.DataFrame | None, reference: pd.DataFrame, k: int, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_set = set(reference.iloc[:, col].values)
    result_set = set(result.iloc[:k, col].values) if result is not None else set()
    intersection_size = len(reference_set.intersection(result_set))
    recall = intersection_size / len(reference_set)
    return recall


def precision_at_k(
    result: pd.DataFrame | None, reference: pd.DataFrame, k: int, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_set = set(reference.iloc[:, col].values)
    result_set = set(result.iloc[:k, col].values) if result is not None else set()
    intersection_size = len(reference_set.intersection(result_set))
    precision = intersection_size / min(k, len(result_set)) if result_set else 0.0
    return precision


def ndcgscore_query(
    result: pd.DataFrame | None, reference: pd.DataFrame, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_scores = np.arange(len(reference))
    result_scores = np.zeros(len(reference))
    # set to one if in the reference results
    for i, row in result.iterrows():
        if i > reference.shape[0]:
            break
        if i > result.shape[0]:
            break
        if result.iloc[i, col] in reference.iloc[:, col].values:
            result_scores[i] = 1
    score = ndcg_score([reference_scores], [result_scores])
    return score


def to_pow_10(x):
    base_10 = int(np.log10(x))
    extra = x / (10**base_10)
    if extra == 1:
        return f"$10^{{{base_10}}}$"

    return f"${extra:.2f} \\cdot 10^{{{base_10}}}$"


def pretty_print_counts(
    counts_df: pd.DataFrame,
    out_path: str,
    column_mapping: dict = None,
    format_cols: list[str] = None,
    to_int_cols: list[str] = None,
    index_cols: list[str] = None,
    label_xtra: str = "",
    caption: str = "",
) -> pd.DataFrame:
    if column_mapping is None:
        column_mapping = {
            "power": "Power",
            "size": "Generation $t$",
            "full_size": "$n$",
            "full_number_of_tensors": "$n_{tensors}$",
        }
    if format_cols is None:
        format_cols = list(column_mapping.keys())
        to_int_cols = format_cols
    non_format_cols = [col for col in column_mapping.keys() if col not in format_cols]
    print(format_cols, non_format_cols)
    selection = list(column_mapping.keys())
    counts_df[to_int_cols] = counts_df[to_int_cols].astype(int)

    counts_df = counts_df[non_format_cols + format_cols].rename(columns=column_mapping)
    if index_cols is not None:
        counts_df.set_index([column_mapping[col] for col in index_cols], inplace=True)

    with open(out_path, "w") as f:
        counts_df.style.format(
            {column_mapping[col]: lambda x: x for col in non_format_cols}
            | {column_mapping[col]: to_pow_10 for col in format_cols}
        ).to_latex(
            buf=f,
            caption=caption,
            label=f"tab:{label_xtra}",
            clines="all;data",
            hrules=True,
        )
    return counts_df
