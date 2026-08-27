from numpy.random import f
import pandas as pd
import numpy as np
from pandas import isna
from sklearn.metrics import ndcg_score


import pandas as pd
from pathlib import Path


def recall_at_k(
    result: pd.DataFrame | None, reference: pd.DataFrame, k: int, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_set = set(reference.iloc[:, col].values)
    result_set = set(result.iloc[:k, col].values) if result is not None else set()
    tp = len(
        reference_set.intersection(result_set)
    )  # docs that are in both -relevant docs
    fn = len(
        reference_set.difference(result_set)
    )  # relevant docs that are not present in doc set - missing docs
    return tp / len(reference_set) if (tp + fn) > 0 else 0.0


def precision_at_k(
    result: pd.DataFrame | None, reference: pd.DataFrame, k: int, col: str | int = 0
) -> float:
    if reference.empty:
        return -1
    reference_set = set(reference.iloc[:, col].values)
    result_set = set(result.iloc[:k, col].values) if result is not None else set()
    tp = len(
        reference_set.intersection(result_set)
    )  # docs that are in both -relevant docs
    fp = len(
        result_set.difference(reference_set)
    )  # docs that are not in relevant set - irrelevant docs (false positiv)
    return tp / (tp + fp) if (tp + fp) > 0 else 0.0


def ndcgscore_query(
    result: pd.DataFrame | None,
    reference: pd.DataFrame,
    col: str | int = 0,
    k: int = 10,
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
    reference_scores = reference_scores[:k]
    result_scores = result_scores[:k]
    if len(result_scores) == 1 or len(reference_scores) == 1:
        return 0.0
    score = ndcg_score([reference_scores], [result_scores])
    return score


def to_pow_10(x, bold=False):
    if pd.isna(x) or x <= 0:
        return "-"
    base_10 = int(np.log10(x))
    extra = x / (10**base_10)
    repr = f"{extra:.2f} \\cdot 10^{{{base_10}}}"
    if base_10 == 0:
        repr = f"{extra:.2f}"
    if extra == 1:
        repr = f"10^{{{base_10}}}"
    if np.allclose(x, 1.0):
        repr = "1"
    if bold:
        return f"$\\bm{{{repr}}}$"
    return f"${repr}$"


def pretty_print_counts(
    counts_df_: pd.DataFrame,
    out_path: str,
    column_mapping: dict = None,
    format_cols: list[str] = None,
    to_int_cols: list[str] = None,
    index_cols: list[str] = None,
    label_xtra: str = "",
    caption: str = "",
    significant_cols: list[str] = [],
    significant_affected_cols: list[str] | None = None,
    twocol: bool = False,
) -> pd.DataFrame:
    if column_mapping is None:
        column_mapping = {
            "power": "Power",
            "size": "Generation $t$",
            "full_size": "$n$",
            "full_number_of_tensors": "$n_{tensors}$",
        }
    counts_df = counts_df_.copy()
    significant_col_vals = [counts_df[col].to_list() for col in significant_cols]

    def filter_cols(cols: list[str], df: pd.DataFrame):
        return [c for c in cols if c in df.columns]

    if significant_affected_cols is None:
        significant_affected_cols = []
    if significant_cols is not None and significant_affected_cols is not None:
        significant_affected_cols = filter_cols(significant_affected_cols, counts_df)
    if format_cols is None:
        format_cols = list(column_mapping.keys())
        to_int_cols = format_cols

    format_cols = filter_cols(format_cols, counts_df)
    to_int_cols = filter_cols(to_int_cols, counts_df)
    non_format_cols = filter_cols(
        [col for col in column_mapping.keys() if col not in format_cols], counts_df
    )
    print("Format cols:", format_cols, "Non-format cols:", non_format_cols)
    selection = list(column_mapping.keys())
    counts_df[to_int_cols] = counts_df[to_int_cols].astype(int)
    counts_df = counts_df[non_format_cols + format_cols].rename(columns=column_mapping)
    if index_cols is not None:
        idx_cols = [column_mapping[col] for col in index_cols]
        idx_cols = filter_cols(idx_cols, counts_df)
        print(f"Indexing by {idx_cols}")
        counts_df.set_index(
            idx_cols,
            inplace=True,
        )
    with open(out_path, "w") as out_f:
        stylers_significant = []
        for idx, col in enumerate(significant_affected_cols):
            assert idx < len(significant_col_vals), (
                f"Not enough significant columns provided for the affected column {col}"
            )
            assert len(significant_col_vals[idx]) == counts_df.shape[0], (
                f"Significant column {significant_cols[idx]} has different number of values than the DataFrame rows for the affected column {col}"
            )

            def styler_significant(x: float, idx_local=idx):
                print(
                    f"Col: {col=}, Idx: {idx_local=} {significant_col_vals[idx_local]=}"
                )
                is_significant = (
                    significant_col_vals[idx_local].pop(0)
                    if significant_col_vals is not None
                    else False
                )
                print(f"Value: {x}, Significant: {is_significant}")
                return to_pow_10(x, bold=is_significant)

            stylers_significant.append(styler_significant)
        formatters = (
            {column_mapping[col]: lambda x: x for col in non_format_cols}
            | {column_mapping[col]: to_pow_10 for col in format_cols}
            | {
                column_mapping[col]: stylers_significant[i]
                for i, col in enumerate(significant_affected_cols)
            }
        )

        counts_df.style.format(formatter=formatters).to_latex(
            buf=out_f,
            caption=caption,
            label=f"tab:{label_xtra}",
            clines="all;data",
            hrules=True,
        )
    return counts_df


def collapse_first_rows(tex_file: Path):
    with open(tex_file, "r") as f:
        lines = f.readlines()

    # Find the line with the first \toprule after the header
    hline_index = next(i for i, line in enumerate(lines) if "\\toprule" in line)
    header_lines = lines[hline_index + 1 : hline_index + 3]
    header_content = [li.split("&") for li in header_lines]
    combined_header = []
    for col in range(len(header_content[0])):
        col_contents = [
            header_content[row][col].strip().replace("\\\\", "")
            for row in range(len(header_content))
        ]
        combined_header.append(" ".join(col_contents))
    lines = (
        lines[: hline_index + 1]
        + [" & ".join(combined_header) + " \\\\\n"]
        + lines[hline_index + 3 :]
    )
    # Write the modified lines back to the file
    with open(tex_file, "w") as f:
        f.writelines(lines)


def to_multicol(tex_file: Path):
    with open(tex_file, "r") as in_f:
        latex_str = in_f.read()
        latex_str = latex_str.replace("\\begin{table}", "\\begin{table*}").replace(
            "\\end{table}", "\\end{table*}"
        )
    with open(tex_file, "w") as out_f:
        out_f.write(latex_str)


def add_args(tex_file: Path, before_caption: str = "", after_caption: str = ""):
    with open(tex_file, "r") as in_f:
        latex_lines = in_f.readlines()
    caption_index = next(i for i, line in enumerate(latex_lines) if "\\caption" in line)
    latex_lines = (
        latex_lines[:caption_index]
        + [before_caption + "\n"]
        + latex_lines[caption_index : caption_index + 1]
        + [after_caption + "\n"]
        + latex_lines[caption_index + 1 :]
    )
    with open(tex_file, "w") as out_f:
        out_f.writelines(latex_lines)
