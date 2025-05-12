import pandas as pd
from pathlib import Path
from typing import List, Tuple



def load_df(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path, usecols=["start_time", "end_time"])

def iou_segment(a_start: float, a_end: float,
                b_start: float, b_end: float) -> float:
    inter = max(0.0, min(a_end, b_end) - max(a_start, b_start))
    union = max(a_end, b_end) - min(a_start, b_start)
    return 0.0 if union == 0 else inter / union

def per_row_iou(df_a: pd.DataFrame, df_b: pd.DataFrame) -> List[float]:
    """Return IoU for each corresponding row, stopping at the shorter DataFrame."""
    n = min(len(df_a), len(df_b))          # NEW: safe length
    return [
        iou_segment(sa, ea, sb, eb)
        for (sa, ea), (sb, eb) in zip(df_a.values[:n], df_b.values[:n])
    ]

def aggregate_ious(ious: List[float], unions: List[float]) -> dict:
    macro = sum(ious) / len(ious)
    micro = sum(i * u for i, u in zip(ious, unions)) / sum(unions)
    return {"macro_avg": macro, "micro_avg": micro}

def evaluate(file_a: str | Path, file_b: str | Path, verbose: bool = True) -> dict:
    A = load_df(file_a)
    B = load_df(file_b)

    n_pairs = min(len(A), len(B))
    if verbose and len(A) != len(B):
        print(f"⚠️  Row mismatch: using the first {n_pairs} pairs ("
              f"{len(A)} rows in A, {len(B)} rows in B).")

    ious   = per_row_iou(A, B)
    unions = [max(ea, eb) - min(sa, sb)
              for (sa, ea), (sb, eb) in zip(A.values[:n_pairs], B.values[:n_pairs])]

    agg = aggregate_ious(ious, unions)

    if verbose:
        print("Per-row IoUs:", ["{:.3f}".format(x) for x in ious])
        print("Macro average:", round(agg["macro_avg"], 3))
        print("Micro average:", round(agg["micro_avg"], 3))

    return {"per_row": ious, **agg}

# example:
ROBOT_ID = 0
evaluate(f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/episodes_groundtruth_{ROBOT_ID}.csv",
         f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/episodes_{ROBOT_ID}.csv")
