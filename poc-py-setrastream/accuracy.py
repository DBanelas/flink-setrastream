import pandas as pd
from typing import List

def boundaries(df: pd.DataFrame, start: bool = True) -> List[float]:
    """
    Return *internal* episode-end stamps as a sorted list.
    Expects columns start_time, end_time (seconds).
    """
    if start:
        return df["start_time"].iloc[:-1].tolist() if len(df) > 1 else []
    else:
        return df["end_time"].iloc[1:].tolist() if len(df) > 1 else []

def change_point_metrics(detected_csv: str,
                         truth_csv: str,
                         tol: float = 0.5):
    """
    Compare change-points in detected_csv against truth_csv.
    tol: ± seconds tolerance window.
    Prints P, R, F1 and returns a dict.
    """
    det_start_times = boundaries(pd.read_csv(detected_csv), start=True)
    det_end_times = boundaries(pd.read_csv(detected_csv), start=False)
    gt_start_times = boundaries(pd.read_csv(truth_csv), start=True)
    gt_end_times = boundaries(pd.read_csv(truth_csv), start=False)

    det_used = [False]*len(det_start_times)   # each detection may match once
    TP = 0
    for gt_ep_index in len(gt_start_times):
        # nearest unused detection within ± tol
        best = None
        best_dist = tol + 1
        for i, d in enumerate(det_times):
            if det_used[i]:
                continue
            dist = abs(d - g)
            if dist <= tol and dist < best_dist:
                best, best_dist = i, dist
        if best is not None:
            TP += 1
            det_used[best] = True

    FP = det_used.count(False)
    FN = len(gt_times) - TP

    precision = TP / (TP+FP) if TP+FP else 0.0
    recall    = TP / (TP+FN) if TP+FN else 0.0
    f1        = 2*precision*recall / (precision+recall) if precision+recall else 0.0

    print(f"Δ = ±{tol:.2f}s   TP={TP}  FP={FP}  FN={FN}")
    print(f"Precision {precision:.3f}   Recall {recall:.3f}   F1 {f1:.3f}")

    return dict(TP=TP, FP=FP, FN=FN,
                precision=precision, recall=recall, f1=f1)

# ---------------------------------------------------------------------------
# EXAMPLE
# ---------------------------------------------------------------------------
ROBOT_ID = 0
change_point_metrics(f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/episodes_{ROBOT_ID}.csv",
                     f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/episodes_groundtruth_{ROBOT_ID}.csv",
                     tol=0.5)
