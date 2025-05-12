import pandas as pd
import numpy as np
import math
import os
from collections import deque

ROBOT_ID = 0  # ID of the robot to analyze (0, 1, 2, ...)

# ------------------------------ CONFIGURATION ------------------------------
params = {
    "window_size_T": 60.0,        # seconds kept in the sliding window (unused offline)
    "batch_interval_tau": 0.5,    # τ in seconds – size of each batch
    "division_threshold_sigma": 0.7,  # RV threshold σ
    "feature_columns": ["dpx", "dpy"],  # features forming the movement-vector
    "input_csv": f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/robots_id{ROBOT_ID}.csv",    # path to your input file
    "output_csv": f"/Users/dbanelas/Developer/flink-setrastream/poc-py-setrastream/episodes_{ROBOT_ID}.csv"       # path to generated episodes file
}
# ---------------------------------------------------------------------------

# ------------------------------ HELPER FUNCTIONS ---------------------------

def batch_matrix(df_batch: pd.DataFrame, feature_cols):
    """Return X·Xᵀ for the dxm feature matrix X (d = len(feature_cols))."""
    X =  df_batch[feature_cols].to_numpy(dtype=float).T  # shape (d, m)
    # X -= np.mean(X, axis=1, keepdims=True)  # mean-center
    if X.size == 0:
        return np.zeros((len(feature_cols), len(feature_cols)))
    return X @ X.T

def cosine_cov(A: np.ndarray, B: np.ndarray, eps: float = 1e-12) -> float:
    """Cosine similarity of flattened matrices (aka inner‑product coefficient)."""
    num = np.trace(A @ B)
    den = math.sqrt(max(eps, np.trace(A @ A)) * max(eps, np.trace(B @ B)))
    return num / den

def rv_coeff(A: np.ndarray, B: np.ndarray, eps: float = 1e-12) -> float:
    """
    RV coefficient between two dxd matrices A, B.
    Returns 1.0 when both are 0, 0.0 when only one is 0.
    """
    num   = np.trace(A @ B)
    trA2  = np.trace(A @ A)
    trB2  = np.trace(B @ B)
    denom = math.sqrt(trA2 * trB2)

    if denom < eps:                 # at least one matrix is (near-)zero
        if trA2 < eps and trB2 < eps:
            return 1.0              # both zero → identical
        else:
            return 0.0              # only one zero → dissimilar
    return num / denom

# ---------------------------------------------------------------------------

# ------------------------------ LOAD DATA ----------------------------------
print("[INFO] Loading", params["input_csv"])
df = (
    pd.read_csv(params["input_csv"])
    .sort_values("current_time")
    .reset_index(drop=True)
)

df["dpx"] = df.px.diff().fillna(0)
df["dpy"] = df.py.diff().fillna(0)

print(df[["dpx", "dpy"]].head(10))

# assign batch IDs (integer) -------------------------------------------------
tau = params["batch_interval_tau"]
start_t0 = df["current_time"].iloc[0]
df["batch_id"] = ((df["current_time"] - start_t0) // tau).astype(int)


# build per-batch summaries --------------------------------------------------
batches = []
for bid, grp in df.groupby("batch_id"):
    batches.append({
        "batch_id": bid,
        "start_time": grp["current_time"].iloc[0],
        "end_time":   grp["current_time"].iloc[-1],
        "BBt": batch_matrix(grp, params["feature_columns"]),
        "n_points": len(grp)
    })

print("[INFO] Built", len(batches), "batches of", tau, "sec each")

# ------------------------------ SEGMENTATION -------------------------------

sigma = params["division_threshold_sigma"]
episodes = []

# index of first batch in the current episode
ep_start = 0

for j in range(1, len(batches)):
    print(f"[INFO] Processing batch {j} of {len(batches)}", end="\r")
    right = batches[j]
    left  = batches[j-1]

    # short-term change check (prev vs right)
    rv_val = rv_coeff(left["BBt"], right["BBt"])

    boundary = False
    if rv_val <= sigma:
        boundary = True
    else:
        # exponential look-back sizes 2,4,8,... batches
        max_left_len = j - ep_start  # how many batches in current episode so far
        k = 2   # look-back size = 2**(k-1)
        while 2**(k-1) <= max_left_len:
            left_len = 2**(k-1)
            left_start = j - left_len
            left_batches = batches[left_start:j]
            BBt_left_sum = sum(b["BBt"] for b in left_batches)
            rv_val = rv_coeff(BBt_left_sum, right["BBt"])

            if rv_val <= sigma:
                boundary = True
                break
            k += 1  # double look-back window size

    if boundary:
        # close episode from ep_start .. j-1
        ep_batches = batches[ep_start:j]
        episodes.append({
            "start_time": ep_batches[0]["start_time"],
            "end_time":   ep_batches[-1]["end_time"],
            "num_points": sum(b["n_points"] for b in ep_batches)
        })
        ep_start = j  # new episode starts at right batch

# add final episode ----------------------------------------------------------
final_batches = batches[ep_start:]
if final_batches:
    episodes.append({
        "start_time": final_batches[0]["start_time"],
        "end_time":   final_batches[-1]["end_time"],
        "num_points": sum(b["n_points"] for b in final_batches)
    })

# ------------------------------ SAVE OUTPUT --------------------------------

out_df = pd.DataFrame(episodes)
print("[INFO] Writing", params["output_csv"], "with", len(out_df), "episodes")
out_df.to_csv(params["output_csv"], index=False)

print("[DONE]")
