import pandas as pd
import matplotlib.pyplot as plt

# Load CSV benchmark data
df = pd.read_csv("results.csv")

# Convert all numeric columns safely
df["KeyCount"] = pd.to_numeric(df["KeyCount"], errors="coerce")
df["SET RPS"] = pd.to_numeric(df["SET RPS"], errors="coerce")
df["GET RPS"] = pd.to_numeric(df["GET RPS"], errors="coerce")
df["SET p50"] = pd.to_numeric(df["SET p50"], errors="coerce")
df["GET p50"] = pd.to_numeric(df["GET p50"], errors="coerce")

# Separate engines
gomap = df[df["Engine"] == "gomap"]
badger = df[df["Engine"] == "badger"]

# Create figure with 2x2 subplots
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# ---- SET RPS ----
axes[0, 0].plot(gomap["KeyCount"].to_numpy(), gomap["SET RPS"].to_numpy(), label="Gomap", marker='o')
axes[0, 0].plot(badger["KeyCount"].to_numpy(), badger["SET RPS"].to_numpy(), label="Badger", marker='s')
axes[0, 0].set_title("SET RPS")
axes[0, 0].set_ylabel("Requests/sec")
axes[0, 0].set_xlabel("Key Count")
axes[0, 0].set_xscale("log")
axes[0, 0].set_ylim(bottom=0)
axes[0, 0].legend()
axes[0, 0].grid(True)

# ---- GET RPS ----
axes[0, 1].plot(gomap["KeyCount"].to_numpy(), gomap["GET RPS"].to_numpy(), label="Gomap", marker='o')
axes[0, 1].plot(badger["KeyCount"].to_numpy(), badger["GET RPS"].to_numpy(), label="Badger", marker='s')
axes[0, 1].set_title("GET RPS")
axes[0, 1].set_ylabel("Requests/sec")
axes[0, 1].set_xlabel("Key Count")
axes[0, 1].set_xscale("log")
axes[0, 1].set_ylim(bottom=0)
axes[0, 1].legend()
axes[0, 1].grid(True)

# ---- SET p50 Latency ----
axes[1, 0].plot(gomap["KeyCount"].to_numpy(), gomap["SET p50"].to_numpy(), label="Gomap", marker='o')
axes[1, 0].plot(badger["KeyCount"].to_numpy(), badger["SET p50"].to_numpy(), label="Badger", marker='s')
axes[1, 0].set_title("SET p50 Latency")
axes[1, 0].set_ylabel("Latency (ms)")
axes[1, 0].set_xlabel("Key Count")
axes[1, 0].set_xscale("log")
axes[1, 0].set_ylim(bottom=0)
axes[1, 0].legend()
axes[1, 0].grid(True)

# ---- GET p50 Latency ----
axes[1, 1].plot(gomap["KeyCount"].to_numpy(), gomap["GET p50"].to_numpy(), label="Gomap", marker='o')
axes[1, 1].plot(badger["KeyCount"].to_numpy(), badger["GET p50"].to_numpy(), label="Badger", marker='s')
axes[1, 1].set_title("GET p50 Latency")
axes[1, 1].set_ylabel("Latency (ms)")
axes[1, 1].set_xlabel("Key Count")
axes[1, 1].set_xscale("log")
axes[1, 1].set_ylim(bottom=0)
axes[1, 1].legend()
axes[1, 1].grid(True)

# Finalize
plt.tight_layout()
plt.savefig("benchmark_performance_combined.png")
print("✅ Saved benchmark_performance_combined.png")


