import pandas as pd
import matplotlib.pyplot as plt

# Load benchmark results CSV
df = pd.read_csv("results.csv")

# Ensure column names are stripped
df.columns = df.columns.str.strip()

# Convert data types safely
df["KeyCount"] = pd.to_numeric(df["KeyCount"], errors="coerce")
df["SET RPS"] = pd.to_numeric(df["SET RPS"], errors="coerce")
df["GET RPS"] = pd.to_numeric(df["GET RPS"], errors="coerce")

# Separate by engine
gomap = df[df["Engine"] == "gomap"]
badger = df[df["Engine"] == "badger"]


# Plot
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

# SET RPS Plot
axes[0].plot(gomap["KeyCount"].values, gomap["SET RPS"].values, label="Gomap SET RPS", marker='o')
axes[0].plot(badger["KeyCount"].values, badger["SET RPS"].values, label="Badger SET RPS", marker='s')
axes[0].set_title("SET Requests per Second vs Key Count")
axes[0].set_xlabel("Key Count (log scale)")
axes[0].set_ylabel("Requests per Second")
axes[0].set_xscale("log")
axes[0].grid(True)
axes[0].legend()

# GET RPS Plot
axes[1].plot(gomap["KeyCount"].values, gomap["GET RPS"].values, label="Gomap GET RPS", marker='o')
axes[1].plot(badger["KeyCount"].values, badger["GET RPS"].values, label="Badger GET RPS", marker='s')
axes[1].set_title("GET Requests per Second vs Key Count")
axes[1].set_xlabel("Key Count (log scale)")
axes[1].set_ylabel("Requests per Second")
axes[1].set_xscale("log")
axes[1].grid(True)
axes[1].legend()

plt.tight_layout()
plt.savefig("benchmark_performance_combined.png")
plt.show()


