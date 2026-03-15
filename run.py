import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys

parser = argparse.ArgumentParser()

parser.add_argument("--input", required=True)
parser.add_argument("--config", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--log-file", required=True)

args = parser.parse_args()

logging.basicConfig(
    filename=args.log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

start_time = time.time()

try:
    logging.info("Job started")

    with open(args.config) as f:
        config = yaml.safe_load(f)

    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    np.random.seed(seed)

    logging.info("Config loaded")

    df = pd.read_csv(args.input)

    if df.empty:
        raise Exception("Dataset empty")

    if "close" not in df.columns:
        raise Exception("Column close missing")

    logging.info(f"Rows loaded: {len(df)}")

    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    logging.info("Rolling mean calculated")

    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

    rows_processed = len(df)
    signal_rate = float(df["signal"].mean())

    latency_ms = int((time.time() - start_time) * 1000)

    metrics = {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": round(signal_rate, 4),
        "latency_ms": latency_ms,
        "seed": seed,
        "status": "success"
    }

    with open(args.output, "w") as f:
        json.dump(metrics, f, indent=2)

    logging.info("Job finished successfully")

    print(json.dumps(metrics, indent=2))

except Exception as e:

    error = {
        "version": "v1",
        "status": "error",
        "error_message": str(e)
    }

    with open(args.output, "w") as f:
        json.dump(error, f, indent=2)

    logging.error(str(e))

    print(json.dumps(error, indent=2))

    sys.exit(1)