# mlops-task
MLOps batch job with rolling mean signal generation
# MLOps Task
This project implements a batch data processing pipeline that computes a rolling mean signal from market data.
## Features
- Reads CSV market data
- Computes rolling mean signal
- Outputs metrics in JSON
- Logs execution details
- Containerized using Docker
## How to Run
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
