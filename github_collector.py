# Backward-compatible entrypoint. The implementation lives in src/ingestion.
from src.ingestion.github_collector import run_collector


if __name__ == "__main__":
    run_collector()
