# Backward-compatible entrypoint. The implementation lives in src/processing.
from src.processing.spark_streaming import main


if __name__ == "__main__":
    main()
