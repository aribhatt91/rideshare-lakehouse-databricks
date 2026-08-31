"""
Caller script for the event simulator. This script is intended to be run as a standalone program.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from .replay import TripReplay
from .writer import S3EventWriter

load_dotenv()

file_path = os.getenv("TEST_FILE_PATH")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
batch_size = int(os.getenv("BATCH_SIZE", "5000"))  # Default to 5,000 if not set
speed = float(os.getenv("SPEED", "60"))  # Default to 60 if not set
eventstream_prefix = os.getenv("EVENTSTREAM_PREFIX", "eventstream/trips/")

def main() -> None:
    if(not file_path or not BUCKET_NAME):
        raise ValueError("TEST_FILE_PATH and S3_BUCKET_NAME must be set in the .env file.")

    parquet_file = Path(file_path)

    writer = S3EventWriter(
        bucket=BUCKET_NAME,
        prefix=eventstream_prefix,
    )

    replay = TripReplay(
        parquet_file=parquet_file,
        writer=writer,

        # 60 historical seconds = 1 real second
        speed=speed,

        # Number of events in each S3 file
        batch_size=batch_size,

        # Number of rows PyArrow reads at a time
        read_batch_size=10_000,
    )

    replay.run()


if __name__ == "__main__":
    main()
