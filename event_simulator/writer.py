import logging
import time
import uuid
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from .models import TripCompletedEvent

# Set up logging for infrastructure alerts
logger = logging.getLogger(__name__)

class S3EventWriter:
    """
    Writes event batches to S3 as JSON Lines files securely.
    Includes network resilience and automatic partition formatting.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3")
        
        self.batch_number = 0
        # FIX 1: Generate a unique ID for this execution run to prevent overwrites
        self.run_id = str(uuid.uuid4())[:8] 
        
        # Network resilience tuning
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def write_batch(self, events: list[TripCompletedEvent]) -> str:
        """
        Write one batch of events to S3 securely with automatic retry handling.
        """
        if not events:
            raise ValueError("Cannot write an empty event batch.")

        self.batch_number += 1
        lines: list[str] = []
        ingestion_time = datetime.now(timezone.utc)

        for event in events:
            event.ingestion_time = ingestion_time
            lines.append(event.model_dump_json())

        body = "\n".join(lines) + "\n"

        # FIX 2: Create a Hive-partitioned directory layout using current date
        # Format: prefix/year=YYYY/month=MM/day=DD/run_XXXX_batch_00000001.jsonl
        date_partition = ingestion_time.strftime("year=%Y/month=%m/day=%d")
        key = (
            f"{self.prefix}/{date_partition}/"
            f"run-{self.run_id}_batch-{self.batch_number:08d}.jsonl"
        )

        # FIX 3: Robust upload wrapper with Exponential Backoff
        for attempt in range(1, self.max_retries + 1):
            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body.encode("utf-8"),
                    ContentType="application/x-ndjson",
                )
                print(f"Wrote {len(events):,} events → s3://{self.bucket}/{key}")
                return key

            except (ClientError, Exception) as e:
                if attempt == self.max_retries:
                    print(f"Fatal: Failed to upload {key} after {self.max_retries} attempts.")
                    logger.critical(f"Fatal: Failed to upload {key} after {self.max_retries} attempts.")
                    raise e  # Crash explicitly if retries are fully exhausted
                
                # Calculate sleep timing: 0.5s, 1.0s, 2.0s...
                sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                print(
                    f"S3 upload failed (Attempt {attempt}/{self.max_retries}). "
                    f"Retrying in {sleep_time}s... Error: {e}"
                )
                logger.warning(
                    f"S3 upload failed (Attempt {attempt}/{self.max_retries}). "
                    f"Retrying in {sleep_time}s... Error: {e}"
                )
                time.sleep(sleep_time)

        return key