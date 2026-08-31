import time
from pathlib import Path

import pyarrow.parquet as pq

from .models import TripCompletedEvent
from .transformer import transform_row
from .writer import S3EventWriter


class TripReplay:
    """
    Replays historical TLC trip records as if they were
    arriving in real time.

    The historical event_time is preserved.

    speed=60 means:

        60 historical seconds
        =
        1 real second
    """

    def __init__(
        self,
        parquet_file: Path,
        writer: S3EventWriter,
        speed: float = 60.0,
        batch_size: int = 1_000,
        read_batch_size: int = 10_000,
    ) -> None:

        if speed <= 0:
            raise ValueError("speed must be greater than zero")

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        if read_batch_size <= 0:
            raise ValueError(
                "read_batch_size must be greater than zero"
            )

        self.parquet_file = parquet_file
        self.writer = writer

        self.speed = speed
        self.batch_size = batch_size
        self.read_batch_size = read_batch_size

    def run(self) -> None:
        """
        Start replaying the historical dataset.
        """

        parquet = pq.ParquetFile(
            self.parquet_file
        )

        print(
            f"Starting replay:"
            f"\n  Source: {self.parquet_file}"
            f"\n  Speed: {self.speed}x"
            f"\n  Event batch size: {self.batch_size:,}"
        )

        events: list[TripCompletedEvent] = []

        previous_event_time = None
        row_number = 0
        batch_number = 0

        batches = parquet.num_row_groups
        print(f"Total batches: {batches}")

        for record_batch in parquet.iter_batches(
            batch_size=self.read_batch_size
        ):
            rows = record_batch.to_pylist()
            print(f"Processing batch {batch_number} with {len(rows):,} rows...")
            batch_number += 1

            for row in rows:

                event = transform_row(
                    row=row,
                    source_file=self.parquet_file.name,
                    row_number=row_number,
                )

                row_number += 1

                if event is None:
                    # Skip the rest of this iteration and move to the next row
                    print(f"Skipping row {row_number:,} due to missing critical data.")
                    continue

                # -------------------------------------------------
                # Replay historical time
                # -------------------------------------------------

                if previous_event_time is not None:

                    historical_delta = (
                        event.event_time
                        - previous_event_time
                    ).total_seconds()

                    # If the source contains an out-of-order event,
                    # don't sleep. Emit it immediately.
                    if historical_delta > 0:
                        real_sleep_seconds = (historical_delta / self.speed)
                        time.sleep(real_sleep_seconds)

                previous_event_time = event.event_time

                print(event)

                # -------------------------------------------------
                # Add event to current output batch
                # -------------------------------------------------

                events.append(event)
                

                if len(events) >= self.batch_size:
                    print(f"Writing batch of {len(events):,} events...")
                    self.writer.write_batch(events)

                    events = []

        # ---------------------------------------------------------
        # Write final partial batch
        # ---------------------------------------------------------

        if events:
            print(f"Writing final batch of {len(events):,} events...")
            self.writer.write_batch(events)

        print(
            f"Replay complete. "
            f"Processed {row_number:,} source records."
        )
