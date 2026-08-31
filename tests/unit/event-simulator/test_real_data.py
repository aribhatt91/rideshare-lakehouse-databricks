from pathlib import Path
import logging
import pyarrow.parquet as pq

from event_simulator import transformer

file_path = r"C:/Users/aribh/Downloads/2026-01/yellow_tripdata_2026-01.parquet"

PARQUET_FILE = Path(
    file_path
)

logger = logging.getLogger(__name__)

def main() -> None:
    parquet_file = pq.ParquetFile(PARQUET_FILE)

    global_row_number = 0

    for batch in parquet_file.iter_batches(
        batch_size=5,
    ):
        rows = batch.to_pylist()
        logger.info(f"Processing batch with {len(rows)} rows...")
        
        for row in rows:
            event = transformer.transform_row(
                row=row,
                source_file=PARQUET_FILE.name,
                row_number=global_row_number,
            )

            logger.info(f"Transformed event: {event.model_dump_json()}")

            global_row_number += 1

            if global_row_number == 5:
                return
        

if __name__ == "__main__":
    main()
