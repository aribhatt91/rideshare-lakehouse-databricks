from typing import Any
import logging
import pandas as pd

from .models import TripCompletedEvent

# Configure a logger for this module
logger = logging.getLogger(__name__)

def nullable_float(value: Any) -> float | None:
    """Convert a nullable numeric value to float."""
    if value is None or pd.isna(value):
        logger.warning("Encountered null or NaN value for float conversion: %s", value)
        return None

    return float(value)


def create_trip_id(
    source_file: str,
    row_number: int,
) -> str:
    """
    Create a deterministic trip ID from the source file and row number.
    """
    return f"{source_file}:{row_number}"


def transform_row(
    row: dict[str, Any],
    source_file: str,
    row_number: int,
) -> TripCompletedEvent | None:
    """
    Transform a TLC trip record into a canonical trip-completed event.
    
    Returns:
        TripCompletedEvent if successful; None if the row is corrupt or missing critical data.
    """
    trip_id = create_trip_id(source_file=source_file, row_number=row_number)

    try:
        # 1. Strict Null & Type Checks for CRITICAL structural fields
        dropoff_dt = row.get("tpep_dropoff_datetime")
        if dropoff_dt is None:
            # raise ValueError("Missing critical timestamp: tpep_dropoff_datetime")
            event_time = None  # Use pandas NaT for missing timestamps
        else:
            # Safely handle if it's already a pydatetime or needs conversion
            event_time = (
                dropoff_dt.to_pydatetime() 
                if hasattr(dropoff_dt, "to_pydatetime") 
                else dropoff_dt
            )

        # 2. Defensive extraction using .get() to prevent KeyErrors
        # Enforce explicit fallback values or conversions for analytical metrics
        return TripCompletedEvent(
            event_id=f"{trip_id}:completed",
            trip_id=trip_id,
            event_time=event_time,
            vendor_id=int(row["VendorID"]) if row.get("VendorID") is not None else -1,
            pickup_location_id=int(row["PULocationID"]) if row.get("PULocationID") is not None else -1,
            dropoff_location_id=int(row["DOLocationID"]) if row.get("DOLocationID") is not None else -1,
            passenger_count=nullable_float(row.get("passenger_count")),
            trip_distance=float(row.get("trip_distance", 0.0)),
            rate_code_id=nullable_float(row.get("RatecodeID")),
            payment_type=int(row.get("payment_type", 0)),
            fare_amount=float(row.get("fare_amount", 0.0)),
            tip_amount=float(row.get("tip_amount", 0.0)),
            tolls_amount=float(row.get("tolls_amount", 0.0)),
            total_amount=float(row.get("total_amount", 0.0)),
        )

    except (KeyError, ValueError, TypeError, AttributeError) as e:
        # 3. Graceful Error Handling & Observability
        # Logs the specific failure context without crashing the entire run
        logger.error(
            f"Failed to transform row {row_number} in file '{source_file}'. "
            f"Error: {type(e).__name__}: {e}. Row contents: {row}"
        )
        return None
