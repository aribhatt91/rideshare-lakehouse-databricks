from datetime import datetime

from pydantic import BaseModel, ConfigDict


class TripCompletedEvent(BaseModel):
    model_config = ConfigDict(
        extra="forbid"
    )

    event_id: str
    event_type: str = "trip_completed"

    trip_id: str

    event_time: datetime
    ingestion_time: datetime | None = None

    vendor_id: int

    pickup_location_id: int
    dropoff_location_id: int

    passenger_count: float | None = None
    trip_distance: float

    rate_code_id: float | None = None
    payment_type: int

    fare_amount: float
    tip_amount: float
    tolls_amount: float
    total_amount: float