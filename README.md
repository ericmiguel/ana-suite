# ana-suite

Data suite for the Brazilian National Water and Basic Sanitation Agency (ANA)
station inventory and hydrometeorological time series. The package has no CLI:
collection is exposed through typed Python requests and `Experiment`.

```python
from datetime import date

from ana import AnaRequest, Experiment, LiveHorizon, StationCodes

experiment = Experiment(
    name="sao_paulo_rain",
    rainfall=AnaRequest(
        selection=StationCodes(("2345000",)),
        start=date(2020, 1, 1),
        end=LiveHorizon.TODAY,
        variable="chuva",
    ),
)
experiment.download()
experiment.to_parquet()
observations = experiment.open("observations")
```

## Data model

`AnaRequest` selects a variable, period, station type, and optionally station
codes, a bounding box, or a vector geometry. Omitting the selection keeps the
whole ANA inventory. `LiveHorizon.TODAY` is deliberately part of the request
identity, not the resolved date, so repeated runs extend one experiment instead
of creating a new cache.

The canonical store is written at
`data/ana/<cache-key>.parquet/`, with `stations` and partitioned `observations`
tables. The raw station cache lives at `.cache/ana/<cache-key>/`.

## Incremental and live cache

ANA's SOAP service is slow and its station availability changes over time. The
cache therefore stores, for every station and variable:

- downloaded observations, merged by timestamp with newer values winning;
- every queried date interval, including confirmed empty intervals;
- the latest known data range and station status;
- a dynamic retry TTL for inactive stations;
- a cooldown after consecutive transport errors.

Only uncovered intervals and expired empty intervals are queried. Each response
is persisted before the next concurrent response is processed, and metadata and
Parquet files are replaced atomically. This makes a live run resumable after an
interruption and allows a station that returns after a period of inactivity to
be discovered on a later run.

## Quality

```bash
uv run ruff check .
uv run ruff format . --check
uv run pyrefly check
uv run pytest
```
