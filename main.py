import pathlib

from engine.engine import AnalyticsEngine, EngineConfig
from engine.ingestion import IngestionManager

THIS_DIR = pathlib.Path(__file__).parent

engine = AnalyticsEngine(
    data_dir=THIS_DIR / "jlytics_db",
    config=EngineConfig(target_partitions=4, batch_size=8192),
)

ingestor = IngestionManager(engine)


def main() -> None:
    """Ingest the airports dataset into the engine."""
    ingestor.ingest_file(THIS_DIR / "data" / "airports.parquet", "airports")


def get_airports():
    """Query the airports table and print the result and optimised plan.

    Returns:
        A :class:`~engine.result.QueryResult` containing all airport rows.
    """
    #main()
    result = engine.execute_sql("SELECT * FROM public.airports WHERE iata = 'ABR' AND country = 'United States'")
    print(result)
    print(result.table)
    plans = engine.explain_sql("SELECT * FROM airports")
    print(f"Optimized Plan:\n {plans['optimized']}")
    return result


if __name__ == "__main__":
    get_airports()
