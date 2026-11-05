import logging

from taxi_etl.config import Settings
from taxi_etl.enrich import LLMEnricher
from taxi_etl.extract import TaxiDataExtractor
from taxi_etl.load import PostgresLoader
from taxi_etl.spark import get_spark
from taxi_etl.transform import TaxiTransformer

logger = logging.getLogger(__name__)


def run_pipeline(settings: Settings, app_name: str = "TaxiETL") -> None:
    """Extract -> transform (bronze/silver/gold) -> load -> LLM enrichment."""
    logger.info("Starting pipeline for %d-%02d", settings.taxi_year, settings.taxi_month)

    # --- Extract ---
    extractor = TaxiDataExtractor(year=settings.taxi_year, month=settings.taxi_month)
    raw_path = extractor.download(destination_folder=settings.data_dir)

    spark = get_spark(app_name)
    try:
        # --- Transform ---
        transformer = TaxiTransformer(spark)
        bronze = transformer.load_bronze(raw_path)
        silver, rejected = transformer.clean_to_silver(bronze)
        gold = transformer.aggregate_to_gold(silver)
        rejected_summary = transformer.summarize_rejections(rejected)
        llm_agg = transformer.aggregate_for_llm(silver)

        # --- Load ---
        loader = PostgresLoader(settings.db_url)
        loader.save_gold(gold)
        loader.save_rejected_summary(rejected_summary)

        # --- Enrich ---
        enricher = LLMEnricher(settings.llm_model, api_host=settings.llm_host)
        analysis = enricher.analyze_hourly_patterns(llm_agg)
        logger.info("LLM analysis generated (%d characters)", len(analysis))
        loader.save_llm_analysis(analysis)
    finally:
        spark.stop()

    logger.info("Pipeline finished")
