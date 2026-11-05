import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Zapisuje zagregowane wyniki (Spark DataFrame -> pandas -> PostgreSQL)."""

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def save_gold(self, gold_df, table_name: str = "gold_hourly_zone"):
        # zapisuje gold, tam jest pandas zeby ominac java/.jar
        gold_pd = gold_df.toPandas()
        gold_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        logger.info("Saved %s to PostgreSQL (%d rows)", table_name, len(gold_pd))

    def save_rejected_summary(self, rejected_summary_df, table_name: str = "rejected_summary"):
        #Zapisuje podsumowanie odrzuceń (mała tabela: reason + liczba).
        rejected_pd = rejected_summary_df.toPandas()
        rejected_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        logger.info("Saved %s to PostgreSQL (%d rows)", table_name, len(rejected_pd))

    def save_llm_analysis(self, analysis_text: str, table_name: str = "llm_analysis"):
        #zapisuje analize jako pojedynczy tekst
        df = pd.DataFrame([{
            "generated_at": datetime.now(),
            "analysis": analysis_text
        }])
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        logger.info("Saved LLM analysis to %s", table_name)