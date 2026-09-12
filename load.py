from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

class PostgresLoader:
    """Zapisuje zagregowane wyniki (Spark DataFrame -> pandas -> PostgreSQL)."""

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def save_gold(self, gold_df, table_name: str = "gold_hourly_zone"):
        # zapisuje gold, tam jest pandas zeby ominac java/.jar
        gold_pd = gold_df.toPandas()
        gold_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        print(f"Zapisano do PostgreSQL: {table_name} ({len(gold_pd)} wierszy)")

    def save_rejected_summary(self, rejected_summary_df, table_name: str = "rejected_summary"):
        #Zapisuje podsumowanie odrzuceń (mała tabela: reason + liczba).
        rejected_pd = rejected_summary_df.toPandas()
        rejected_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        print(f"Zapisano do PostgreSQL: {table_name} ({len(rejected_pd)} wierszy)")

    def save_llm_analysis(self, analysis_text: str, table_name: str = "llm_analysis"):
        #zapisuje analize jako pojedynczy tekst
        df = pd.DataFrame([{
            "generated_at": datetime.now(),
            "analysis": analysis_text
        }])
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        print(f"Zapisano analizę LLM do: {table_name}")