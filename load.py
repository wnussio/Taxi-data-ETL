from sqlalchemy import create_engine


class PostgresLoader:
    """Zapisuje zagregowane wyniki (Spark DataFrame -> pandas -> PostgreSQL)."""

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def save_gold(self, gold_df, table_name: str = "gold_hourly_zone"):
        """Zapisuje tabelę Gold. UWAGA: gold_df musi być już małym, zagregowanym
        Spark DataFrame - ta metoda konwertuje go do pandas przed zapisem."""
        gold_pd = gold_df.toPandas()
        gold_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        print(f"Zapisano do PostgreSQL: {table_name} ({len(gold_pd)} wierszy)")

    def save_rejected_summary(self, rejected_summary_df, table_name: str = "rejected_summary"):
        """Zapisuje podsumowanie odrzuceń (mała tabela: reason + liczba)."""
        rejected_pd = rejected_summary_df.toPandas()
        rejected_pd.to_sql(table_name, self.engine, if_exists="replace", index=False)
        print(f"Zapisano do PostgreSQL: {table_name} ({len(rejected_pd)} wierszy)")