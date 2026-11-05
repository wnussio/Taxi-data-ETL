import logging

import lmstudio as lms

logger = logging.getLogger(__name__)


class LLMEnricher:
    """Generuje analizę wzorców godzinowych przez lokalny model LM Studio."""

    def __init__(self, model_name: str, api_host: str | None = None):
        if api_host:
            # must run before the first lms.llm() call
            lms.configure_default_client(api_host)
        logger.info("Connecting to LM Studio model %s", model_name)
        self.model = lms.llm(model_name)

    def analyze_hourly_patterns(self, hourly_df) -> str:
        """
        Przyjmuje mały (24-wierszowy) Spark DataFrame z agregacją per godzina
        (kolumny: godzina, liczba_kursow, avg_duration_min, avg_distance, avg_fare, ...).
        Zwraca tekst analizy wygenerowany przez LLM.
        """
        dane_pd = hourly_df.orderBy("godzina").toPandas()
        dane_tekst = dane_pd.to_string(index=False)

        prompt = f"""Poniżej znajdują się zagregowane dane o kursach taksówek w Nowym Jorku,
pogrupowane według godziny rozpoczęcia kursu (0-23).

Dane:
{dane_tekst}

Kolumny: liczba_kursow (liczba kursów w tej godzinie), avg_duration_min (średni czas
przejazdu w minutach), avg_distance (średni dystans w milach), avg_fare (średnia cena
w dolarach), total_duration_min / total_distance / total_fare (sumy dla tej godziny).

Na podstawie tych danych wskaż 3-5 najważniejszych wzorców lub anomalii (np. godziny
szczytu, godziny o najdłuższych/najkrótszych przejazdach, nietypowe zależności między
czasem przejazdu a dystansem, co może sugerować kongestię). Napisz krótko, rzeczowo,
w punktach, po polsku. Nie zmyślaj liczb, które nie wynikają wprost z danych."""

        result = self.model.respond(prompt)
        return str(result)