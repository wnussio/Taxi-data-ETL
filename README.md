# NYC Taxi ETL Pipeline

End-to-end pipeline analizujący dane o kursach taksówek w Nowym Jorku: od pobrania
surowych danych, przez transformacje w Apache Spark, po zapis wyników w PostgreSQL
i wzbogacenie o analizę wygenerowaną lokalnie przez LLM (LM Studio).

## Architektura

```
NYC TLC (Parquet, publiczne API)
        │  Extract
        ▼
   Spark: Bronze (surowe dane)
        │  Transform (czyszczenie)
        ▼
   Spark: Silver (oczyszczone dane + log odrzuceń z przyczyną)
        │  Transform (agregacja)
        ▼
   Spark: Gold (agregacja godzina × strefa odbioru)
        │  Load
        ▼
   PostgreSQL (gold_hourly_zone, rejected_summary)
        │  Enrich
        ▼
   LLM (LM Studio) → analiza wzorców godzinowych
        │  Load
        ▼
   PostgreSQL (llm_analysis)
```

## Dlaczego tak

- **Medallion architecture (Bronze/Silver/Gold)** — surowe dane nigdy nie są
  bezpośrednio modyfikowane; każda warstwa jest odtwarzalna od nowa z poprzedniej.
- **Audytowalne czyszczenie danych** — żaden rekord nie znika bez śladu. Każdy
  odrzucony wiersz ma przypisaną przyczynę (`too_short`, `bad_distance`,
  `high_price` itd.), zliczoną w `rejected_summary`.
- **Próg cenowy oparty o percentyl (99.5), nie sztywną liczbę** — dostosowuje się
  do rzeczywistego rozkładu danych zamiast zakładać z góry, co jest "za drogo".
- **Gold trafia do PostgreSQL, surowe dane nie** — PostgreSQL trzyma tylko już
  zagregowane, gotowe do zapytań wyniki (tysiące wierszy), nie miliony surowych
  transakcji, które nikt nie odpytuje wiersz po wierszu.
- **LLM jako analityk, nie jako baza danych** — model dostaje już zagregowane,
  24-wierszowe podsumowanie godzinowe i generuje interpretację w języku
  naturalnym, zamiast przetwarzać surowe dane.

## Struktura projektu

```
├── extract.py    # TaxiDataExtractor - pobieranie danych z NYC TLC
├── transform.py  # TaxiTransformer - logika Spark (Bronze/Silver/Gold)
├── load.py       # PostgresLoader - zapis wyników do PostgreSQL
├── enrich.py     # LLMEnricher - analiza wzorców przez LM Studio
├── main.py       # orkiestracja całego pipeline'u
└── transform_exploration.ipynb  # notebook roboczy z eksploracją transformacji
```

## Uruchomienie

1. Zainstaluj zależności: `pip install -r requirements.txt`
2. Utwórz plik `.env` z zawartością: `DB_PASSWORD=twoje_haslo`
3. Upewnij się, że masz uruchomiony serwer PostgreSQL i LM Studio (z załadowanym modelem)
4. Uruchom: `python main.py`

## Znane ograniczenia

- Model LLM (lokalny, mniejszy model w LM Studio) czasem generuje wyjaśnienia
  przyczynowe, które nie wynikają wprost z danych (np. sugeruje przyczyny
  zjawisk, których dane nie potwierdzają) — wynik LLM należy traktować jako
  punkt wyjścia do analizy, nie ostateczny wniosek.
- Kwarantanna cenowa (percentyl 99.5) może wyjść pusta dla miesięcy bez
  ekstremalnych wartości odstających — to oczekiwane zachowanie, nie błąd.
- Projekt przetwarza jeden miesiąc danych na raz; rozszerzenie o wiele miesięcy
  wymaga jedynie zmiany parametrów w `TaxiDataExtractor`.

## Źródło danych

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
