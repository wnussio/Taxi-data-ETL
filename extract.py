import os
import requests


class TaxiDataExtractor:
    """Pobiera pliki Parquet z danymi NYC Taxi z publicznego serwera TLC."""

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def __init__(self, year: int, month: int, type: str='yellow_tripdata'):
        self.year = year
        self.month = month
        self.type=type


    def get_filename(self) -> str:
        """Zwraca nazwę pliku, np. 'yellow_tripdata_2024-01.parquet'."""
        

        return f"{self.type}_{self.year}-{self.month:02d}.parquet"
        

    def get_url(self) -> str:
        """Zwraca pełny URL do pliku na serwerze TLC."""
        
        return f'{self.BASE_URL}/{TaxiDataExtractor.get_filename(self)}'

    def get_local_path(self, destination_folder: str = "data/raw") -> str:
        """Zwraca lokalną ścieżkę docelową pliku."""

        return os.path.join(destination_folder, self.get_filename())

    def download(self, destination_folder: str = "data/raw") -> str:
        """Pobiera plik, jeśli jeszcze nie istnieje lokalnie. Zwraca ścieżkę do pliku."""
        os.makedirs(destination_folder, exist_ok=True)

        local_path = self.get_local_path(destination_folder)

        if os.path.exists(local_path):
            print(f"Plik już istnieje: {local_path}")
            return local_path

        url = self.get_url()
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception(f"Nie udało się pobrać pliku, status: {response.status_code}")

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Pobrano: {local_path}")
        return local_path

                

        


if __name__ == "__main__":
    extractor = TaxiDataExtractor(year=2024, month=1)
    path = extractor.download()
    print(f"Dane zapisane w: {path}")