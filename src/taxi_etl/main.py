import logging

from dotenv import load_dotenv

from taxi_etl.config import Settings
from taxi_etl.pipeline import run_pipeline


def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_pipeline(settings)


if __name__ == "__main__":
    main()
