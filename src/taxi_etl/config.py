import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Pipeline settings read from environment variables (12-factor config)."""

    db_password: str = field(repr=False)
    db_user: str
    db_host: str
    db_port: int
    db_name: str
    taxi_year: int
    taxi_month: int
    data_dir: str = "data/raw"
    llm_model: str = "qwen/qwen2.5-coder-14b"
    llm_host: str = "localhost:1234"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        password = os.environ.get("DB_PASSWORD")
        if not password:
            raise RuntimeError("DB_PASSWORD is not set. Copy .env.example to .env and fill it in.")
        return cls(
            db_password=password,
            db_user=os.environ.get("DB_USER", "postgres"),
            db_host=os.environ.get("DB_HOST", "localhost"),
            db_port=int(os.environ.get("DB_PORT", "5432")),
            db_name=os.environ.get("DB_NAME", "taxi"),
            taxi_year=int(os.environ.get("TAXI_YEAR", "2024")),
            taxi_month=int(os.environ.get("TAXI_MONTH", "1")),
            data_dir=os.environ.get("DATA_DIR", "data/raw"),
            llm_model=os.environ.get("LLM_MODEL", "qwen/qwen2.5-coder-14b"),
            llm_host=os.environ.get("LLM_HOST", "localhost:1234"),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )

    @property
    def db_url(self) -> str:
        """SQLAlchemy connection string. Contains the password: never log it."""
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )
