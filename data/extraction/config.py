import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv

# Load .env file from project root
ROOT_DIR: Final[Path] = Path(__file__).parent.parent.parent
load_dotenv(ROOT_DIR / ".env", override=True)

# Riot API 
RIOT_API_KEY: Final[str] = os.getenv("RIOT_API_KEY", "")
PLATFORM: Final[str] = os.getenv("RIOT_PLATFORM", "vn2")
REGION: Final[str] = os.getenv("RIOT_REGION", "sea")
QUEUE: Final[int] = int(os.getenv("RIOT_QUEUE", "420"))

# API URLs
PLATFORM_URL: Final[str] = f"https://{PLATFORM}.api.riotgames.com"
REGIONAL_URL: Final[str] = f"https://{REGION}.api.riotgames.com"

# File Paths
BASE_DIR: Final[Path] = Path(__file__).parent.parent
BRONZE_DIR: Final[Path] = BASE_DIR / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# MinIO Configuration
MINIO_ENDPOINT: Final[str] = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: Final[str] = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY: Final[str] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET: Final[str] = os.getenv("MINIO_BUCKET", "lol-bronze")
