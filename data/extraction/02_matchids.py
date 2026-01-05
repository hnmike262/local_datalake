import logging
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from config import PLATFORM, REGION, QUEUE, REGIONAL_URL, BRONZE_DIR
from riot_client import api_request, SESSION

def get_match_ids(puuid: str, count: int = 20) -> list:
    """Get match IDs for a player"""
    url = f"{REGIONAL_URL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
    params = f"?queue={QUEUE}&start=0&count={count}"
    result = api_request(url + params, SESSION)
    return result  

def main():
    # Load ladder
    output_path = BRONZE_DIR / "ladder.parquet"
    ladder_df = pd.read_parquet(output_path)
    puuids = ladder_df["puuid"].dropna().tolist()

    # Fetch match IDs
    all_matches = []

    for puuid in tqdm(puuids):
        try:
            match_ids = get_match_ids(puuid, count=20)
            for mid in match_ids:
                all_matches.append(
                    {
                        "match_id": mid,
                        "puuid": puuid,
                        "platform": PLATFORM,
                        "region": REGION,
                        "queue_id": QUEUE,
                        "ingest_ts": datetime.now().isoformat(),
                    }
                )
            time.sleep(0.05)
        except Exception as e:
            print(f" Failed to fetch matches for PUUID {puuid}: {e}")
            continue

    if not all_matches:
        print(" No match IDs fetched.")
        return

    df = pd.DataFrame(all_matches)
    df = df.drop_duplicates(subset=["match_id"])

    # Save
    output_path = BRONZE_DIR / "match_ids.parquet"
    df.to_parquet(output_path, index=False)


if __name__ == "__main__":
    main()
