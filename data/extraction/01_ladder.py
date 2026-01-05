import pandas as pd
from datetime import datetime
from config import PLATFORM, PLATFORM_URL, BRONZE_DIR
from riot_client import api_request, SESSION


def get_ladder(top: int = 500) -> pd.DataFrame:
    """Get top X players from VN2 ladder (Challenger/GM/Master).

    Riot API now includes PUUID directly in league entries!
    """
    chall_url = (
        f"{PLATFORM_URL}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    )
    gm_url = f"{PLATFORM_URL}/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5"
    master_url = f"{PLATFORM_URL}/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5"

    all_entries = []

    # Challenger
    chall = api_request(chall_url, SESSION)
    if chall:
        for e in chall.get("entries", []):
            e["tier"] = "CHALLENGER"
            all_entries.append(e)
    print(f" len({all_entries}) CHALLENGER players")
    
    # Grandmaster
    if top > len(all_entries):
        gm = api_request(gm_url, SESSION)
        if gm:
            for e in gm.get("entries", []):
                e["tier"] = "GRANDMASTER"
                all_entries.append(e)
        print( f" len({all_entries}) GRANDMASTER players")

    # Master
    if top > len(all_entries):
        master = api_request(master_url, SESSION)
        if master:
            for e in master.get("entries", []):
                e["tier"] = "MASTER"
                all_entries.append(e)
        print(f" len({all_entries}) MASTER players")

    # DataFrame
    df = pd.DataFrame(all_entries)
    
    df = (
        df.sort_values("leaguePoints", ascending=False).head(top).reset_index(drop=True)
    )

    # Rename columns
    df = df.rename(columns={"leaguePoints": "league_points"})

    # Add metadata 
    df["platform"] = PLATFORM.upper()  
    df["ingest_ts"] = datetime.now().isoformat()

    # Select columns 
    cols = [
        "platform",
        "puuid",
        "tier",
        "rank",
        "league_points",
        "wins",
        "losses",
        "ingest_ts",
    ]
    df = df[[c for c in cols if c in df.columns]]

    return df


def main():
    df = get_ladder(top=500)
    # Save
    output_path = BRONZE_DIR / "ladder.parquet"
    df.to_parquet(output_path)

if __name__ == "__main__":
    main()
