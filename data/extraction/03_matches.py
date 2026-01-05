import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from config import PLATFORM, REGION, REGIONAL_URL, BRONZE_DIR
from riot_client import api_request, SESSION


def get_match_detail(match_id: str) -> dict | None:
    """Fetch match detail from Riot API."""
    url = f"{REGIONAL_URL}/lol/match/v5/matches/{match_id}"
    return api_request(url, SESSION)


def process_participants(match_json: dict) -> list[dict]:
    """Extract participants from match."""
    side_dict = {100: "blue", 200: "red"}
    rows = []

    info = match_json["info"]
    metadata = match_json["metadata"]
    match_id = metadata["matchId"]

    for p in info["participants"]:
        perks = p.get("perks", {})
        styles = perks.get("styles", [{}, {}])
        stat_perks = perks.get("statPerks", {})

        primary = styles[0] if len(styles) > 0 else {}
        secondary = styles[1] if len(styles) > 1 else {}
        primary_sel = primary.get("selections", [{}, {}, {}, {}])
        secondary_sel = secondary.get("selections", [{}, {}])

        rows.append(
            {
                # Match metadata
                "match_id": match_id,
                "platform": info["platformId"],
                "queue_id": info["queueId"],
                "game_mode": info["gameMode"],
                "game_version": info["gameVersion"],
                "game_duration": info.get("gameDuration", 0),
                "game_creation": info["gameCreation"],
                "game_start_timestamp": info["gameStartTimestamp"],
                "game_end_timestamp": info["gameEndTimestamp"],
                # Player identity
                "puuid": p["puuid"],
                "summoner_name": p.get("summonerName", ""),
                "riot_id_name": p.get("riotIdGameName", ""),
                "riot_id_tagline": p.get("riotIdTagline", ""),
                "participant_id": p["participantId"],
                "team_id": p["teamId"],
                "side": side_dict.get(p["teamId"], "unknown"),
                # Champion & Role
                "champion_id": p["championId"],
                "champion_name": p["championName"],
                "team_position": p["teamPosition"],
                "lane": p["lane"],
                "win": p["win"],
                # KDA
                "kills": p["kills"],
                "deaths": p["deaths"],
                "assists": p["assists"],
                # Economy
                "gold_earned": p["goldEarned"],
                "total_minions_killed": p["totalMinionsKilled"],
                "neutral_minions_killed": p.get("neutralMinionsKilled", 0),
                "total_ally_jungle_minions_killed": p.get(
                    "totalAllyJungleMinionsKilled", 0
                ),
                "total_enemy_jungle_minions_killed": p.get(
                    "totalEnemyJungleMinionsKilled", 0
                ),
                # Damage
                "total_damage_dealt_to_champions": p["totalDamageDealtToChampions"],
                "total_damage_taken": p["totalDamageTaken"],
                "damage_self_mitigated": p["damageSelfMitigated"],
                "total_damage_shielded_on_teammates": p[
                    "totalDamageShieldedOnTeammates"
                ],
                "total_heals_on_teammates": p["totalHealsOnTeammates"],
                "damage_dealt_to_buildings": p["damageDealtToBuildings"],
                "damage_dealt_to_objectives": p["damageDealtToObjectives"],
                # Objectives
                "dragon_kills": p["dragonKills"],
                "turret_kills": p["turretKills"],
                "turrets_lost": p["turretsLost"],
                "objectives_stolen": p["objectivesStolen"],
                # Vision
                "vision_score": p["visionScore"],
                "wards_placed": p["wardsPlaced"],
                "wards_killed": p["wardsKilled"],
                "control_wards_placed": p["detectorWardsPlaced"],
                # Game flow
                "first_blood_kill": p["firstBloodKill"],
                "first_blood_assist": p["firstBloodAssist"],
                "first_tower_kill": p["firstTowerKill"],
                "first_tower_assist": p["firstTowerAssist"],
                "game_ended_in_early_surrender": p["gameEndedInEarlySurrender"],
                "game_ended_in_surrender": p["gameEndedInSurrender"],
                "longest_time_spent_living": p["longestTimeSpentLiving"],
                "largest_killing_spree": p["largestKillingSpree"],
                "total_time_cc_dealt": p["totalTimeCCDealt"],
                "total_time_spent_dead": p["totalTimeSpentDead"],
                # Summoner spells
                "summoner1_id": p["summoner1Id"],
                "summoner2_id": p["summoner2Id"],
                # Items
                "item0": p["item0"],
                "item1": p["item1"],
                "item2": p["item2"],
                "item3": p["item3"],
                "item4": p["item4"],
                "item5": p["item5"],
                "item6": p["item6"],
                # Perks/Runes
                "perk_keystone": primary_sel[0].get("perk", 0)
                if len(primary_sel) > 0
                else 0,
                "perk_primary_row_1": primary_sel[1].get("perk", 0)
                if len(primary_sel) > 1
                else 0,
                "perk_primary_row_2": primary_sel[2].get("perk", 0)
                if len(primary_sel) > 2
                else 0,
                "perk_primary_row_3": primary_sel[3].get("perk", 0)
                if len(primary_sel) > 3
                else 0,
                "perk_secondary_row_1": secondary_sel[0].get("perk", 0)
                if len(secondary_sel) > 0
                else 0,
                "perk_secondary_row_2": secondary_sel[1].get("perk", 0)
                if len(secondary_sel) > 1
                else 0,
                "perk_primary_style": primary.get("style", 0),
                "perk_secondary_style": secondary.get("style", 0),
                "perk_shard_defense": stat_perks.get("defense", 0),
                "perk_shard_flex": stat_perks.get("flex", 0),
                "perk_shard_offense": stat_perks.get("offense", 0),
                "ingest_ts": datetime.now().isoformat(),
            }
        )

    return rows


def process_teams(match_json: dict) -> list[dict]:
    """Extract team-level data including bans."""
    side_dict = {100: "blue", 200: "red"}
    rows = []

    info = match_json["info"]
    match_id = match_json["metadata"]["matchId"]

    for team in info["teams"]:
        team_id = team["teamId"]
        obj = team.get("objectives", {})

        # Extract ban champion IDs
        bans = [b.get("championId", 0) for b in team.get("bans", [])]

        rows.append(
            {
                "match_id": match_id,
                "team_id": team_id,
                "side": side_dict.get(team_id, "unknown"),
                "win": team["win"],
                # Objectives
                "baron_kills": obj.get("baron", {}).get("kills", 0),
                "dragon_kills": obj.get("dragon", {}).get("kills", 0),
                "rift_herald_kills": obj.get("riftHerald", {}).get("kills", 0),
                "tower_kills": obj.get("tower", {}).get("kills", 0),
                "inhibitor_kills": obj.get("inhibitor", {}).get("kills", 0),
                "champion_kills": obj.get("champion", {}).get("kills", 0),
                # First objectives
                "first_baron": obj.get("baron", {}).get("first", False),
                "first_dragon": obj.get("dragon", {}).get("first", False),
                "first_rift_herald": obj.get("riftHerald", {}).get("first", False),
                "first_tower": obj.get("tower", {}).get("first", False),
                "first_inhibitor": obj.get("inhibitor", {}).get("first", False),
                "first_blood": obj.get("champion", {}).get("first", False),
                # Bans
                "ban_1": bans[0] if len(bans) > 0 else 0,
                "ban_2": bans[1] if len(bans) > 1 else 0,
                "ban_3": bans[2] if len(bans) > 2 else 0,
                "ban_4": bans[3] if len(bans) > 3 else 0,
                "ban_5": bans[4] if len(bans) > 4 else 0,
                # Metadata
                "game_version": info["gameVersion"],
                "queue_id": info["queueId"],
                "ingest_ts": datetime.now().isoformat(),
            }
        )

    return rows


def main():
    # Load match IDs
    output_path = BRONZE_DIR / "match_ids.parquet"
    if not output_path.exists():
        print("Run 02_matchids.py first!")
        return

    match_ids_df = pd.read_parquet(output_path)
    all_match_ids = match_ids_df["match_id"].unique().tolist()
    print(f" {len(all_match_ids)} match IDs ")

    match_ids_to_fetch = all_match_ids[:200]
    print(f" Fetching {len(match_ids_to_fetch)} matches")

    all_participants = []
    all_teams = []

    for match_id in tqdm(match_ids_to_fetch, desc="Fetching matches"):
        try:
            match_json = get_match_detail(match_id)
            if match_json:
                all_participants.extend(process_participants(match_json))
                all_teams.extend(process_teams(match_json))
            time.sleep(0.05)
        except Exception as e:
            print(f"\n Error {match_id}: {e}")
            continue

    # Save participants
    if all_participants:
        df_participants = pd.DataFrame(all_participants)
        participants_path = BRONZE_DIR / "matches_participants.parquet"
        df_participants.to_parquet(participants_path, index=False)
        print(f"\n Participants: {len(df_participants)} rows -> {participants_path}")

    # Save teams
    if all_teams:
        df_teams = pd.DataFrame(all_teams)
        teams_path = BRONZE_DIR / "matches_teams.parquet"
        df_teams.to_parquet(teams_path, index=False)
        print(f" Teams: {len(df_teams)} rows -> {teams_path}")

    print("Done")

if __name__ == "__main__":
    main()
