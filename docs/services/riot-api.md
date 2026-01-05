# Riot API (Data Source)

## Overview

The Riot Games API provides access to League of Legends match data, player statistics, and game metadata. This is the primary data source for the Bronze layer of the data lakehouse.

## Quick Reference

| Property | Value |
|----------|-------|
| **Base URL** | `https://developer.riotgames.com/` |
| **API Regions** | VN2  |
| **Rate Limits** | 20 requests/sec, 100 requests/2min |


## Getting API Key

### Step 1: Access Developer Portal

1. Go to [Riot Developer Portal](https://developer.riotgames.com/)
2. Click **Sign In** 
3. Login with your Riot account

![Riot Developer Portal](../../images/riot%20portal.png) 

### Step 2: Get Development API Key

1. After login, go to [Dashboard](https://developer.riotgames.com/)
2. Your **Development API Key** is displayed on the main page
3. Click **Regenerate API Key** if expired

![Riot API Key](../../images/riot%20api%20key.png)

Note: Development key expires every **24 hours**. 

### Step 3: Add to Environment

Add your API key to the `.env` file:

```bash
# .env
RIOT_API_KEY=RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

## API Endpoints

### Account & Summoner APIs

| Endpoint | Description | Example |
|----------|-------------|---------|
| `/lol/summoner/v4/summoners/by-name/{name}` | Get summoner by name | Get player info |
| `/lol/summoner/v4/summoners/by-puuid/{puuid}` | Get summoner by PUUID | Get player by unique ID |
| `/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}` | Get account by Riot ID | Get account info |


![Riot Account](../../images/account%20v1.png) 

![Riot Summoner](../../images/account%20v1.png)


### Match APIs

| Endpoint | Description | Example |
|----------|-------------|---------|
| `/lol/match/v5/matches/by-puuid/{puuid}/ids` | Get match IDs by PUUID | List of recent matches |
| `/lol/match/v5/matches/{matchId}` | Get match details | Full match data |
| `/lol/match/v5/matches/{matchId}/timeline` | Get match timeline | Minute-by-minute events | 

### League APIs
| Endpoint | Description | Example |
|----------|-------------|---------|
| `/lol/league/v4/entries/by-summoner/{summonerId}` | Get rank info | Player's rank |
| `/lol/league/v4/challengerleagues/by-queue/{queue}` | Get Challenger ladder | Top players |
| `/lol/league/v4/grandmasterleagues/by-queue/{queue}` | Get Grandmaster ladder | High-elo players |
| `/lol/league/v4/masterleagues/by-queue/{queue}` | Get Master ladder | Master tier players |


![Riot league v4](../../images/league%20v4.png)

![Riot match v5](../../images/match%20v5.png)
### Static Data (Data Dragon)

| Resource | URL | Description |
|----------|-----|-------------|
| Champions | `https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json` | Champion metadata |
| Items | `https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json` | Item metadata |
| Spells | `https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/summoner.json` | Summoner spells |
| Versions | `https://ddragon.leagueoflegends.com/api/versions.json` | Game versions |

## Regional Routing

### Platform Routing Values

| Vietnam | `vn2` | `vn2.api.riotgames.com` |


### Regional Routing Values (for Match-V5)
| SEA | VN | `sea.api.riotgames.com` |




## Implementation

### Prerequisites

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create `.env` file with Riot API key:**
   ```bash
   RIOT_API_KEY=RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   ```

3. **Run these scripts:**

```bash
cd data/extraction

# Step 1: Ladder
python 01_ladder.py --top 500

# Step 2: Match IDs  
python 02_matchids.py --count 20

# Step 3: Match Details
python 03_matches.py --limit 200

# Step 4: Upload to MinIO
python minio_upload.py
```


### Step 1: Fetch Ladder Data

**Command:**
```bash
cd data/extraction
python 01_ladder.py 
```

**Output File:** `data/bronze/ladder.parquet`


---

### Step 2: Fetch Match IDs

**Goal:** Lấy match IDs từ danh sách PUUIDs trong ladder.

**Command:**
```bash
python 02_matchids.py 
```

**Output File:** `data/bronze/match_ids.parquet`


---

### Step 3: Fetch Match Details

Tải chi tiết trận đấu và tách thành participants + teams.

**Command:**
```bash
python 03_matches.py --limit 200
```

**Output Files:**
- `data/bronze/matches_participants.parquet` 
- `data/bronze/matches_teams.parquet` 

---

### Step 4: Upload to MinIO 

Upload Bronze data lên MinIO storage.

**Command:**
```bash
python minio_upload.py
```



[← Back to Services](../README.md#service-guides)
