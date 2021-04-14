# NFL data dive

# Libraries
library(tidyverse)
library(nflfastR)
library(readxl)

# Build DB
update_db()

# Rosters
rosters_00_20 <- fast_scraper_roster(2000:2020)

# Salaries
setwd("./OverTheCap")

# Cleaning up data
## First, organize the contracts chronologically and by player
salary_files <- list.files(pattern = ".xlsx") %>%
  lapply(read_xlsx) %>%
  bind_rows() %>%
  mutate(Player_YearSigned = paste0(Player, " ", `Year Signed`)) %>%
  distinct(Player_YearSigned, .keep_all = T) %>%
  select(-Player_YearSigned) %>%
  arrange(Player, `Year Signed`)

## Next, number contracts and lag by player, so we see APY, guaranteed, length, total
## from prior contract

salary_files <- salary_files %>%
  group_by(Player) %>%
  mutate(number_contract = row_number()) %>%
  group_by(Player) %>%
  mutate(lag_years = lag(Years, 1),
         lag_value = lag(Value, 1),
         lag_apy = lag(APY, 1),
         lag_guaranteed = lag(Guaranteed, 1),
         lag_pctofcap = lag(`APY as % Of Cap At Signing`, 1))

## Cleaning up team names

### Multiple team contracts
salary_files <- salary_files %>%
  mutate(Team = str_remove(Team, "^(.*?/)"),
         Team = str_remove(Team, "^(.*?/)"),
         Team = str_trim(Team))

### City abbreviations
cities <- salary_files %>%
  ungroup() %>%
  distinct(Team) %>%
  arrange(Team)

#### Recode team abbreviations
salary_files$Team <- recode(salary_files$Team,
                            "49ers" = "SF",
                            "Bears" = "CHI",
                            "Bengals" = "CIN",
                            "Bills" = "BUF",
                            "Broncos" = "DEN",
                            "Browns" = "CLE",
                            "Buccaneers" = "TB",
                            "Cardinals" = "ARI",
                            "Chargers" = "LAC",
                            "Chiefs" = "KC",
                            "Colts" = "IND",
                            "Cowboys" = "DAL",
                            "Dolphins" = "MIA",
                            "Eagles" = "PHI",
                            "Falcons" = "ATL",
                            "Giants" = "NYG",
                            "Jaguars" = "JAX",
                            "Jets" = "NYJ",
                            "Lions" = "DET",
                            "Packers" = "GB",
                            "Panthers" = "CAR",
                            "Patriots" = "NE",
                            "Raiders" = "LV",
                            "Rams" = "LAR",
                            "Ravens" = "BAL",
                            "Saints" = "NO",
                            "Seahawks" = "SEA",
                            "Steelers" = "PIT",
                            "Texans" = "HOU",
                            "Titans" = "TEN",
                            "Vikings" = "MIN",
                            "Washington" = "WAS")
                            
## Matching them up

### Rename to match up
salary_files <- salary_files %>%
  rename(full_name = Player,
         team = Team,
         season = `Year Signed`) 
  #mutate(guide = paste0(full_name, " ", team, " ", season))

#rosters_00_20 <- rosters_00_20 %>%
 # mutate(guide = paste0(full_name, " ", team, " ", season))

## Merge
roster_salary <- merge(rosters_00_20, salary_files, by = c("full_name",
                                                           "team",
                                                           "season"),
                       all.x = T)

# Bring in play-by-play data
setwd("..")

update_db()

connection <- DBI::dbConnect(RSQLite::SQLite(), "./pbp_db")

pbp_db <- dplyr::tbl(connection, "nflfastR_pbp")

# rushing data cleaned
rushing_all <- pbp_db %>%
  filter(play_type == "run") %>%
  dplyr::select(rusher_id, posteam_type, posteam, defteam, yardline_100, half_seconds_remaining,
                game_half, down, ydstogo, shotgun, no_huddle, run_gap, run_location,
                score_differential, season, roof, yards_gained, wpa, epa)

rushing_all.df <- as.data.frame(rushing_all) %>%
  unite(col = "team_run_gap", c("posteam", "run_location", "run_gap"), remove = F) %>%
  unite(col = "def_season", c("defteam", "season"), remove = F) %>%
  mutate(team_run_gap = as.factor(team_run_gap),
         def_season = as.factor(def_season))

rosters_all <- fast_scraper_roster(seasons = c(1999:2020))%>%
  select(season, gsis_id, full_name, position, height, weight) %>%
  arrange(season) %>%
  distinct(gsis_id, .keep_all = T) %>%
  rename(rookie_year = season)

rushing_all.df <- decode_player_ids(rushing_all.df) %>%
  left_join(rosters_all, by = c("rusher_id" = "gsis_id")) 

# Non QB
rushing_all.rb <- rushing_all.df %>%
  filter(position == "RB",
         is.na(rusher_id) == F) %>%
  mutate(position = as.factor(position))

# Pass defense strength
defensive_strength_pass <- pbp_db %>%
  filter(play_type == "pass") %>%
  group_by(defteam, season) %>%
  summarize(def_strength_pass = -mean(epa))

defensive_strength <- as.data.frame(defensive_strength_pass) %>%
  unite(col = def_season, c("defteam", "season")) %>%
  left_join(defensive_strength_run, by = "def_season")

rushing_all.rb <- rushing_all.rb %>%
  left_join(defensive_strength, by = "def_season") %>%
  mutate(def_season = as.factor(def_season))

# Ranking by season
carries_by_season <- rushing_all.rb %>%
  group_by(rusher_id, season) %>%
  summarize(carries = n()) %>%
  group_by(season) %>%
  mutate(rank_carries = dense_rank(desc(carries)))

carries_by_season <- carries_by_season %>%
  unite(col= "rusher_season", c("rusher_id", "season"), remove = F)
