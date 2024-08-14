import os
from datetime import datetime
import pandas as pd
import csv
import requests
from requests.exceptions import RequestException
from urllib3.exceptions import NewConnectionError, ConnectTimeoutError
from time import sleep

pd.options.mode.chained_assignment = None
pd.set_option('display.max_rows', None)
pd.reset_option('display.max_columns')

from include.global_variables import global_variables as gv


# --------------- #
# FUNCTION 1
# --------------- #

def apply_filtering_logic(df):
    """
    Function to go iterate through a dataframe of league games, checking for each game whether either team
    in the fixture have 
    (1) have had more than 2 common opponents in their last 5 games and
    (2) have won more than 3 matches against identified common opponents
    """

    def filter_fixtures_today(df):
                """
                Function to return only fixtures playing today. 
                For logging purposes. Not part of the core logic
                """
                # Get the current date in UTC format
                current_date_utc = pd.to_datetime(datetime.utcnow())

    
                df['date'] = pd.to_datetime(
                    df['date']).dt.date
                df['date'] = pd.to_datetime(
                    df['date']).dt.date


                df.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_all_days_across_71_leagues.csv"))
                gv.task_log.info("Saved all fixtures successfully \n")
                
                # Filter DataFrames based on the current date
                filtered_df_today = df[df['date'] == current_date_utc.date(
                )]
                filtered_df_today = filtered_df_today.drop_duplicates()
                
                filtered_df_today.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_today_across_71_leagues.csv"))
                gv.task_log.info("Save successful. Showing today's fixtures. \n")

                gv.task_log.info(filtered_df_today.drop(columns=["season", "country", "date"], errors='ignore'))


    df = df.drop_duplicates()
    df = df[df['season'] == gv.CURRENT_SEASON]

    df['date'] = pd.to_datetime(df['date'], utc=True)

    df['home_team_score'] = pd.to_numeric(
        df['home_team_score'], errors='coerce').astype('Int64')
    
    df['away_team_score'] = pd.to_numeric(
        df['away_team_score'], errors='coerce').astype('Int64')

    gv.task_log.info("Import successful for all files.\n")
    gv.task_log.info(df.info())
    gv.task_log.info("\n")

    filter_fixtures_today(df)

    gv.task_log.info("\n")


    if df is not None:
        try:
            gv.task_log.info("Applying filtering logic...  This would take some time.\n")
            df1 = df[df["match_status"] == "Match Finished"]

            # Initialize lists to store data
            dates_both_conditions = []
            home_teams_both_conditions = []
            home_scores_both_conditions = []
            away_scores_both_conditions = []
            away_teams_both_conditions = []

            def team_won_3_games_or_more(team, g):
                """
                    Core logic of the program:
                    Function to check for the counts of wins of both teams in the current fixture,
                    against teams in their last 5 games
                """
                win_count = 0
                for index, row in g.iterrows():
                    if row["home_team"] != team:
                        # Swap home and away team names and scores
                        g.loc[index, "home_team"] = row["away_team"]
                        g.loc[index, "away_team"] = row["home_team"]

                        g.loc[index, "home_team_score"] = row["away_team_score"]
                        g.loc[index, "away_team_score"] = row["home_team_score"]

                win_count = 0
                for index, row in g.iterrows():
                    if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                        win_count += 1
                if win_count >= 3:
                    return True

            # Find all match containers
            match_containers = df.to_dict('records')

            # Loop through each match container
            for match in match_containers:
                home_team = match['home_team']
                away_team = match['away_team']


                # Get the last 5 fixtures for both home and away teams
                last_5_home_team_fixtures = df1[(df1['home_team'] == home_team) | (
                    df1['away_team'] == home_team)].sort_values(by='date', ascending=False).head(5)


                last_5_away_team_fixtures = df1[(df1['home_team'] == away_team) | (
                    df1['away_team'] == away_team)].sort_values(by='date', ascending=False).head(5)

               # Get all opponents for either teams, including both home and away teams
                last_5_home_team_opps = pd.unique(
                    last_5_home_team_fixtures[["home_team", "away_team"]].stack())
 
                last_5_away_team_opps = pd.unique(
                    last_5_away_team_fixtures[["home_team", "away_team"]].stack())

                # Convert NumPy arrays to Python sets
                last_5_home_team_opps_set = set(last_5_home_team_opps)
                last_5_away_team_opps_set = set(last_5_away_team_opps)

                # Find common opponents
                common_opponents = last_5_home_team_opps_set.intersection(
                    last_5_away_team_opps_set)

                # Assuming home_team and away_team are variables that represent the current teams
                current_teams = set([home_team, away_team])
                common_opponents = common_opponents.difference(current_teams)

                # common opponents - home team
                condition_1 = last_5_home_team_fixtures['home_team'].isin(
                    common_opponents) | last_5_home_team_fixtures['away_team'].isin(common_opponents)
                g = last_5_home_team_fixtures[condition_1]

                # common opponents - away team
                condition_2 = last_5_away_team_fixtures['home_team'].isin(
                    common_opponents) | last_5_away_team_fixtures['away_team'].isin(common_opponents)
                h = last_5_away_team_fixtures[condition_2]

                # Check if count of common opponents fixtures is greater than 2
                if len(common_opponents) > 2:
                    if team_won_3_games_or_more(home_team, g) or team_won_3_games_or_more(away_team, h):

                        # Append data to lists for both conditions
                        dates_both_conditions.append(match['date'])
                        home_teams_both_conditions.append(home_team)
                        home_scores_both_conditions.append(match['home_team_score'])
                        away_scores_both_conditions.append(match['away_team_score'])
                        away_teams_both_conditions.append(away_team)

            # Create DataFrames for both conditions
            data_both_conditions = {'Date': dates_both_conditions, 'HomeTeam': home_teams_both_conditions,
                                    'HomeScore': home_scores_both_conditions, 'AwayScore': away_scores_both_conditions, 'AwayTeam': away_teams_both_conditions}
            result_df_both_conditions = pd.DataFrame(data_both_conditions)


            # Define file paths for both conditions
            file_path_both_conditions = os.path.join(
                gv.RESULTS_DATA_PATH, "result_for_all_days_both_condition.csv")

            result_df_both_conditions.to_csv(file_path_both_conditions, index=False)

            gv.task_log.info(
                f"DataFrame satisfying both conditions saved to: {file_path_both_conditions}")
           
            result_df_both_conditions['HomeScore'] = pd.to_numeric(
                result_df_both_conditions['HomeScore'], errors='coerce').astype('Int64')
            result_df_both_conditions['AwayScore'] = pd.to_numeric(
                result_df_both_conditions['AwayScore'], errors='coerce').astype('Int64')


            def filter_fixtures_today_and_2_days_onwards(result_df_both_conditions):
                    """
                    Function to filter for only fixtures playing today and up to the next 2 days
                    """

                    current_date_utc = pd.to_datetime(datetime.utcnow())

                    result_df_both_conditions['Date'] = pd.to_datetime(
                        result_df_both_conditions['Date']).dt.date

                    date_range = pd.date_range(current_date_utc, periods=3).date
                    # date_range = pd.date_range(current_date_utc, end='2024-03-31').date

                    filtered_df_both_conditions = result_df_both_conditions[
                        result_df_both_conditions['Date'].isin(date_range)]


                    return filtered_df_both_conditions

            filtered_both_conditions = filter_fixtures_today_and_2_days_onwards(
                result_df_both_conditions)
            
            gv.task_log.info("\nFiltered DataFrame satisfying both conditions for today's, tomorrow's and next tomorrow's fixtures:")
            gv.task_log.info(filtered_both_conditions)


        except Exception as e:
            gv.task_log.warning(f"Error during processing: {e}")

    return filtered_both_conditions



# --------------- #
# FUNCTION 2 
# --------------- #


def won_last_5_matches(df):
    """
    Variation on previous function. Now checking for only fixtures where either teams in both fixtures
    won their last 5 league games.
    """

    def filter_fixtures_today(df):
                current_date_utc = pd.to_datetime(datetime.utcnow())

                df['date'] = pd.to_datetime(
                    df['date']).dt.date
                df['date'] = pd.to_datetime(
                    df['date']).dt.date

                df.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_all_days_across_71_leagues.csv"))
                gv.task_log.info("Saved all fixtures successfully \n")

                filtered_df_today = df[df['date'] == current_date_utc.date()]
                filtered_df_today = filtered_df_today.drop_duplicates()
                
                filtered_df_today.to_csv(os.path.join(gv.RESULTS_DATA_PATH, "all_fixtures_today_across_71_leagues.csv"))
                gv.task_log.info("Save successful. Showing today's fixtures. \n")

                gv.task_log.info(filtered_df_today.drop(columns=["season", "country", "date"], errors='ignore'))


    df = df.drop_duplicates()
    df = df[df['season'] == gv.CURRENT_SEASON]
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df['home_team_score'] = pd.to_numeric(
        df['home_team_score'], errors='coerce').astype('Int64')
    df['away_team_score'] = pd.to_numeric(
        df['away_team_score'], errors='coerce').astype('Int64')

    gv.task_log.info("Import successful for all files.\n")
    gv.task_log.info(df.info())
    gv.task_log.info("\n")

    filter_fixtures_today(df)

    gv.task_log.info("\n")

    if df is not None:
        try:
            gv.task_log.info("Applying filtering logic...  This would take some time.\n")
            df1 = df[df["match_status"] == "Match Finished"]

            dates_both_conditions = []
            home_teams_both_conditions = []
            home_scores_both_conditions = []
            away_scores_both_conditions = []
            away_teams_both_conditions = []


            def team_won_all_last_5_games(team, g):
                win_count = 0
                for index, row in g.iterrows():
                    if row["home_team"] != team:
                        g.loc[index, "home_team"] = row["away_team"]
                        g.loc[index, "away_team"] = row["home_team"]

                        g.loc[index, "home_team_score"] = row["away_team_score"]
                        g.loc[index, "away_team_score"] = row["home_team_score"]

                
                win_count = 0
                for index, row in g.iterrows():
                    if g.loc[index, "home_team_score"] > g.loc[index, "away_team_score"]:
                        win_count += 1
                if win_count == 5:
                    return True

            match_containers = df.to_dict('records')

            for match in match_containers:
                home_team = match['home_team']
                away_team = match['away_team']

                last_5_home_team_fixtures = df1[(df1['home_team'] == home_team) | (
                    df1['away_team'] == home_team)].sort_values(by='date', ascending=False).head(5)

                last_5_away_team_fixtures = df1[(df1['home_team'] == away_team) | (
                    df1['away_team'] == away_team)].sort_values(by='date', ascending=False).head(5)

                if team_won_all_last_5_games(home_team, last_5_home_team_fixtures) or team_won_all_last_5_games(away_team, last_5_away_team_fixtures):


                    dates_both_conditions.append(match['date'])
                    home_teams_both_conditions.append(home_team)
                    home_scores_both_conditions.append(match['home_team_score'])
                    away_scores_both_conditions.append(match['away_team_score'])
                    away_teams_both_conditions.append(away_team)

            dataframe_dict = {'Date': dates_both_conditions, 'HomeTeam': home_teams_both_conditions,
                                    'HomeScore': home_scores_both_conditions, 'AwayScore': away_scores_both_conditions, 'AwayTeam': away_teams_both_conditions}
            result_df_both_conditions = pd.DataFrame(dataframe_dict)



            file_path_both_conditions = os.path.join(
                gv.RESULTS_DATA_PATH, "result_for_all_days_c2.csv")

            result_df_both_conditions.to_csv(file_path_both_conditions, index=False)

            gv.task_log.info(
                f"DataFrame satisfying both conditions saved to: {file_path_both_conditions}")

            result_df_both_conditions['HomeScore'] = pd.to_numeric(
                result_df_both_conditions['HomeScore'], errors='coerce').astype('Int64')
            result_df_both_conditions['AwayScore'] = pd.to_numeric(
                result_df_both_conditions['AwayScore'], errors='coerce').astype('Int64')

            def filter_fixtures_today_and_2_days_onwards(result_df_both_conditions):
                    current_date_utc = pd.to_datetime(datetime.utcnow())

                    result_df_both_conditions['Date'] = pd.to_datetime(
                        result_df_both_conditions['Date']).dt.date

                    
                    date_range = pd.date_range(current_date_utc, periods=3).date
                    # date_range = pd.date_range(current_date_utc, end='2024-03-31').date

                    filtered_df_both_conditions = result_df_both_conditions[
                        result_df_both_conditions['Date'].isin(date_range)]


                    return filtered_df_both_conditions

            filtered_both_conditions = filter_fixtures_today_and_2_days_onwards(
                result_df_both_conditions)

            gv.task_log.info("\nFiltered DataFrame satisfying both conditions for today's, tomorrow's and next tomorrow's fixtures:")
            gv.task_log.info(filtered_both_conditions)


        except Exception as e:
            gv.task_log.warning(f"Error during processing: {e}")

    return filtered_both_conditions

# --------------- #
# FUNCTION 1 - API FETCHING
# --------------- #

unique_league_ids = set(gv.LEAGUE_IDS)

url = gv.API_ENDPOINT
headers = {
    "X-RapidAPI-Key": gv.API_KEY,
    "X-RapidAPI-Host": gv.API_HOST
}

total_calls = len(unique_league_ids)
current_call = 0

def fetch_data(chosen_season=str(gv.CURRENT_SEASON)):
    global current_call
    for league_id in unique_league_ids:
        try:
            querystring = {"league": str(league_id), "season": chosen_season}
            response = requests.get(url, headers=headers, params=querystring)

            response.raise_for_status()

            data = response.json()['response']

            csv_data = []
            for fixture in data:
                fixture_data = {
                    'date': fixture['fixture']['date'],
                    'season': fixture['league']['season'],
                    'league_name': fixture['league']['name'],
                    'country': fixture['league']['country'],
                    'home_team': fixture['teams']['home']['name'],
                    'home_team_score': fixture['goals']['home'],
                    'away_team_score': fixture['goals']['away'],
                    'away_team': fixture['teams']['away']['name'],
                    'match_status': fixture['fixture']['status']['long']
                }
                csv_data.append(fixture_data)

            folder_name = os.path.join(gv.FIXTURES_DATA_FOLDER, f"{csv_data[0]['league_name']}_{csv_data[0]['country']}")
            os.makedirs(folder_name, exist_ok=True)

            csv_file_name = f"{csv_data[0]['league_name']}_{csv_data[0]['season']}.csv"
            csv_file_path = os.path.join(folder_name, csv_file_name)

            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
                fieldnames = ['date', 'season', 'league_name', 'country', 'home_team',
                            'home_team_score', 'away_team_score', 'away_team', 'match_status']
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                writer.writeheader()

                writer.writerows(csv_data)

            current_call += 1
            gv.task_log.info(f"{csv_data[0]['country']} {csv_data[0]['league_name']} called: ({current_call}/{total_calls})")
            gv.task_log.info(f"\nCSV file saved at: {csv_file_path}")


        except (NewConnectionError, ConnectTimeoutError) as e:
            gv.task_log.warning(f"Connection error in API call for league {league_id}: {e}")
            gv.task_log.warning("Ensure your internet connection is stable. Exiting the program.")

        except RequestException as e:
            gv.task_log.warning(f"Error in API call for league {league_id}: {e}")

        except Exception as e:
            gv.task_log.warning(f"Unexpected error in processing league {league_id}: {e}")

        # Sleep for 3 seconds before the next API call
        sleep(3)
