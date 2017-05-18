import os
import sys
import glob
import csv


# global averages of all stats
def analyze_global():
    
    # find input csv files and output files
    cwd = os.getcwd()
    path = cwd + "/atp_matches_features.csv"
    output = cwd + "/player_aggregate_stats.csv"
    result = {}
    
    # read all csv files
    with open(output, 'w') as f_out:
        f_out.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % ("name", "avg_seed", "avg_ht", "avg_age", "avg_rank", "avg_rank_pts", "avg_aces", "avg_dfs", "avg_svps", "avg_1stIn", "avg_1stWon", "avg_2ndWon", "avg_SvGms", "avg_bpSaved", "avg_bpFaced", "matchesPlayed"))

        for filename in glob.glob(path):
            with open(filename, 'r') as f_in:
                # read input as csv dictionary
                reader = csv.DictReader(f_in)
                for row in reader:
                    # add player to dictionary
                    if result.get(row['name']) is None:
                        result[row['name']] = {'count': 1, 'seed': float(row['seed']), 'ht': float(row['ht']), 'age': float(row['age']), 'rank': float(row['rank']), 'rank_pts': float(row['rank_pts']), 'ace': float(row['ace']), 'df': float(row['df']), 'svpt': float(row['svpt']), '1stIn': float(row['1stIn']), '1stWon': float(row['1stWon']), '2ndWon': float(row['2ndWon']), 'SvGms': float(row['SvGms']), 'bpSaved': float(row['bpSaved']), 'bpFaced': float(row['bpFaced'])}
                    # sum up player stats
                    else:
                        result[row['name']]['count'] += 1
                        result[row['name']]['seed'] += float(row['seed'])
                        result[row['name']]['ht'] += float(row['ht'])
                        result[row['name']]['age'] += float(row['age'])
                        result[row['name']]['rank'] += float(row['rank'])
                        result[row['name']]['rank_pts'] += float(row['rank_pts'])
                        result[row['name']]['ace'] += float(row['ace'])
                        result[row['name']]['df'] += float(row['df'])
                        result[row['name']]['svpt'] += float(row['svpt'])
                        result[row['name']]['1stIn'] += float(row['1stIn'])
                        result[row['name']]['1stWon'] += float(row['1stWon'])
                        result[row['name']]['2ndWon'] += float(row['2ndWon'])
                        result[row['name']]['SvGms'] += float(row['SvGms'])
                        result[row['name']]['bpSaved'] += float(row['bpSaved'])
                        result[row['name']]['bpFaced'] += float(row['bpFaced'])

                
        for player in result:
            matchesPlayed = float(result[player]['count'])
            f_out.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (player, result[player]['seed']/matchesPlayed, result[player]['ht']/matchesPlayed, result[player]['age']/matchesPlayed, result[player]['rank']/matchesPlayed, result[player]['rank_pts']/matchesPlayed, result[player]['ace']/matchesPlayed, result[player]['df']/matchesPlayed, result[player]['svpt']/matchesPlayed, result[player]['1stIn']/matchesPlayed, result[player]['1stWon']/matchesPlayed, result[player]['2ndWon']/matchesPlayed, result[player]['SvGms']/matchesPlayed, result[player]['bpSaved']/matchesPlayed, result[player]['bpFaced']/matchesPlayed, matchesPlayed))



if __name__ == "__main__":
    analyze_global()

#("key", "name", "o_name", "id", "o_id", "seed", "o_seed","ht", "o_ht", "age", "o_age", "rank", "o_rank", "rank_pts", "o_rank_pts", "ace", "o_ace", "df", "o_df", "svpt", "o_svpt", "1stIn", "o_1stIn", "1stWon", "o_1stWon", "2ndWon", "o_2ndWon", "SvGms", "o_SvGms", "bpSaved", "o_bpSaved", "bpFaced", "o_bpFaced", "age_diff", "rank_diff", "rank_point_diff", "date", "surface", "minutes", "result", "hand", "o_hand", "score", "o_score", "round_of", "best_of", "draw_size", "tourney_level")
