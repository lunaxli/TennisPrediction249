import os
import sys
import glob
import csv

# "w" stands for winner and "l" stands for loser

def parser():
    
    # find input csv files and output files
    owd = os.getcwd()
    os.chdir("..")
    cwd = os.getcwd()
    path = cwd + "/tennis_atp/atp_matches_2[0-9]*.csv"
    output = owd + "/atp_matches_features.csv"
    
    # read all csv files
    with open(output, 'w') as f_out:
        for filename in glob.glob(path):
            print 'processing ', filename
            # write first row of output csv file
            f_out.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % ("key", "name", "o_name", "id", "o_id", "seed", "o_seed","ht", "o_ht", "age", "o_age", "rank", "o_rank", "rank_pts", "o_rank_pts", "ace", "o_ace", "df", "o_df", "svpt", "o_svpt", "1stIn", "o_1stIn", "1stWon", "o_1stWon", "2ndWon", "o_2ndWon", "SvGms", "o_SvGms", "bpSaved", "o_bpSaved", "bpFaced", "o_bpFaced", "age_diff", "rank_diff", "rank_point_diff", "date", "surface", "minutes"))
            with open(filename, 'r') as f_in:
                # read input as csv dictionary
                reader = csv.DictReader(f_in)
                for row in reader:
                  
                    # create key for each row of csv file
                    w_key = row['tourney_date'] + row['tourney_name'] + row['winner_id'] + row['loser_id']
                    l_key = row['tourney_date'] + row['tourney_name'] + row['loser_id'] + row['winner_id']
                        
                    # handle empty cells for rank and points
                    if not row['loser_rank'].strip():
                        row['loser_rank'] = 2500
                    if not row['winner_rank'].strip():
                        row['winner_rank'] = 2500
             
                    if not row['loser_rank_points'].strip():
                        row['loser_rank_points'] = 0
                    if not row['winner_rank_points'].strip():
                        row['winner_rank_points'] = 0
        
                    # handle empty cells for age
                    if not row['winner_age'].strip() or not row['loser_age'].strip():
                        w_age_diff = 0
                    else:
                        w_age_diff = int(float(row['winner_age'])) - int(float(row['loser_age']))

                    w_id = row['winner_id']; l_id = row['loser_id']
                    w_name = row['winner_name']; l_name = row['loser_name']
                    w_seed = row['winner_seed']; l_seed = row['loser_seed']
                    w_ht = row['winner_ht']; l_ht = row['loser_ht']
                    w_age = row['winner_age']; l_age = row['loser_age']
                    w_rank = row['winner_rank']; l_rank = row['loser_rank']
                    w_rank_pts = row['winner_rank_points']
                    l_rank_pts = row['loser_rank_points']
                    w_ace = row['w_ace']; l_ace = row['l_ace']
                    w_df = row['w_df']; l_df = row['l_df']
                    w_svpt = row['w_svpt']; l_svpt = row['l_svpt']
                    w_1stIn = row['w_1stIn']; l_1stIn = row['l_1stIn']
                    w_1stWon = row['w_1stWon']; l_1stWon = row['l_1stWon']
                    w_2ndWon = row['w_2ndWon']; l_2ndWon = row['l_2ndWon']
                    w_SvGms = row['w_SvGms']; l_SvGms = row['l_SvGms']
                    w_bpSaved = row['w_bpSaved']; l_bpSaved = row['l_bpSaved']
                    w_bpFaced = row['w_bpFaced']; l_bpFaced = row['l_bpFaced']
                    w_rank_diff = int(row['winner_rank']) - int(row['loser_rank'])
                    w_rank_point_diff = int(row['winner_rank_points']) - int(row['loser_rank_points'])
                    date = row['tourney_date']
                    surface = row['surface']
                    minutes = row['minutes']

            #("key", "w_name", "w_id", "l_name", "l_id", "w_seed", "l_seed","w_ht", "l_ht", "w_age", "l_age", "w_rank", "l_rank", "w_rank_pts", "l_rank_pts", "w_ace", "l_ace", "w_df", "l_df", "w_svpt", "l_svpt", "w_1stIn", "l_1stIn", "w_1stWon", "l_1stWon", "w_2ndWon", "l_2ndWon", "w_SvGms", "l_SvGms", "w_bpSaved", "l_bpSaved", "w_bpFaced", "l_bpFaced", "age_diff", "rank_diff", "rank_point_diff", "date", "surface", "minutes"))



                    # write output
                    f_out.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (w_key, w_name, l_name, w_id, l_id, w_seed, l_seed, w_ht, l_ht, w_age, l_age, w_rank, l_rank, w_rank_pts, l_rank_pts, w_ace, l_ace, w_df, l_df, w_svpt, l_svpt, w_1stIn, l_1stIn, w_1stWon, l_1stWon, w_2ndWon, l_2ndWon, w_SvGms, l_SvGms, w_bpSaved, l_bpSaved, w_bpFaced, l_bpFaced, w_age_diff, w_rank_diff, w_rank_point_diff, date, surface, minutes))
                    f_out.write("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n" % (l_key, l_name, w_name, l_id, w_id, l_seed, w_seed, l_ht, w_ht, l_age, w_age, l_rank, w_rank, l_rank_pts, w_rank_pts, l_ace, w_ace, l_df, w_df, l_svpt, w_svpt, l_1stIn, w_1stIn, l_1stWon, w_1stWon, l_2ndWon, w_2ndWon, l_SvGms, w_SvGms, l_bpSaved, w_bpSaved, l_bpFaced, w_bpFaced, -w_age_diff, -w_rank_diff, -w_rank_point_diff, date, surface, minutes))
    

if __name__ == "__main__":
    parser()

