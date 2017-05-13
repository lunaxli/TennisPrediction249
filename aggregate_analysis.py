import os
import sys
import glob
import csv


# global averages of all stats
def analyze_global():
    
    # find input csv files and output files
    cwd = os.getcwd()
    path = cwd + "/atp_matches_features.csv"
    output = cwd + "/agg_analysis.csv"

    seed_tot = [[], []]; ht_tot = [[], []]; age_tot = [[], []]
    rank_tot = [[], []]; rank_pts_tot = [[], []]; ace_tot = [[], []]
    df_tot = [[], []]; svpt_tot = [[], []]; firstIn_tot = [[], []]
    firstWon_tot = [[], []]; secWon_tot = [[], []]; SvGms_tot = [[], []]
    bpSaved_tot = [[], []]; bpFaced_tot = [[], []]; minutes_tot = []
    surface_tot = dict()
    
    # read all csv files
    with open(output, 'w') as f_out:
        for filename in glob.glob(path):
            with open(filename, 'r') as f_in:
                # read input as csv dictionary
                reader = csv.DictReader(f_in)
                for row in reader:
                    res = int(row['result'])
                    if float(row['seed']):
                        seed_tot[res] += [float(row['seed'])]
                    if float(row['ht']):
                        ht_tot[res] += [float(row['ht'])]
                    if float(row['age']):
                        age_tot[res] += [float(row['age'])]
                    if float(row['rank']):
                        rank_tot[res] += [float(row['rank'])]
                    if float(row['rank_pts']):
                        rank_pts_tot[res] += [float(row['rank_pts'])]
                    if float(row['ace']):
                        ace_tot[res] += [float(row['ace'])]
                    if float(row['df']):
                        df_tot[res] += [float(row['df'])]
                    if float(row['svpt']):
                        svpt_tot[res] += [float(row['svpt'])]
                    if float(row['1stIn']):
                        firstIn_tot[res] += [float(row['1stIn'])]
                    if float(row['1stWon']):
                        firstWon_tot[res] += [float(row['1stWon'])]
                    if float(row['2ndWon']):
                        secWon_tot[res] += [float(row['2ndWon'])]
                    if float(row['SvGms']):
                        SvGms_tot[res] += [float(row['SvGms'])]
                    if float(row['bpSaved']):
                        bpSaved_tot[res] += [float(row['bpSaved'])]
                    if float(row['bpFaced']):
                        bpFaced_tot[res] += [float(row['bpFaced'])]
                    if float(row['minutes']):
                        minutes_tot += [float(row['minutes'])]

                    if row['surface'] in surface_tot:
                        surface_tot[row['surface']] += 1
                    else:
                        surface_tot[row['surface']] = 1

                avg_w_seed = sum(seed_tot[1]) / float(len(seed_tot[1]))
                avg_l_seed = sum(seed_tot[0]) / float(len(seed_tot[0]))
                avg_w_ht = sum(ht_tot[1]) / float(len(ht_tot[1]))
                avg_l_ht = sum(ht_tot[0]) / float(len(ht_tot[0]))
                avg_w_age = sum(age_tot[1]) / float(len(age_tot[1]))
                avg_l_age = sum(age_tot[0]) / float(len(age_tot[0]))
                avg_w_rank = sum(rank_tot[1]) / float(len(rank_tot[1]))
                avg_l_rank = sum(rank_tot[0]) / float(len(rank_tot[0]))
                avg_w_rank_pts = sum(rank_pts_tot[1]) / float(len(rank_pts_tot[1]))
                avg_l_rank_pts = sum(rank_pts_tot[0]) / float(len(rank_pts_tot[0]))
                avg_w_ace = sum(ace_tot[1]) / float(len(ace_tot[1]))
                avg_l_ace = sum(ace_tot[0]) / float(len(ace_tot[0]))
                avg_w_df = sum(df_tot[1]) / float(len(df_tot[1]))
                avg_l_df = sum(df_tot[0]) / float(len(df_tot[0]))
                avg_w_svpt = sum(svpt_tot[1]) / float(len(svpt_tot[1]))
                avg_l_svpt = sum(svpt_tot[0]) / float(len(svpt_tot[0]))
                avg_w_1stIn = sum(firstIn_tot[1]) / float(len(firstIn_tot[1]))
                avg_l_1stIn = sum(firstIn_tot[0]) / float(len(firstIn_tot[0]))
                avg_w_1stWon = sum(firstWon_tot[1]) / float(len(firstWon_tot[1]))
                avg_l_1stWon = sum(firstWon_tot[0]) / float(len(firstWon_tot[0]))
                avg_w_2ndWon = sum(secWon_tot[1]) / float(len(secWon_tot[1]))
                avg_l_2ndWon = sum(secWon_tot[0]) / float(len(secWon_tot[0]))
                avg_w_SvGms = sum(SvGms_tot[1]) / float(len(SvGms_tot[1]))
                avg_l_SvGms = sum(SvGms_tot[0]) / float(len(SvGms_tot[0]))
                avg_w_bpSaved = sum(bpSaved_tot[1]) / float(len(bpSaved_tot[1]))
                avg_l_bpSaved = sum(bpSaved_tot[0]) / float(len(bpSaved_tot[0]))
                avg_w_bpFaced = sum(bpFaced_tot[1]) / float(len(bpFaced_tot[1]))
                avg_l_bpFaced = sum(bpFaced_tot[0]) / float(len(bpFaced_tot[0]))
                avg_minutes = sum(minutes_tot) / float(len(minutes_tot))

                print "Avg Winning Seed:", avg_w_seed
                print "Avg Losing Seed:", avg_l_seed
                print "Avg Winning Height:", avg_w_ht
                print "Avg Losing Height:", avg_l_ht
                print "Avg Winning Age:", avg_w_age
                print "Avg Losing Age:", avg_l_age
                print "Avg Winning Rank:", avg_w_rank
                print "Avg Losing Rank:", avg_l_rank
                print "Avg Winning Rank Pts:", avg_w_rank_pts
                print "Avg Losing Rank Pts:", avg_l_rank_pts
                print "Avg Winning Aces:", avg_w_ace
                print "Avg Losing Aces:", avg_l_ace
                print "Avg Winning DF:", avg_w_df
                print "Avg Losing DF:", avg_l_df
                print "Avg Winning SvPts:", avg_w_svpt
                print "Avg Losing SvPts:", avg_l_svpt
                print "Avg Winning 1stIn:", avg_w_1stIn
                print "Avg Losing 1stIn:", avg_l_1stIn
                print "Avg Winning 1stWon:", avg_w_1stWon
                print "Avg Losing 1stWon:", avg_l_1stWon
                print "Avg Winning 2ndWon:", avg_w_2ndWon
                print "Avg Losing 2ndWon:", avg_l_2ndWon
                print "Avg Winning SvGms:", avg_w_SvGms
                print "Avg Losing SvGms:", avg_l_SvGms
                print "Avg Winning bpSaved:", avg_w_bpSaved
                print "Avg Losing bpSaved:", avg_l_bpSaved
                print "Avg Winning bpFaced:", avg_w_bpFaced
                print "Avg Losing bpFaced:", avg_l_bpFaced
                print "Avg Minutes:", avg_minutes
                print "Surface Distribution:"
                print surface_tot

if __name__ == "__main__":
    analyze_global()

