import os
import sys
import glob
import csv

def parser():
	
	# find input csv files and output files
	cwd = os.getcwd()
	path = cwd + "\\tennis_atp-master\\atp_matches_[1-2]*.csv"
	output = cwd + "\\atp_matches_features.csv"
	
	# read all csv files
	with open(output, 'w') as f_out:
		for filename in glob.glob(path):
				# write first row of output csv file
				f_out.write("%s, %s, %s, %s\n" % ("key", "age_diff", "rank_diff", "rank_point_diff"))
				with open(filename, 'r') as f_in:
					# read input as csv dictionary
					reader = csv.DictReader(f_in)
					for row in reader:
					
						# create key for each row of csv file
						winner_key = row['tourney_name'] + row['winner_id'] + row['loser_id']
						loser_key = row['tourney_name'] + row['loser_id'] + row['winner_id']
						
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
							winner_age_diff = 0
						else:
							winner_age_diff = int(float(row['winner_age'])) - int(float(row['loser_age']))
						
						# calculate rank and rank point difference
						winner_rank_diff = int(row['winner_rank']) - int(row['loser_rank'])
						winner_point_diff = int(row['winner_rank_points']) - int(row['loser_rank_points'])

						# write output
						f_out.write("%s, %d, %d, %d\n" % (winner_key, winner_age_diff, winner_rank_diff, winner_point_diff))
						f_out.write("%s, %d, %d, %d\n" % (loser_key, -winner_age_diff, -winner_rank_diff, -winner_point_diff))		
	
if __name__ == "__main__":
	parser()
