from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
import argparse
import sys
import constants


conf = SparkConf()
conf.setAppName('NBA analysis')
conf.set("spark.eventlog.enabled", True)

parser = argparse.ArgumentParser()

parser.add_argument("operation", help="the operation that needs to be invoked", choices=["populate", "2_point_shooters", "3_point_shooters","attackers","defenders","rebounders","plus_minus"])
parser.add_argument("-ip", "--master-ip", help="ip address of the driver/master, could be a name resolvable with DNS")
parser.add_argument("-dist", "--distributed", action="store_true", help="switch to cluster mode")
parser.add_argument("-c", "--college", action="store_true", help="switch to college analysis for category 'operation'")


args = parser.parse_args()

constants.setRedisConnectionAddress(args.master_ip)
conf.set('redis_connection', args.master_ip)

sc = SparkContext.getOrCreate(conf)


import util
import scoring

#TODO: ottimizzare l'inserimento su mongo tenendo conto della posizione del cursore
if args.distributed:
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/scoring.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/util.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/lxml.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/pymongo.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/redis.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/wget.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/bson.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/constants.py')

bonus = None

if args.operation == "populate":
	util.populate()

elif args.operation == "2_point_shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = constants.twop_percentage
	tresholds = constants.twop_tresholds

elif args.operation == "3_point_shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = constants.threep_percentage
	tresholds = constants.threep_tresholds

elif args.operation == "attackers":
	percentage = constants.att_percentage
	tresholds = constants.att_tresholds

elif args.operation == "defenders":
	percentage = constants.def_percentage
	tresholds = constants.def_tresholds
	bonus = [('personal_fouls', 0.2, -1)]

elif args.operation == "rebounders":
	percentage = constants.reb_percentage
	tresholds = constants.reb_tresholds

elif args.operation == "plus_minus":
	percentage = constants.pm_percentage
	tresholds = constants.pm_tresholds

if args.college:
	scoring.collegeAnalysis(percentage, tresholds, bonus = bonus)
else:
	scoring.analyze(percentage, tresholds, bonus = bonus, out=True)

sc.stop()
