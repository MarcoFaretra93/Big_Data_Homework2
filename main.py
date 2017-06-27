import time
start_time = time.time()
from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
import pymongo_spark
pymongo_spark.activate()
import argparse
import sys
import constants

conf = SparkConf()
conf.setAppName('NBA analysis')
#conf.set("spark.eventlog.enabled", True)

parser = argparse.ArgumentParser()

parser.add_argument("action", help="the action that needs to be invoked", choices=["populate", "2_point_shooters", "3_point_shooters","attackers","defenders","rebounders","plus_minus", "all_around"])
parser.add_argument("-ip", "--master-ip", help="ip address of the driver/master, could be a name resolvable with DNS")
parser.add_argument("-dist", "--distributed", action="store_true", help="switch to cluster mode")
parser.add_argument("-c", "--college", action="store_true", help="switch to college analysis for category 'action'")
parser.add_argument("-dp", "--data-provider", help="choose the data provider used during the worker parallelization. Redis is very slow", choices=["mongo", "redis"])
parser.add_argument("-l", "--limit", help="choose the number of record parallelized at once, reducing ram usage but increasing network usage. Used only if the 'data-provider' is redis. default alphabetical splitting", default=0, type=int)



args = parser.parse_args()

constants.setRedisConnectionAddress(args.master_ip)
conf.set('redis_connection', args.master_ip)
conf.set('provider', args.data_provider)
conf.set('limit', args.limit)
conf.set('mongo_host', args.master_ip)



sc = SparkContext.getOrCreate(conf)

import util
import scoring

if args.distributed:
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/scoring.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/util.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/lxml.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/pymongo.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/redis.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/wget.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/bson.zip')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/constants.py')
	sc.addPyFile('/home/hadoop/Big_Data_Homework2/mongo-hadoop/spark/src/main/python/pymongo_spark.py')

bonus = None

if args.action == "populate":
	util.populate()

elif args.action == "2_point_shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = constants.twop_percentage
	tresholds = constants.twop_tresholds

elif args.action == "3_point_shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = constants.threep_percentage
	tresholds = constants.threep_tresholds

elif args.action == "attackers":
	percentage = constants.att_percentage
	tresholds = constants.att_tresholds

elif args.action == "defenders":
	percentage = constants.def_percentage
	tresholds = constants.def_tresholds
	bonus = [('personal_fouls', 0.2, -1)]

elif args.action == "rebounders":
	percentage = constants.reb_percentage
	tresholds = constants.reb_tresholds

elif args.action == "plus_minus":
	percentage = constants.pm_percentage
	tresholds = constants.pm_tresholds

elif args.action == "all_around":
	percentage = constants.all_around_percentage
	tresholds = constants.all_around_tresholds

if args.college and args.action != 'populate':
	scoring.collegeAnalysis(percentage, tresholds, bonus = bonus)
elif args.action != 'populate':
	scoring.analyze(percentage, tresholds, bonus = bonus, out=True)

sc.stop()
print("--- %s seconds ---" % (time.time() - start_time))

