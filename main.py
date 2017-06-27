import time
start_time = time.time()
from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics
import pymongo_spark
pymongo_spark.activate()
import argparse
import sys
import constants
import redis
import ast

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


""" values = [(field_name, operator, modifier)] """
def checkTreshold(season, op, values, player):
	#sc = SparkContext.getOrCreate()
	redisclient = redis.StrictRedis(host='localhost', port=6379, db=1)
	valuesList = redisclient.get(season + '.' + op)
	header = redisclient.get('0000-0000').split(',')
	check = True
	valuesList = ast.literal_eval(valuesList)
	for element in values:
		field_name = element[0]
		value = player['seasons'][season]['all'][field_name]
		operator = element[1]
		try:
			modifier = element[2]
		except IndexError:
			modifier = "1"
		index = header.index(field_name)
		if eval(value + operator + valuesList[index] + '*' + modifier) == False:
			return  False
	return check

""" scorefinale += scoreAnnuale * percent*(valore - mediaValore) """
def getBonus(bonus, season, stats):
	redisclient = redis.StrictRedis(host='localhost', port=6379, db=1)
	#sc = SparkContext.getOrCreate()
	meanStats = redisclient.get(season + '.mean')
	header = redisclient.get('0000-0000').split(',')
	meanStats = ast.literal_eval(meanStats)
	bonus_name = bonus[0]
	bonus_value = bonus[1]
	modifier = bonus[2]
	index = header.index(bonus_name)
	if float(stats[bonus_name] == 0):
		return 0
	else:
		return bonus_value * (float(stats[bonus_name]) - float(meanStats[index])) * modifier


def score4Player(player, percentage, tresholds, bonus = None, normalizer = False):
	redisClient = redis.StrictRedis(host="localhost", port=6379, db=1)
	totalScore = 0
	count = 0
	try: 
		season = min([int(x.split('-')[0]) for x in player['seasons'].keys()])
		season = str(season) + '-' + str(season + 1)
		for i in range(4):
			annualScore = 0
			allParameters = player['seasons'][season]['all']
			if(checkTreshold(season, 'mean', tresholds, player)):
				count += 1 
				for percentageKey in percentage.keys():
					if normalizer:
						normalizeValue = util.normalize(allParameters[percentageKey], percentageKey, season)
						annualScore += float(normalizeValue) * float(percentage[percentageKey])
						totalScore += float(normalizeValue) * float(percentage[percentageKey])
					else:
						annualScore += float(allParameters[percentageKey]) * float(percentage[percentageKey])
						totalScore += float(allParameters[percentageKey]) * float(percentage[percentageKey])
			if bonus != None:
				for b in bonus:
					totalScore += annualScore * getBonus(b, season, allParameters)
			season = str(int(season.split('-')[0])+1) + '-' + str(int(season.split('-')[1])+1)
	except KeyError:
		pass
	finalScore = totalScore * 100
	count = count if count != 0 else 1
	return (player['player_id'], finalScore/count)

"""Testare la configurazione con REDIS cbe fornisce i dati al posto di mongo, 208job ma alto livello di parallelizzazione"""
def analyze(sc, percentage, tresholds, out = False, bonus = None, normalizer = False):
	spark_context = sc #SparkContext.getOrCreate()
	parallel_players = []
	if spark_context.getConf().get("provider") == 'mongo':
		#players = db.basketball_reference.find()
		#parallel_players = spark_context.parallelize([p for p in players])
		parallel_players = spark_context.mongoRDD('mongodb://' + spark_context.getConf().get('mongo_host') + ':27017/basketball_reference.basketball_reference')
	if spark_context.getConf().get("provider") == 'redis':
		limit = spark_context.getConf().get('limit')
		parallel_players = splitRedisRecord(sc,limit, spark_context)
	#scorer = Scorer(percentage, tresholds, bonus, normalizer)
	#scores = scorer.startScoring(parallel_players)
	f = lambda player: score4Player(player, percentage, tresholds, bonus, normalizer)	
	scores = parallel_players.map(f)
	if out:
		util.pretty_print(util.normalize_scores(100,scores.collect()))
	else:
		return scores


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
	scoring.collegeAnalysis(sc, percentage, tresholds, bonus = bonus)
elif args.action != 'populate':
	analyze(sc, percentage, tresholds, bonus = bonus, out=True)

sc.stop()
print("--- %s seconds ---" % (time.time() - start_time))

