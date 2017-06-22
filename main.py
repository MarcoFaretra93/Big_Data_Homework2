import pymongo
import redis
import numpy as np
from pyspark import SparkContext
import sys
import util
import scoring
import bson

from pyspark.mllib.stat import Statistics

#TODO: ottimizzare l'inserimento su mongo tenendo conto della posizione del cursore

sc = SparkContext(appName="SummaryStatisticsExample")  # SparkContext
sc.addPyFile('/home/hadoop/Big_Data_Homework2/scoring.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/util.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/lxml.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/pymongo.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/redis.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/wget.py')

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"
mongoClient = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
db = mongoClient['basketball_reference']
redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)

""" legge da mongo e torna un dizionario {stagione : all_statistica di quell'anno per tutti i giocatori} """
def mongoRead():
	years2stats = dict()
	players = db.basketball_reference.find()
	for player in players:
		for year in player['seasons'].keys():
			try:
				years2stats[year].append(np.array([float(x) if x != None and x != "" else 0 for x in player['seasons'][year]['all'].values()]))
			except KeyError:
				years2stats[year] = []
				years2stats[year].append(np.array([float(x) if x != None and x != "" else 0 for x in player['seasons'][year]['all'].values()]))
	return years2stats

""" calcola media e varianza prendendo come input il risultato di mongoRead e l'op (mean o variance) """
def calculateStats(years2stats, op):
	result = dict()
	for year in years2stats:
		stats = sc.parallelize(years2stats[year])
		summary = Statistics.colStats(stats)
		if op == 'mean':
			means = summary.mean()
			valuesList = []
			for singleElement in means:
				valuesList.append(str(singleElement).rstrip())
			result[year] = valuesList
		if op == 'variance':
			variances = summary.variance()
			valuesList = []
			for singleElement in variances:
				valuesList.append(str(singleElement).rstrip())
			result[year] = valuesList
	return result

""" prende il risultato di calculateStats e lo inserisce dentro redis """
def insertIntoRedis(dictionary, op):
	for key in dictionary:
		dictionary[key] = map(lambda s : s.strip(), dictionary[key])
		redisClient.set(key + '.' + op, dictionary[key])
	redisClient.set("0000-0000", "three_field_goals_percentage,free_throws_attempted,2_field_goals_attempted,three_field_goals,field_goals_percentage,games_played,game_score,offensive_rebounds,three_field_goals_attempted,free_throws_percentage,blocks,effective_field_goals_percentage,2_field_goals,total_rebounds,2_field_goals_percentage,steals,turnovers,plus_minus,played_minutes,field_goals,free_throws,defensive_rebounds,points,personal_fouls,assists,field_goals_attempted")

""" op = mean or variance """
def getAndInsertAllStatsByType(op):
	mongoDict = mongoRead()
	insertIntoRedis(calculateStats(mongoDict, op),op)



if sys.argv[1] == "populate":
	if sys.argv[2] == "mean":
		#insertIntoRedis(calculateStats(mongoRead(),"mean"),"mean")
		getAndInsertAllStatsByType('mean')
	elif sys.argv[2] == "variance":
		getAndInsertAllStatsByType('variance')
		#insertIntoRedis(calculateStats(mongoRead(),"variance"),"variance")
	else:
		print "error: need second argument"

elif sys.argv[1] == "2-point-shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = {'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}
	tresholds = [('2_field_goals_attempted', '>='),('played_minutes', '>=', '0.5'),('games_played', '>='),('three_field_goals_attempted', '>=')]
	scoring.analyze(sc, percentage, tresholds)

elif sys.argv[1] == "3-point-shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	percentage = {'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.85}
	tresholds = [('2_field_goals_attempted', '>='),('played_minutes', '>=', '0.5'),('games_played', '>='),('three_field_goals_attempted', '>=')]
	scoring.analyze(sc, percentage, tresholds)

elif sys.argv[1] == "attackers":
	percentage = {'effective_field_goals_percentage' : 0.3, 'points' : 0.7}
	tresholds = [('field_goals_attempted', '>='),('played_minutes', '>=', '0.5')]
	scoring.analyze(sc, percentage, tresholds)

elif sys.argv[1] == "defenders":
	percentage = {'defensive_rebounds' : 0.5, 'steals' : 0.3, 'blocks' : 0.2}
	tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]
	bonus = [('personal_fouls', 0.2, -1)]
	scoring.analyze(sc, percentage, tresholds, bonus)

elif sys.argv[1] == "rebounders":
	percentage = {'total_rebounds' : 0.9, 'steals' : 0.1}
	tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]
	scoring.analyze(sc, percentage, tresholds)

elif sys.argv[1] == "plus_minus":
	percentage = {'plus_minus' : 1}
	tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]
	scoring.analyze(sc, percentage, tresholds)


sc.stop()
