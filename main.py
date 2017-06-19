import pymongo
import redis
import numpy as np
import redis
from pyspark import SparkContext
import ast

from pyspark.mllib.stat import Statistics

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"
sc = SparkContext(appName="SummaryStatisticsExample")  # SparkContext
mongoClient = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
db = mongoClient['basketball_reference']
redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)

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

def insertIntoRedis(dictionary, op):
	for key in dictionary:
		dictionary[key] = map(lambda s : s.strip(), dictionary[key])
		redisClient.set(key + '.' + op, dictionary[key])
	redisClient.set("0000-0000", "three_field_goals_percentage,free_throws_attempted,2_field_goals_attempted,three_field_goals,field_goals_percentage,games_played,game_score,offensive_rebounds,three_field_goals_attempted,free_throws_percentage,blocks,effective_field_goals_percentage,2_field_goals,total_rebounds,2_field_goals_percentage,steals,turnovers,plus_minus,played_minutes,field_goals,free_throws,defensive_rebounds,points,personal_fouls,assists,field_goals_attempted")

""" op = mean or variance """
def getAndInsertAllStatsByType(op):
	mongoDict = mongoRead()
	insertIntoRedis(calculateStats(mongoDict, op),op)

""" values = [(field_name, value, operator)] """
def checkTreshold(season, op, values):
	valuesList = redisClient.get(season + '.' + op)
	header = redisClient.get('0000-0000').split(',')
	check = True
	valuesList = ast.literal_eval(valuesList)
	for element in values:
		field_name = element[0]
		value = element[1]
		""" da pulire, mettere 0 a monte dentro mongo """
		if(value == None or value == ""):
			value = "0"
		operator = element[2]
		try:
			modifier = element[3]
		except IndexError:
			modifier = "1"
		index = header.index(field_name)
		if eval(value + operator + valuesList[index] + '*' + modifier) == False:
			return  False
	return check

""" calcolare anche bonus """
def score4Shooters(percentage, tresholds, bonus = None):
	players = db.basketball_reference.find()
	for player in players:
		""" da rimuovere il try catch quando sistemiamo i dati sul DB """
		try:
			try:  
				season = min([int(x.split('-')[0]) for x in player['seasons'].keys()])
				season = str(season) + '-' + str(season + 1)
				for i in range(4):
					allParameters = player['seasons'][season]['all']
					if(checkTreshold(season, 'mean', [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>=')])):
						print "passed"
					season = str(int(season.split('-')[0])+1) + '-' + str(int(season.split('-')[1])+1)
			except KeyError:
				pass

		except ValueError: 
			pass


score4Shooters(None,None)
sc.stop()
