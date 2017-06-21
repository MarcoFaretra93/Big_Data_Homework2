import pymongo
import redis
import numpy as np
import redis
from pyspark import SparkContext
import ast
import sys

from pyspark.mllib.stat import Statistics

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"
sc = SparkContext(appName="SummaryStatisticsExample")  # SparkContext
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

""" values = [(field_name, operator, modifier)] """
def checkTreshold(season, op, values, player):
	redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)
	valuesList = redisClient.get(season + '.' + op)
	header = redisClient.get('0000-0000').split(',')
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

def getBonus(bonus, season, stats):
	redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)
	meanStats = redisClient.get(season + '.mean')
	header = redisClient.get('0000-0000').split(',')
	meanStats = ast.literal_eval(meanStats)
	bonus_name = bonus[0]
	bonus_value = bonus[1]
	modifier = bonus[2]
	index = header.index(bonus_name)
	if float(stats[bonus_name] == 0):
		return 0
	else:
		return bonus_value * (float(stats[bonus_name]) - float(meanStats[index])) * modifier

""" calcolare anche bonus """
def score4Shooters(player, percentage, tresholds, bonus = None):
	totalScore = 0
	count = 0
	try: 
		season = min([int(x.split('-')[0]) for x in player['seasons'].keys()])
		season = str(season) + '-' + str(season + 1)
		for i in range(4):
			annualScore = 0
			allParameters = player['seasons'][season]['all']
			if(checkTreshold(season, 'mean', tresholds, player)):
				for percentageKeys in percentage.keys():
					annualScore += float(allParameters[percentageKeys]) * float(percentage[percentageKeys])
					totalScore += float(allParameters[percentageKeys]) * float(percentage[percentageKeys])
			if bonus != None:
				for b in bonus:
					totalScore += annualScore * getBonus(b, season, allParameters)
			season = str(int(season.split('-')[0])+1) + '-' + str(int(season.split('-')[1])+1)
			count += 1
	except KeyError:
		pass
	finalScore = totalScore * 100
	return (player['player_id'], round(finalScore/count,3))



def analyzeShooters():
	players = db.basketball_reference.find()
	parallel_players = sc.parallelize([p for p in players])
	percentage = {'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}
	#tresholds = [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')]
	tresholds = [('2_field_goals_attempted', '>='),('played_minutes', '>=', '0.5'),('games_played', '>='),('three_field_goals_attempted', '>=')]
	scores = parallel_players.map(lambda player: score4Shooters(player, percentage, tresholds)).collect()
	for couple in scores:
		print str(couple[0]) + " : " + str(couple[1])


def analyzeAttacker():
	players = db.basketball_reference.find().limit(100)
	parallel_players = sc.parallelize([p for p in players])
	percentage = {'effective_field_goals_percentage' : 0.3, 'points' : 0.7}
	tresholds = [('field_goals_attempted', '>='),('played_minutes', '>=', '0.5')]
	bonus = [('turnovers', 0.2, -1)]
	scores = parallel_players.map(lambda player: score4Shooters(player, percentage, tresholds, bonus)).collect()
	for couple in scores:
		print str(couple[0]) + " : " + str(couple[1])

if sys.argv[1] == "populate":
	if sys.argv[2] == "mean":
		insertIntoRedis(calculateStats(mongoRead(),"mean"),"mean")
	elif sys.argv[2] == "variance":
		insertIntoRedis(calculateStats(mongoRead(),"variance"),"variance")
	else:
		print "error: need second argument"
elif sys.argv[1] == "shooters":
	""" percentage =  {2pointperc = 80%, free throws perc = 15%, 3pointperc = 5%} """
	#score4Shooters({'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}, [('2_field_goals_attempted', allParameters['2_field_goals_attempted'], '>='),('played_minutes', allParameters['played_minutes'], '>=', '0.5'),('games_played', allParameters['games_played'], '>='),('three_field_goals_attempted', allParameters['three_field_goals_attempted'], '>=')])
	analyzeShooters()
elif sys.argv[1] == "attacker":
	analyzeAttacker()


sc.stop()
