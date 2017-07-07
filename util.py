from __future__ import print_function
import pymongo
import redis
import numpy as np
import constants
import types
import ast
import sys
import csv
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics


sc = SparkContext.getOrCreate()

mongoClient = pymongo.MongoClient(constants.MONGO_CONNECTION)
db = mongoClient['basketball_reference']
redisClient = redis.StrictRedis(host=sc.getConf().get('redis_connection'), port=6379, db=1)

def isTuple(x): return type(x) == types.TupleType

def flatten(T):
	if not isTuple(T): return (T,)
	elif len(T) == 0: return ()
	else: 
		return flatten(T[0]) + flatten(T[1:]) 

def pretty_print(couples):
	map(lambda x: print('\t'.join([str(y) for y in flatten(x)])), couples)

""" funzione per normalizzare lo score per categorie """
def normalize_scores(max_value, scores):
	max_score = max([x for (y,x) in scores])
	min_score = min([x for (y,x) in scores])
	if min_score < 0:
		scores = map(lambda (x,y): (x,y - min_score), scores)
		max_score = max([x for (y,x) in scores])

	return map(lambda (x,y): (x,y*max_value/max_score), scores)

""" funzione per normalizzare lo score per college """
def normalize_scores_college(max_value, scores):
	max_score = max([x for (y,(x,z)) in scores])
	min_score = min([x for (y,(x,z)) in scores])
	if min_score < 0:
		scores = map(lambda (x,(y,z)): (x,(y - min_score,z)), scores)
		max_score = max([x for (y,(x,z)) in scores])

	return map(lambda (x,(y,z)): (x,(y*max_value/max_score, z)), scores)

""" funzione per normalizzare i valori di un singolo campo """
def normalize(parameter, key, season):
	field_names = redisClient.get('0000-0000').split(',')
	maxValue = ast.literal_eval(redisClient.get(season + '.max'))
	minValue = ast.literal_eval(redisClient.get(season + '.min'))
	index = field_names.index(key)
	return (float(parameter) - float(minValue[index]))/(float(maxValue[index]) - float(minValue[index]))

""" funzione che legge da mongo e torna un dizionario {stagione : all_statistica di quell'anno per tutti i giocatori} """
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

""" funzione che calcola media e varianza prendendo come input il risultato di mongoRead e l'op (mean o variance) """
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
		if op == 'max':
			maxValue = summary.max()
			valuesList = []
			for singleElement in maxValue:
				valuesList.append(str(singleElement).rstrip())
			result[year] = valuesList
		if op == 'min':
			minValue = summary.min()
			valuesList = []
			for singleElement in minValue:
				valuesList.append(str(singleElement).rstrip())
			result[year] = valuesList
	return result

""" funzione che prende il risultato di calculateStats e lo inserisce dentro redis """
def insertIntoRedis(dictionary, op):
	for key in dictionary:
		dictionary[key] = map(lambda s : s.strip(), dictionary[key])
		redisClient.set(key + '.' + op, dictionary[key])
	redisClient.set("0000-0000", "three_field_goals_percentage,free_throws_attempted,2_field_goals_attempted,three_field_goals,field_goals_percentage,games_played,game_score,offensive_rebounds,three_field_goals_attempted,free_throws_percentage,blocks,effective_field_goals_percentage,2_field_goals,total_rebounds,2_field_goals_percentage,steals,turnovers,plus_minus,played_minutes,field_goals,free_throws,defensive_rebounds,points,personal_fouls,assists,field_goals_attempted")

""" op = mean or variance """
def getAndInsertAllStatsByType(op):
	mongoDict = mongoRead()
	insertIntoRedis(calculateStats(mongoDict, op),op)

def populate():
	getAndInsertAllStatsByType('mean')
	getAndInsertAllStatsByType('variance')
	getAndInsertAllStatsByType('max')
	getAndInsertAllStatsByType('min')



 




