import pymongo
import redis
import numpy as np
import redis
from pyspark import SparkContext

from pyspark.mllib.stat import Statistics

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"
sc = SparkContext(appName="SummaryStatisticsExample")  # SparkContext


def mongoRead():
	years2stats = dict()
	mongoClient = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
	db = mongoClient['basketball_reference']
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
			result[year] = summary.mean()
		if op == 'variance':
			result[year] = summary.variance()
	return result

def insertIntoRedis(dictionary, op):
	redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)
	for key in dictionary:
		redisClient.set(key + '.' + op, dictionary[key])
	redisClient.set('0000-0000', '3fgp,fta,2fga,tfg,fgp,gp,score,ro,tfga,ftp,bl,efgp,2fg,tr,2fgp,st,tu,plus_min,pm,fg,ft,or,pt,pf,ass,fga')



mongoDict = mongoRead()
op = 'variance'
insertIntoRedis(calculateStats(mongoDict, op),op)
sc.stop()
