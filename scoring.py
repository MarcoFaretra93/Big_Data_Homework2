import util
import redis
import ast
import os.path
import csv
import redis
import string
import constants
import pymongo
from pyspark import SparkContext

LETTERS = list(string.ascii_lowercase)

sc = SparkContext.getOrCreate()

""" funzione che controlla se la soglia e' soddisfatta o meno, e ritorna rispettivamente true o false """
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

""" funzione che applica un bonus allo score """
""" scorefinale += scoreAnnuale * percent*(valore - mediaValore) """
def getBonus(bonus, season, stats):
	redisclient = redis.StrictRedis(host='localhost', port=6379, db=1)
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

""" funzione per il calcolo dello score di un giocatore """
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

""" funzione che splitta i record di redis, nel caso in cui volessi parallelizzare usando redis """
def splitRedisRecord(limit, spark_context):
	redisclient = redis.StrictRedis(host=sc.getConf().get('redis_connection'), port=6379, db=0)
	parallel_players = []
	player_list = []
	if limit == 0:
		for letter in LETTERS:
			for key in redisclient.scan_iter(letter + '*'):
				player_list.append(ast.literal_eval(redisclient.get(key)))
			parallel_players.append(spark_context.parallelize(player_list))
			player_list = []
	else:
		for key in redisclientlient.scan_iter():
			player_list.append(ast.literal_eval(redisclient.get(key)))
			if len(player_list) == limit:
				parallel_players.append(spark_context.parallelize(player_list))
				player_list = []
		if len(player_list) != 0:
			parallel_players.append(spark_context.parallelize(player_list))
	return spark_context.union(parallel_players)

""" funzione secondaria, che splitta i record di mongoDB, nel caso in cui non volessimo usare il connettore """
def splitMongoRecord(limit, spark_context):
	parallel_players = []
	player_list = []
	mongoClient = pymongo.MongoClient('mongodb://' + spark_context.getConf().get('mongo_host') + ':27017/')
	db = mongoClient['basketball_reference']
	if limit == 0:
		parallel_players = spark_context.parallelize([x for x in db.basketball_reference.find()])
	else:
		count = db.basketball_reference.count()
		counter = -1
		while counter < count:
			player_list.append(spark_context.parallelize(db.basketball_reference.find({ 'number' : { '$gt': counter, '$lt' : int(counter) + int(limit) } })))
			counter = int(counter) + int(limit)
		parallel_players = spark_context.union(player_list)
	return parallel_players

""" funzione che calcola lo score su tutti i player parallelizzando, e infine normalizzando i valori
su una scala da 0 a 100 """
def analyze(sc, percentage, tresholds, out = False, bonus = None, normalizer = False):
	spark_context = sc 
	parallel_players = []
	if spark_context.getConf().get("provider") == 'mongo':
		parallel_players = spark_context.mongoRDD('mongodb://' + spark_context.getConf().get('mongo_host') + ':27017/basketball_reference.basketball_reference')
	if spark_context.getConf().get("provider") == 'redis':
		limit = spark_context.getConf().get('limit')
		parallel_players = splitRedisRecord(limit, spark_context)
	f = lambda player: score4Player(player, percentage, tresholds, bonus, normalizer)	
	scores = parallel_players.map(f)
	if out:
		util.pretty_print(util.normalize_scores(100,scores.collect()))
	else:
		return scores

""" funzione che prende il college del player passato come parametro e ritorna una coppia (college, (score,1)) """
def collegeScore(player, score):
	redisClient = redis.StrictRedis(host=sc.getConf().get('redis_connection'), port=6379, db=0)
	college = ast.literal_eval(redisClient.get(player))['college']
	return (college, (score,1))

""" funzione che calcola lo score per tutti i college """
def collegeAnalysis(sc, percentage, tresholds, bonus = None, category="", normalizer = False):
	spark_context = sc 
	player2Score = []
	if not os.path.isfile('res_' + category + '.tsv'):
		player2Score = analyze(sc, percentage, tresholds, bonus = bonus, normalizer = normalizer)
	else:
		with open('res_' + category + '.tsv') as playerFile:
			player2Score = csv.reader(playerFile, delimiter='\t')

	college2score = player2Score.map(lambda (player, score): collegeScore(player, score)).reduceByKey(lambda (score1,one1), (score2,one2): (score1+score2,one1+one2)).collect()
	util.pretty_print(util.normalize_scores_college(100, college2score))
