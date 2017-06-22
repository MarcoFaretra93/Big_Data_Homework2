import pymongo
import util
import redis
import ast
import os.path
import csv
import redis

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"
REDIS_LOCAL_CONNECTION = "localhost"
REDIS_CLUSTER_CONNECTION = "ec2-52-38-110-138.us-west-2.compute.amazonaws.com"
mongoClient = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
db = mongoClient['basketball_reference']
client = redisClient = redis.StrictRedis(host=REDIS_LOCAL_CONNECTION, port=6379, db=0)


""" values = [(field_name, operator, modifier)] """
def checkTreshold(season, op, values, player):
	redisClient = redis.StrictRedis(host=REDIS_CLUSTER_CONNECTION, port=6379, db=1)
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

""" scorefinale += scoreAnnuale * percent*(valore - mediaValore) """
def getBonus(bonus, season, stats):
	redisClient = redis.StrictRedis(host=REDIS_CLUSTER_CONNECTION, port=6379, db=1)
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
def score4Player(player, percentage, tresholds, bonus = None):
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
				for percentageKeys in percentage.keys():
					annualScore += float(allParameters[percentageKeys]) * float(percentage[percentageKeys])
					totalScore += float(allParameters[percentageKeys]) * float(percentage[percentageKeys])
			if bonus != None:
				for b in bonus:
					totalScore += annualScore * getBonus(b, season, allParameters)
			season = str(int(season.split('-')[0])+1) + '-' + str(int(season.split('-')[1])+1)
	except KeyError:
		pass
	finalScore = totalScore * 100
	count = count if count != 0 else 1
	return (player['player_id'], finalScore/count)

def analyze(spark_context, percentage, tresholds, bonus = None):
	players = db.basketball_reference.find()
	parallel_players = spark_context.parallelize([p for p in players],10)
	scores = parallel_players.map(lambda player: score4Player(player, percentage, tresholds, bonus)).collect()
	util.pretty_print(util.normalize_scores(100,scores))

def collegeAnalysis(spark_context, category):
	player2Score = []
	if not os.path.isfile('res_' + category + '.tsv'):
		player2Score = analyze(category).collect()
	else:
		with open('res_' + category + '.tsv') as playerFile:
			player2Score = csv.reader(playerFile, delimiter='\t')
	toBeParallelized = []
	for player in player2Score:
		college = client.get(player)['college']
		toBeParallelized.append((college, player[1], player[2]))
	rdd = spark_context.parallelize(toBeParallelized)
	college2score = rdd.map(lambda (college, player, score): (college, score)).reduce(lambda (score1, score2): score1+score2).collect()
	util.pretty_print()

	#players = db.basketball_reference.find({'college' : $ne : {'null'}})



		




