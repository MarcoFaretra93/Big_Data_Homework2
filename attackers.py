import time
start_time = time.time()
import redis 
import ast 
from pyspark import SparkContext, SparkConf
import pymongo_spark
pymongo_spark.activate()

att_percentage = {'effective_field_goals_percentage' : 0.3, 'points' : 0.7}
att_tresholds = [('field_goals_attempted', '>='),('played_minutes', '>=', '0.5')]

conf = SparkConf()
conf.setAppName('NBA analysis')
conf.set("spark.eventlog.enabled", True)

sc = SparkContext.getOrCreate(conf)

sc.addPyFile('/home/hadoop/Big_Data_Homework2/scoring.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/util.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/lxml.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/pymongo.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/redis.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/wget.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/dependencies/bson.zip')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/constants.py')
sc.addPyFile('/home/hadoop/Big_Data_Homework2/mongo-hadoop/spark/src/main/python/pymongo_spark.py')

redisc = redis.StrictRedis(host='ec2-34-209-195-193.us-west-2.compute.amazonaws.com', port=6379, db=0)

def checkTreshold(season, op, values, player, redisclient):
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
def getBonus(bonus, season, stats, redisclient):
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
	redisClient = redis.StrictRedis(host='ec2-34-209-195-193.us-west-2.compute.amazonaws.com', port=6379, db=1)
	totalScore = 0
	count = 0
	try: 
		season = min([int(x.split('-')[0]) for x in player['seasons'].keys()])
		season = str(season) + '-' + str(season + 1)
		for i in range(4):
			annualScore = 0
			allParameters = player['seasons'][season]['all']
			if(checkTreshold(season, 'mean', tresholds, player, redisClient)):
				count += 1 
				for percentageKey in percentage.keys():
					annualScore += float(allParameters[percentageKey]) * float(percentage[percentageKey])
					totalScore += float(allParameters[percentageKey]) * float(percentage[percentageKey])
			if bonus != None:
				for b in bonus:
					totalScore += annualScore * getBonus(b, season, allParameters, redisClient)
			season = str(int(season.split('-')[0])+1) + '-' + str(int(season.split('-')[1])+1)
	except KeyError:
		pass
	finalScore = totalScore * 100
	count = count if count != 0 else 1
	return (player['player_id'], finalScore/count)

def splitRedisRecord(limit, spark_context):
	parallel_players = []
	player_list = []
	if limit == 0:
		for letter in LETTERS:
			for key in redisc.scan_iter(letter + '*'):
				player_list.append(ast.literal_eval(redisc.get(key)))
			parallel_players.append(spark_context.parallelize(player_list))
			player_list = []
	else:
		for key in redisc.scan_iter():
			player_list.append(ast.literal_eval(redisc.get(key)))
			if len(player_list) == limit:
				parallel_players.append(spark_context.parallelize(player_list))
				player_list = []
		if len(player_list) != 0:
			parallel_players.append(spark_context.parallelize(player_list))
	return spark_context.union(parallel_players)

def analyze(percentage, tresholds, out = False, bonus = None, normalizer = False):
	parallel_players = sc.mongoRDD('mongodb://ec2-34-209-195-193.us-west-2.compute.amazonaws.com:27017/basketball_reference.basketball_reference')
	scores = parallel_players.map(lambda player: score4Player(player, percentage, tresholds, bonus, normalizer))
	if out:
		print scores.collect()
	else:
		return scores

def collegeAnalysis(percentage, tresholds, bonus = None, category="", normalizer = False):
	player2Score = []
	if not os.path.isfile('res_' + category + '.tsv'):
		player2Score = analyze(percentage, tresholds, bonus = bonus, normalizer = normalizer)
	else:
		with open('res_' + category + '.tsv') as playerFile:
			player2Score = csv.reader(playerFile, delimiter='\t')

	college2score = player2Score.map(lambda (player, score): collegeScore(player, score)).reduceByKey(lambda (score1,one1), (score2,one2): (score1+score2,one1+one2)).collect()
	print college2score



collegeAnalysis(att_percentage, att_tresholds)

sc.stop()

print("--- %s seconds ---" % (time.time() - start_time))


