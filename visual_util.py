import pymongo
import csv
import constants
import sys

aspects = ['aro','thr','two','def','att','plm','reb']
scoreString = map(lambda aspect: aspect + '_score', aspects)
mongoClient = pymongo.MongoClient(constants.MONGO_CONNECTION)
db = mongoClient['basketball_reference']
basketball_reference = db.basketball_reference

with open('player.csv') as p:
	reader = csv.reader(p, delimiter='\t')
	p_content = [x for x in reader]


def normalize(max_range, value, max_value):
	return value * max_range / max_value

def getPlayerName(ID):
	for line in p_content:
		if line[0] == ID:
			return line[1]

if sys.argv[1] == 'compact':
	collection = db.basketball_reference
	compacted = db.compacted
	cursor = collection.find({},{"player_id":1, "college":1, "state":1, "seasons":1})
	for record in cursor:
		new_record = {"player_id":record['player_id'], "college":record['college'], "state":record['state']}
		new_record['seasons'] = []
		seasons = []
		for season in record['seasons'].keys():
			stats = []
			for stat in record['seasons'][season]['all']:
				stats.append({stat: record['seasons'][season]['all'][stat]})
			seasons.append({'season': season, 'stats': stats})
		new_record['seasons'] = seasons
		compacted.insert(new_record)

if sys.argv[1] == 'colleges':
	collection = db.colleges

	with open('data/complete_with_abbreviations.tsv','rb') as f:
		reader = csv.reader(f, delimiter="\t")
		for line in reader:
			doc = collection.find_one({'name': line[0]})
			if doc == None:
				collection.insert({'name': line[0], 'state_abbreviation': line[2], 'score': float(line[3]), 'players': float(line[4]), 'state': line[1]})
	for aspect in aspects:
		with open('data/' + aspect + '.tsv') as f:
			reader = csv.reader(f, delimiter="\t")
			for line in reader:
				doc = collection.find_one({'name': line[0]})
				if doc != None:
					try:
						doc[aspect + '_score'] = float(line[1])
						collection.save(doc)
					except IndexError:
						pass

if sys.argv[1] == 'players':
	collection = db.players
	"""for player in basketball_reference.find():
		p = collection.find_one({'player_id': player['player_id']})
		if p == None:
			collection.insert({'name': getPlayerName(player['player_id']), 'player_id': player['player_id'], 'college': player['college']})"""
	for aspect in aspects:
		with open('data/player_' + aspect + '.tsv', 'rb') as f:
			reader = csv.reader(f, delimiter="\t")
			for line in reader:
				try:
					p = collection.find_one({'player_id': line[0]})
					p[aspect + '_score'] = float(line[1])
					collection.save(p)
				except IndexError: 
					pass
	for doc in collection.find():
		score = 0
		for string in scoreString:
			score += doc[string]
		doc['score'] = score
		score = 0
		collection.save(doc)




"""
	with open('data/complete_with_abbreviations.tsv','rb') as f:
		reader = csv.reader(f, delimiter="\t")
		for line in reader:
			doc = collection.find_one({'name': line[0]})
			if doc == None:
				collection.insert({'name': line[0], 'state_abbreviation': line[2], 'score': float(line[3]), 'players': float(line[4]), 'state': line[1]})
	for aspect in aspects:
		with open('data/' + aspect + '.tsv') as f:
			reader = csv.reader(f, delimiter="\t")
			for line in reader:
				doc = collection.find_one({'name': line[0]})
				if doc != None:
					try:
						doc[aspect + '_score'] = float(line[1])
						collection.save(doc)
					except IndexError:
						pass
"""

if sys.argv[1] == 'states':
	collection = db.states

	with open('data/states_aggregated.tsv', 'rb') as f:
		reader = csv.reader(f, delimiter="\t")
		for line in reader:
			doc = collection.find_one({'name': line[0]})
			if doc == None:
				collection.insert({'name': line[0], 'state_abbreviation': line[1], 'score': float(line[2]), 'players': float(line[3])})
	for aspect in aspects:
		with open('data/' + aspect + '_states.tsv') as f:
			reader = csv.reader(f, delimiter="\t")
			for line in reader:
				doc = collection.find_one({'name': line[0]})
				if doc != None:
					try:
						doc[aspect + '_score'] = float(line[1])
						collection.save(doc)
					except IndexError:
						pass

if sys.argv[1] == 'normal':
	colleges = db.colleges
	for string in scoreString:
		docs = colleges.find()
		max_value = colleges.find().sort([(string,-1)]).limit(1)[0][string]
		for doc in docs:
			doc[string] = normalize(100, doc[string], max_value)
			colleges.save(doc)

	docs = colleges.find()
	for doc in docs:
		doc['score'] = 0
		for string in scoreString:
			doc['score'] += doc[string]
		colleges.save(doc)

	states = db.states
	score = 0
	for string in scoreString + ['score']:
		for doc in states.find():
			colls = colleges.find({'state': doc['name']})
			for coll in colls:
				score += coll[string]
			doc[string] = score
			states.save(doc)
			score = 0

	for string in scoreString:
		docs = states.find()
		max_value = states.find().sort([(string,-1)]).limit(1)[0][string]
		for doc in docs:
			doc[string] = normalize(100, doc[string], max_value)
			states.save(doc)

	docs = states.find()
	for doc in docs:
		doc['score'] = 0
		for string in scoreString:
			doc['score'] += doc[string]
		states.save(doc)

	players = db.players
	for string in scoreString:
		docs = players.find()
		max_value = players.find().sort([(string,-1)]).limit(1)[0][string]
		for doc in docs:
			doc[string] = normalize(100, doc[string], max_value)
			players.save(doc)

	docs = players.find()
	for doc in docs:
		doc['score'] = 0
		for string in scoreString:
			doc['score'] += doc[string]
		players.save(doc)





