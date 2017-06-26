import lxml.html
import time
import csv
import string
import perPlayerExtractor as ppExtractor
from urllib2 import urlopen
import wget
import pymongo
import redis
import os.path
import sys

LETTERS = list(string.ascii_lowercase) #['b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','y','z']
XPATH_PLAYERS_NAMES = "//th[@data-stat='player']/a/text()|//th[@data-stat='player']/strong/a/text()"
XPATH_PLAYERS_IDS = "//th[@data-append-csv]/@data-append-csv"
XPATH_PLAYERS_COLLEGE = "//td[@data-stat='college_name']"
XPATH_PLAYERS_DEBUT = "//td[@data-stat='year_min']/text()"
XPATH_PLAYERS_LAST_SEASON = "//td[@data-stat='year_max']/text()"

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"

with open('player.csv') as p:
	reader = csv.reader(p, delimiter='\t')
	p_content = [x for x in reader]

with open('stats.tsv') as s:
	reader = csv.reader(s, delimiter='\t')
	s_content = [x for x in reader]



client = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
db = client['basketball_reference']
redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)



def getStateFromCollege(collegeName):
	if(collegeName == "null"):
		return "null"
	else:
		time.sleep(1)
		try:
			url = 'https://en.wikipedia.org/wiki/' + collegeName.replace(" ", "_") 
			page = lxml.html.parse(urlopen(url)).getroot()
			state = page.xpath("//span[contains(@class,'state')]/a/text()")
			return state[0]
		except Exception:
			return "no state"

def getAllPlayerBaseInfo(sleep = 1, outFile = 'player.csv'):
	with open(outFile, 'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter = '\t')
		writer.writerow(['ID', 'NAME', 'COLLEGE', 'STATE', 'FROM', 'TO'])
		for element in letters:
			url = 'http://www.basketball-reference.com/players/' + element + '/'
			page = lxml.html.parse(url).getroot()

			""" get all name of players """
			names = page.xpath(XPATH_PLAYERS_NAMES)

			""" get all id of players """
			identificativi = page.xpath(XPATH_PLAYERS_IDS)

			""" get all college of players """
			collegeTemp = page.xpath(XPATH_PLAYERS_COLLEGE)
			college = []

			for element in collegeTemp:
				try:
					college.append(element.getchildren()[0].text)
				except IndexError:
					college.append("null")

			""" get from and to """
			fromValues = page.xpath(XPATH_PLAYERS_DEBUT)
			toValues = page.xpath(XPATH_PLAYERS_LAST_SEASON)

			""" write csv with all elements """
			for i in range(len(names)):
				row = []
				row.append(identificativi[i])
				row.append(names[i])
				row.append(college[i])
				row.append(getStateFromCollege(college[i]))
				row.append(fromValues[i])
				row.append(toValues[i])
				writer.writerow(row)

			print "finish: " + url
			time.sleep(sleep)

def getStats(outFile = 'stats.tsv'):
	with open(outFile, 'wb') as tsvfile:
		writer = csv.writer(tsvfile, delimiter = '\t')
		writer.writerow(['PLAYER_ID', 'SEASON', 'GAMES_PLAYED', 'PLAYED_MINUTES', 'FIELD_GOALS', 'FIELD_GOALS_ATTEMPTED', 'FIELD_GOALS_PERCENTAGE', '3_FIELD_GOALS', '3_FIELD_GOALS_ATTEMPTED', '3_FIELD_GOALS_PERCENTAGE', '2_FIELD_GOALS', '2_FIELD_GOALS_ATTEMPTED', '2_FIELD_GOALS_PERCENTAGE', 'EFFECTIVE_FIELD_GOALS_PERCENTAGE', 'FREE_THROWS', 'FREE_THROWS_ATTEMPTED', 'FREE_THROWS_PERCENTAGE', 'OFFENSIVE_REBOUNDS', 'DEFENSIVE_REBOUNDS', 'TOTAL_REBOUNDS', 'ASSISTS', 'STEALS', 'BLOCKS', 'TURNOVERS', 'PERSONAL_FOULS', 'POINTS'])
		with open('player.csv', 'rb') as players:
			reader = csv.reader(players, delimiter = '\t', )
			reader.next() #skip header
			for line in reader:
				print "doing " + line[1]
				ppExtractor.writePlayerStat(writer, line[0], line[4], line[5])

def getGamesForPlayerForYear(outFile = 'game_results.tsv'):
	with open(outFile, 'wb') as tsvFile:
		writer = csv.writer(tsvFile, delimiter = '\t')
		head = ['PLAYER_ID', 'SEASON', 'SEASON GAME', 'DATE', 'AGE_OF_PLAYER', 'TEAM', 'AT', 'OPPONENT', 'RESULT_GAME', 'GAMES_STARTED', 'MINUTES_PLAYED', 'FIELD_GOALS',
			'FIELD_GOAL_ATTEMPTS', 'FIELD_GOAL_PERCENTAGE', '3-POINT_FIELD_GOALS', '3-POINT_FIELD_GOAL_ATTEMPTS', '3-POINT_FIELD_GOAL_PERCENTAGE',
			'FREE_THROWS', 'FREE_THROWS_ATTEMPTS', 'FREE_THROW_PERCENTAGE', 'OFFENSIVE_REBOUNDS', 'DEFENSIVE_REBOUNDS', 'TOTAL_REBOUNDS', 'ASSISTS', 
			'STEALS', 'BLOCKS', 'TURNOVERS', 'PERSONAL_FOULS', 'POINTS', 'GAME_SCORE', 'PLUS/MINUS']
		writer.writerow(head)
		with open('stats.tsv', 'rb') as players:
			reader = csv.reader(players, delimiter = '\t')
			reader.next() #skip header
			for line in reader:
				if(line[2] != ''):
					with open('html_pages/' + str(line[0]) + '_' + line[1] + '.html') as f:
						htmlPage = f.read()
					page = lxml.html.fromstring(htmlPage)
					data = page.xpath('//*[@id="pgl_basic"]/tbody/tr')
					header = [line[0], line[1]]
					for game in range(len(data)):
						try: 
							if('pgl_basic' in data[game].attrib['id']):
								row = []
								for attributo in range(1,len(data[game])):
									if data[game][attributo].text == None:
										try:
											row.append(data[game][attributo][0].text)
										except IndexError:
											row.append("0") 
									else:
										row.append(data[game][attributo].text)

								if(len(head) == len(header + row)):
									writer.writerow(header + row)
								else: 
									writer.writerow(header + row + ["0"])
						except KeyError:
							pass
					print "finish: " + str(line[0]) + '_' + line[1]

def downloadPage():
	with open('stats.tsv', 'rb') as players:
			url = 'http://www.basketball-reference.com/players/'
			reader = csv.reader(players, delimiter = '\t', )
			reader.next() #skip header
			for line in reader:
				if(line[2] != ''):
					playerUrl = url + str(line[0][0]) + '/' + str(line[0]) + '/gamelog/' + line[1].split('-')[1]
					if not os.path.isfile('html_pages/'+  str(line[0]) + '_' + line[1] + '.html'):
						wget.download(playerUrl, 'html_pages/' +  str(line[0]) + '_' + line[1] + '.html')
						#time.sleep(1)
						print "finish: " + playerUrl 
					else:
						print 'skipped: ' + playerUrl

def insertAll(limit):
	basketball_reference = db.basketball_reference.initialize_ordered_bulk_op()
	counter = 0
	with open('game_results.tsv', 'rb') as games:
		reader = csv.reader(games, delimiter = '\t', )
		reader.next() #skip header
		curr_player = ""
		curr_season = ""
		json_seasons = {}
		season_games = {}
		all_game_score = 0
		all_plus_minus = 0
		count = 0
		for line in reader:
			player_id = line[0] if line[0] else '0'
			season = line[1] if line[1] else '0'
			season_game = line[2] if line[2] else '0'
			date = line[3] if line[3] else '0'
			age_of_player = line[4] if line[4] else '0'
			team = line[5] if line[5] else '0'
			opponent = line[7] if line[7] else '0'
			games_started = line[9] if line[9] else '0'
			minutes_played = line[10] if line[10] else '0'
			field_goals = line[11] if line[11] else '0'
			field_goal_attempts = line[12] if line[12] else '0'
			field_goal_percentage = line[13] if line[13] else '0'
			three_point_field_goals = line[14] if line[14] else '0'
			three_point_field_goals_attempts = line[15] if line[15] else '0'
			three_point_field_goals_percentage = line[16] if line[16] else '0'
			free_throws = line[17] if line[17] else '0'
			free_throws_attempts = line[18] if line[18] else '0'
			free_throws_percentage = line[19] if line[19] else '0'
			offensive_rebounds = line[20] if line[20] else '0'
			defensive_rebounds = line[21] if line[21] else '0'
			total_rebounds = line[22] if line[22] else '0'
			assists = line[23] if line[23] else '0'
			steals = line[24] if line[24] else '0'
			blocks = line[25] if line[25] else '0'
			turnovers = line[26] if line[26] else '0'
			personal_fouls = line[27] if line[27] else '0'
			points = line[28] if line[28] else '0'
			game_score = line[29] if line[29] else '0'
			plus_minus = line[30] if line[30] else '0'

			if(curr_player == ""):
				curr_player = player_id
			if(curr_season == ""):
				curr_season = season

			if(curr_player != player_id):
				count += 1
				collAndState = getCollegeAndState(curr_player)

				all_stats = insertSeasonStats(curr_season, curr_player)
				if(all_game_score != 0):
					all_stats['game_score'] = all_game_score / len(season_games)
				else: 
					all_stats['game_score'] = '0'
				if(all_plus_minus != 0):
					all_stats['plus_minus'] = all_plus_minus / len(season_games)
				else:
					all_stats['plus_minus'] = '0'
				season_games['all'] = all_stats
				json_seasons[curr_season] = season_games

				final_obj = {'seasons' : json_seasons, 'player_id': curr_player, 'college': collAndState['college'], 'state': collAndState['state'], 'number' : counter}
				json_seasons = {}
				season_games = {}
				all_game_score = 0
				all_plus_minus = 0
				basketball_reference.insert(final_obj)
				print player_id
				curr_season = season
				curr_player = player_id
				counter += 1
				if count == limit:
					break

			if curr_season != season:
				all_stats = insertSeasonStats(curr_season, curr_player)
				if(all_game_score != 0):
					all_stats['game_score'] = all_game_score / len(season_games)
				else: 
					all_stats['game_score'] = '0'
				if(all_plus_minus != 0):
					all_stats['plus_minus'] = all_plus_minus / len(season_games)
				else:
					all_stats['plus_minus'] = '0'
				season_games['all'] = all_stats
				json_seasons[curr_season] = season_games
				curr_season = season
				season_games = {}
				all_game_score = 0
				all_plus_minus = 0

			
			season_games[season_game] = {'date': date, 'age_of_player' : age_of_player, 'team' : team, 'opponent' : opponent, 'games_started': games_started
				, 'minutes_played' : minutes_played, 'field_goals' : field_goals, 'field_goal_attempts' : field_goal_attempts, 'field_goal_percentage' : field_goal_percentage,
				'free_throws': free_throws, 'free_throws_attempts': free_throws_attempts, 'free_throws_percentage' : free_throws_percentage, 
				'offensive_rebounds' : offensive_rebounds, 'defensive_rebounds' : defensive_rebounds, 'total_rebounds' : total_rebounds, 'assists' : assists
				, 'steals' : steals, 'blocks' : blocks, 'turnovers' : turnovers, 'personal_fouls': personal_fouls, 'points' : points, 'game_score': game_score, 'plus_minus' : plus_minus}

			try:
				all_game_score += float(game_score)
			except ValueError:
				pass

			try:
				all_plus_minus += float(plus_minus)
			except ValueError:
				pass

		result = basketball_reference.execute()
		print result

def getCollegeAndState(player_id):
	result = {}
	"""
	with open('player.csv', 'rb') as players:
		reader = csv.reader(players, delimiter = '\t', )
		reader.next() #skip header
		for line in reader:"""
	for line in p_content:
		if(line[0] == player_id):
			result['college'] = line[2]
			result['state'] = line[3]
			return result

def insertSeasonStats(season, player):
	result = {}
	"""
	with open('stats.tsv', 'rb') as statistiche: 
		reader = csv.reader(statistiche, delimiter = '\t', )
		reader.next() #skip header
		for line in reader:
			"""
	for line in s_content:
		if(line[0] == player and line[1] == season):
			result['games_played'] = line[2] if line[2] else '0'
			result['played_minutes'] = line[3] if line[3] else '0'
			result['field_goals'] = line[4]  if line[4] else '0'
			result['field_goals_attempted'] = line[5] if line[5] else '0'
			result['field_goals_percentage'] = line[6] if line[6] else '0'
			result['three_field_goals'] = line[7] if line[7] else '0'
			result['three_field_goals_attempted'] = line[8] if line[8] else '0'
			result['three_field_goals_percentage'] = line[9] if line[9] else '0'
			result['2_field_goals'] = line[10] if line[10] else '0'
			result['2_field_goals_attempted'] = line[11] if line[11] else '0'
			result['2_field_goals_percentage'] = line[12] if line[12] else '0'
			result['effective_field_goals_percentage'] = line[13] if line[13] else '0'
			result['free_throws'] = line[14] if line[14] else '0'
			result['free_throws_attempted'] = line[15] if line[15] else '0'
			result['free_throws_percentage'] = line[16] if line[16] else '0'
			result['offensive_rebounds'] = line[17] if line[17] else '0'
			result['defensive_rebounds'] = line[18] if line[18] else '0'
			result['total_rebounds'] = line[19] if line[19] else '0'
			result['assists'] = line[20] if line[20] else '0'
			result['steals'] = line[21] if line[21] else '0'
			result['blocks'] = line[22] if line[22] else '0'
			result['turnovers'] = line[23] if line[23] else '0'
			result['personal_fouls'] = line[24] if line[24] else '0'
			result['points'] = line[25] if line[25] else '0'
			return result
			
def insertIntoRedisFromMongo():
	players = db.basketball_reference.find({},{'_id': False})
	print players[0]['player_id']
	for player in players:
		redisClient.set(player['player_id'], player)
		print 'inserted ' + player['player_id']

if __name__ == '__main__':
	if sys.argv[1] == "all":
		if sys.argv[2] == "delete":
			db.basketball_reference.drop()
			redisClient.flushall()
		insertAll(int(sys.argv[3]))
		insertIntoRedisFromMongo()


