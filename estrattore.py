import lxml.html
import time
import csv
import string
import perPlayerExtractor as ppExtractor
from urllib2 import urlopen
import wget
import pymongo

LETTERS = list(string.ascii_lowercase) #['b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','y','z']
XPATH_PLAYERS_NAMES = "//th[@data-stat='player']/a/text()|//th[@data-stat='player']/strong/a/text()"
XPATH_PLAYERS_IDS = "//th[@data-append-csv]/@data-append-csv"
XPATH_PLAYERS_COLLEGE = "//td[@data-stat='college_name']"
XPATH_PLAYERS_DEBUT = "//td[@data-stat='year_min']/text()"
XPATH_PLAYERS_LAST_SEASON = "//td[@data-stat='year_max']/text()"

MONGO_LOCAL_CONNECTION = "mongodb://localhost:27017/"

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
			reader = csv.reader(players, delimiter = '\t', )
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
											row.append(None) 
									else:
										row.append(data[game][attributo].text)

								if(len(head) == len(header + row)):
									writer.writerow(header + row)
								else: 
									writer.writerow(header + row + [None])
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
					wget.download(playerUrl, 'html_pages/' +  str(line[0]) + '_' + line[1] + '.html')
					time.sleep(1)
					print "finish: " + playerUrl 

def insertAll():
	client = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
	db = client['basketball_reference']
	basketball_reference = db.basketball_reference.initialize_ordered_bulk_op()
	with open('game_results.tsv', 'rb') as games:
		reader = csv.reader(games, delimiter = '\t', )
		reader.next() #skip header
		curr_player = ""
		curr_season = ""
		json_seasons = {}
		season_games = {}
		all_game_score = 0
		all_plus_minus = 0
		for line in reader:
			player_id = line[0]
			season = line[1]
			season_game = line[2]
			date = line[3]
			age_of_player = line[4]
			team = line[5]
			opponent = line[7]
			games_started = line[9]
			minutes_played = line[10]
			field_goals = line[11]
			field_goal_attempts = line[12]
			field_goal_percentage = line[13]
			three_point_field_goals = line[14]
			three_point_field_goals_attempts = line[15]
			three_point_field_goals_percentage = line[16]
			free_throws = line[17]
			free_throws_attempts = line[18]
			free_throws_percentage = line[19]
			offensive_rebounds = line[20]
			defensive_rebounds = line[21]
			total_rebounds = line[22]
			assists = line[23]
			steals = line[24]
			blocks = line[25]
			turnovers = line[26]
			personal_fouls = line[27]
			points = line[28]
			game_score = line[29]
			plus_minus = line[30]

			if(curr_player == ""):
				curr_player = player_id
			if(curr_season == ""):
				curr_season = season

			if(curr_season != season): 
				all_stats = insertSeasonStats(curr_season, curr_player)
				if(all_game_score != 0):
					all_stats['game_score'] = all_game_score / len(season_games)
				else: 
					all_stats['game_score'] = None
				if(all_plus_minus != 0):
					all_stats['plus_minus'] = all_plus_minus / len(season_games)
				else:
					all_stats['plus_minus'] = None
				season_games['all'] = all_stats
				json_seasons[curr_season] = season_games
				curr_season = season
				season_games = {}
				all_game_score = 0
				all_plus_minus = 0

			if(curr_player != player_id):
				final_obj = {curr_player : json_seasons}
				json_seasons = {}
				season_games = {}
				all_game_score = 0
				all_plus_minus = 0
				basketball_reference.insert(final_obj)
				print player_id
				curr_season = season
				curr_player = player_id

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

def insertSeasonStats(season, player):
	result = {}
	with open('stats.tsv', 'rb') as statistiche: 
		reader = csv.reader(statistiche, delimiter = '\t', )
		reader.next() #skip header

		for line in reader:
			if(line[0] == player and line[1] == season):
				result['games_played'] = line[2]
				result['played_minutes'] = line[3]
				result['field_goals'] = line[4]
				result['field_goals_attempted'] = line[5]
				result['field_goals_percentage'] = line[6]
				result['three_field_goals'] = line[7]
				result['three_field_goals_attempted'] = line[8]
				result['three_field_goals_percentage'] = line[9]
				result['2_field_goals'] = line[10]
				result['2_field_goals_attempted'] = line[11]
				result['2_field_goals_percentage'] = line[12]
				result['effective_field_goals_percentage'] = line[13]
				result['free_throws'] = line[14]
				result['free_throws_attempted'] = line[15]
				result['free_throws_percentage'] = line[16]
				result['offensive_rebounds'] = line[17]
				result['defensive_rebounds'] = line[18]
				result['total_rebounds'] = line[19]
				result['assists'] = line[20]
				result['steals'] = line[21]
				result['blocks'] = line[22]
				result['turnovers'] = line[23]
				result['personal_fouls'] = line[24]
				result['points'] = line[25]
				return result

def checkMancanti():
	client = pymongo.MongoClient(MONGO_LOCAL_CONNECTION)
	db = client['basketball_reference']
	basketball_reference = db.basketball_reference
	with open('player.csv', 'rb') as players:
		reader = csv.reader(players, delimiter = '\t', )
		reader.next() #skip header

		for line in reader:
			player_id = line[0]
			result = basketball_reference.distinct(player_id)
			if result == []:
				print player_id
			
