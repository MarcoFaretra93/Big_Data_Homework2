import lxml.html
import time
import csv
import string
import perPlayerExtractor as ppExtractor
from urllib2 import urlopen
from bs4 import BeautifulSoup
import wget

LETTERS = list(string.ascii_lowercase) #['b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','y','z']
XPATH_PLAYERS_NAMES = "//th[@data-stat='player']/a/text()|//th[@data-stat='player']/strong/a/text()"
XPATH_PLAYERS_IDS = "//th[@data-append-csv]/@data-append-csv"
XPATH_PLAYERS_COLLEGE = "//td[@data-stat='college_name']"
XPATH_PLAYERS_DEBUT = "//td[@data-stat='year_min']/text()"
XPATH_PLAYERS_LAST_SEASON = "//td[@data-stat='year_max']/text()"

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

