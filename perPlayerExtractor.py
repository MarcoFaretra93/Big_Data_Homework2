import lxml.html
import time
import csv
from urllib2 import urlopen

XPATH_PLAYED_GAMES = "//tr[@id='per_game.<season>']/td[@data-stat='g']|//tr[@id='per_game.<season>']/td[@data-stat='g']/strong"
XPATH_PLAYED_MINUTES = "//tr[@id='per_game.<season>']/td[@data-stat='mp_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='mp_per_g']/strong"
XPATH_FIELD_GOALS = "//tr[@id='per_game.<season>']/td[@data-stat='fg_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fg_per_g']/strong"
XPATH_FIELD_GOALS_ATTEMPTED = "//tr[@id='per_game.<season>']/td[@data-stat='fga_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fga_per_g']/strong"
XPATH_FIELD_GOALS_PERCENTAGE = "//tr[@id='per_game.<season>']/td[@data-stat='fg_pct']|//tr[@id='per_game.<season>']/td[@data-stat='fg_pct']/strong"
XPATH_3_FIELD_GOALS = "//tr[@id='per_game.<season>']/td[@data-stat='fg3_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fg3_per_g']/strong"
XPATH_3_FIELD_GOALS_ATTEMPTED= "//tr[@id='per_game.<season>']/td[@data-stat='fg3a_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fg3a_per_g']/strong"
XPATH_3_FIELD_GOALS_PERCENTAGE = "//tr[@id='per_game.<season>']/td[@data-stat='fg3_pct']|//tr[@id='per_game.<season>']/td[@data-stat='fg3_pct']/strong"
XPATH_2_FIELD_GOALS = "//tr[@id='per_game.<season>']/td[@data-stat='fg2_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fg2_per_g']/strong"
XPATH_2_FIELD_GOALS_ATTEMPTED= "//tr[@id='per_game.<season>']/td[@data-stat='fg2a_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fg2a_per_g']/strong"
XPATH_2_FIELD_GOALS_PERCENTAGE = "//tr[@id='per_game.<season>']/td[@data-stat='fg2_pct']|//tr[@id='per_game.<season>']/td[@data-stat='fg2_pct']/strong"
XPATH_EFFECTIVE_FIELD_GOALS_PERCENTAGE = "//tr[@id='per_game.<season>']/td[@data-stat='efg_pct']|//tr[@id='per_game.<season>']/td[@data-stat='efg_pct']/strong"
XPATH_FREE_THROWS = "//tr[@id='per_game.<season>']/td[@data-stat='ft_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='ft_per_g']/strong"
XPATH_FREE_THROWS_ATTEMPTED = "//tr[@id='per_game.<season>']/td[@data-stat='fta_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='fta_per_g']/strong"
XPATH_FREE_THROWS_PERCENTAGE = "//tr[@id='per_game.<season>']/td[@data-stat='ft_pct']|//tr[@id='per_game.<season>']/td[@data-stat='ft_pct']/strong"
XPATH_OFFENSIVE_REBOUNDS = "//tr[@id='per_game.<season>']/td[@data-stat='orb_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='orb_per_g']/strong"
XPATH_DEFENSIVE_REBOUNDS = "//tr[@id='per_game.<season>']/td[@data-stat='drb_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='drb_per_g']/strong"
XPATH_TOTAL_REBOUNDS = "//tr[@id='per_game.<season>']/td[@data-stat='trb_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='trb_per_g']/strong"
XPATH_ASSISTS = "//tr[@id='per_game.<season>']/td[@data-stat='ast_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='ast_per_g']/strong"
XPATH_STEALS = "//tr[@id='per_game.<season>']/td[@data-stat='stl_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='stl_per_g']/strong"
XPATH_BLOCKS = "//tr[@id='per_game.<season>']/td[@data-stat='blk_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='blk_per_g']/strong"
XPATH_TURNOVERS = "//tr[@id='per_game.<season>']/td[@data-stat='tov_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='tov_per_g']/strong"
XPATH_PERSONAL_FOULS = "//tr[@id='per_game.<season>']/td[@data-stat='pf_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='pf_per_g']/strong"
XPATH_POINTS = "//tr[@id='per_game.<season>']/td[@data-stat='pts_per_g']|//tr[@id='per_game.<season>']/td[@data-stat='pts_per_g']/strong"

xpaths = [XPATH_PLAYED_GAMES, XPATH_PLAYED_MINUTES, XPATH_FIELD_GOALS, XPATH_FIELD_GOALS_ATTEMPTED, XPATH_FIELD_GOALS_PERCENTAGE, XPATH_3_FIELD_GOALS, XPATH_3_FIELD_GOALS_ATTEMPTED, XPATH_3_FIELD_GOALS_PERCENTAGE, XPATH_2_FIELD_GOALS, XPATH_2_FIELD_GOALS_ATTEMPTED, XPATH_2_FIELD_GOALS_PERCENTAGE, XPATH_EFFECTIVE_FIELD_GOALS_PERCENTAGE, XPATH_FREE_THROWS, XPATH_FREE_THROWS_ATTEMPTED, XPATH_FREE_THROWS_PERCENTAGE, XPATH_OFFENSIVE_REBOUNDS, XPATH_DEFENSIVE_REBOUNDS, XPATH_TOTAL_REBOUNDS, XPATH_ASSISTS, XPATH_STEALS, XPATH_BLOCKS, XPATH_TURNOVERS, XPATH_PERSONAL_FOULS, XPATH_POINTS]

def applyXpaths(page, season, playerId, resultList):
    for expression in xpaths:
        try:
            result = page.xpath(expression.replace('<season>', str(season)))
            if(len(result) > 1):
                resultList.append(page.xpath(expression.replace('<season>', str(season)))[1].text)
            else:
                resultList.append(page.xpath(expression.replace('<season>', str(season)))[0].text)
        except Exception:
            resultList.append(None)
    return resultList

def extractBaseStats(page, playerId, season):
    row = [playerId, str(season-1) + '-' + str(season)]
    row = applyXpaths(page, season, playerId, row)
    return row

def writePlayerStat(writer, playerId, debut, lastSeason, allSeason = True):
    url = 'http://www.basketball-reference.com/players/' + playerId[0] + '/' + playerId + '.html'
    page = lxml.html.parse(url).getroot()
    if lastSeason == debut:
        lastSeason = int(lastSeason) + 1
    if allSeason:
        for season in range(int(debut), int(lastSeason)):
            writer.writerow(extractBaseStats(page, playerId, season))
    else:
        for season in range(int(debut), int(debut+4)):
            writer.writerow(extractBaseStats(page, playerId, season))