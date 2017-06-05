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

def applyXpaths(page, season, playerId, resultList):
    try:
        resultList.append(page.xpath(XPATH_PLAYED_GAMES.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_PLAYED_MINUTES.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FIELD_GOALS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FIELD_GOALS_ATTEMPTED.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FIELD_GOALS_PERCENTAGE.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_3_FIELD_GOALS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_3_FIELD_GOALS_ATTEMPTED.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_3_FIELD_GOALS_PERCENTAGE.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_2_FIELD_GOALS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_2_FIELD_GOALS_ATTEMPTED.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_3_FIELD_GOALS_PERCENTAGE.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_EFFECTIVE_FIELD_GOALS_PERCENTAGE.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FREE_THROWS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FREE_THROWS_ATTEMPTED.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_FREE_THROWS_PERCENTAGE.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_OFFENSIVE_REBOUNDS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_DEFENSIVE_REBOUNDS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_TOTAL_REBOUNDS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_ASSISTS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_STEALS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_BLOCKS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_TURNOVERS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_PERSONAL_FOULS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None)
    try:
        resultList.append(page.xpath(XPATH_POINTS.replace('<season>', str(season)))[0].text)
    except Exception:
        resultList.append(None) 
    return resultList

def extractBaseStats(page, playerId, seasonStart):
    row = [playerId, str(seasonStart) + '-' + str(seasonStart+1)]
    row = applyXpaths(page, seasonStart+1, playerId, row)
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