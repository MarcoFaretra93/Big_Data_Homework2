MONGO_CONNECTION = "mongodb://localhost:27017/"
REDIS_CONNECTION = ""

twop_percentage = {'2_field_goals_percentage' : 0.8, 'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.05}
twop_tresholds = [('2_field_goals_attempted', '>='),('played_minutes', '>=', '0.5'),('games_played', '>='),('three_field_goals_attempted', '>=')]

threep_percentage = {'free_throws_percentage' : 0.15, 'three_field_goals_percentage' : 0.85}
threep_tresholds = [('2_field_goals_attempted', '>='),('played_minutes', '>=', '0.5'),('games_played', '>='),('three_field_goals_attempted', '>=')]

att_percentage = {'effective_field_goals_percentage' : 0.3, 'points' : 0.7}
att_tresholds = [('field_goals_attempted', '>='),('played_minutes', '>=', '0.5')]

def_percentage = {'defensive_rebounds' : 0.5, 'steals' : 0.3, 'blocks' : 0.2}
def_tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]

reb_percentage = {'total_rebounds' : 0.9, 'steals' : 0.1}
reb_tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]

pm_percentage = {'plus_minus' : 1}
pm_tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]

all_around_tresholds = [('played_minutes', '>=', '0.5'),('games_played', '>=')]
all_around_percentage = {'effective_field_goals_percentage' : 0.25, 'free_throws_percentage' : 0.25, 'total_rebounds' : 0.17, 'blocks' : 0.16, 'steals': 0.17}

def setRedisConnectionAddress(address):
	REDIS_CONNECTION = address