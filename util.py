from __future__ import print_function

def pretty_print(couples):
	map(lambda (x,y): print('\t'.join([x,str(y)])), couples)

def normalize_scores(max_value, scores):
	max_score = max([x for (y,x) in scores])
	return map(lambda (x,y): (x,y*max_value/max_score), scores)
