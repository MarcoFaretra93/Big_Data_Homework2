import lxml.html
import time
import csv

letters = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','y','z']

with open('player.csv', 'wb') as csvfile:
	writer = csv.writer(csvfile, delimiter = '\t')
	writer.writerow(['ID', 'NAME', 'COLLEGE', 'FROM', 'TO'])
	for element in letters:
		url = 'http://www.basketball-reference.com/players/' + element + '/'
		page = lxml.html.parse(url).getroot()

		""" get all name of players """
		names = page.xpath("//th[@data-stat='player']/a/text()|//th[@data-stat='player']/strong/a/text()")

		""" get all id of players """
		identificativi = page.xpath("//th[@data-append-csv]/@data-append-csv")

		""" get all college of players """
		collegeTemp = page.xpath("//td[@data-stat='college_name']")
		college = []

		for element in collegeTemp:
			try:
				college.append(element.getchildren()[0].text)
			except IndexError:
				college.append("null")

		""" get from and to """
		fromValues = page.xpath("//td[@data-stat='year_min']/text()")
		toValues = page.xpath("//td[@data-stat='year_max']/text()")

		""" write csv with all elements """
		for i in range(len(names)):
			row = []
			row.append(identificativi[i])
			row.append(names[i])
			row.append(college[i])
			row.append(fromValues[i])
			row.append(toValues[i])
			writer.writerow(row)

		print "finish: " + url

		time.sleep(1)

print "finish all"

