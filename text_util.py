import sys
import csv

if __name__ == '__main__':
	if sys.argv[1] == "aggregate":
		with open(sys.argv[2]) as f:
			reader = csv.reader(f, delimiter='\t')
			university = ""
			score = 0
			alumni = 0
			for line in reader:
				try:
					if university == "":
						university = line[0]

					if university != line[0]:
						print '\t'.join([university, str(score), str(alumni)])
						university = line[0]
						score = float(line[1])
						alumni = float(line[2])

					else:
						score += float(line[1])
						alumni = float(line[2])
						#print '\t'.join([university, str(score), str(alumni)]) 


				except IndexError:
					reader.next()

			print '\t'.join([university, str(score), str(alumni)])