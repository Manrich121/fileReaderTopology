import storm

class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
 	lines = tup.values[0].split("\n")
	for line in lines:
 	    words = line.split(" ")
	    for word in words:
	      storm.emit([word])

SplitSentenceBolt().run()
