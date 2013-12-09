import storm

class SplitSentenceBolt(storm.BasicBolt):
    def process(self, tup):
 	lines = tup.values[0].splitLines()
	for line in lines:
 	    words = line.replace("\t"," ").split(" ")
	    for word in words:
	      storm.emit([word.strip()])

SplitSentenceBolt().run()
