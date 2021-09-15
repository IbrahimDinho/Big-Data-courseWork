from mrjob.job import MRJob
import time

class no_october_transactions(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(",")
			if len(fields) == 7:
				time_epoch = int(fields[6])
				day = time.strftime("%d-%m-%y", time.gmtime(time_epoch))
				yield(day,1)
		except:
			pass
	def combiner(self,day,count):
		yield(day,sum(count))
	def reducer(self,day,count):
		oct = day.split("-")
		if oct[1] =="10" and oct[2] == "17":
			yield(day,sum(count))
		
if __name__ == '__main__':
	no_october_transactions.run()
