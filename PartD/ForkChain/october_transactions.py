from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class october_transactions(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(",")
			if len(fields) == 7:
				time_epoch = int(fields[6])
				day= time.strftime("%d-%m-%y", time.gmtime(time_epoch))
				addr = str(fields[2])
				money = int(fields[3])
				oct = day.split("-")
				if oct[1] == "10" and oct[2] == "17":
					yield(addr,money)
		except:
			pass

	def combiner(self,key,value):
		yield(key, sum(value))
	def reducer(self,key,value):
		yield(key,sum(value))
	
	def mapper_agg(self,key,total):
		yield(None,(key,total))
	def combiner_agg(self,_,values):
		sorted_values = sorted(values,reverse = True, key= lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield(None,(value))
			i+=1
			if i>=10:
				break
	def reducer_sum(self,_,values):
		sorted_values = sorted(values,reverse = True, key = lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield("top ten", f"{value[0]},{value[1]}")
			i+=1
			if i >=10:
				break

		
	def steps(self):
		return[MRStep(mapper=self.mapper,combiner=self.combiner,reducer=self.reducer),MRStep(mapper=self.mapper_agg,combiner=self.combiner_agg,reducer=self.reducer_sum)]

if __name__ == '__main__':
	october_transactions.run()

