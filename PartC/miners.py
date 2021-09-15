from mrjob.job import MRJob
from mrjob.step import MRStep

class miners(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(',')
			size = int(fields[4])
			address = str(fields[2])
			yield(address, size)
		except:
			pass
	def combiner(self,address,value):
		yield(address,sum(value))
	
	def reducer(self,address,value):
		yield(address, sum(value))	
	
	
	def mapper_agg(self,address,sum):
		yield(None,(address,sum))
		
	def combiner_sum(self,_,values):
		sorted_values = sorted(values,reverse = True, key= lambda tup:tup[1])	
		i=0
		for value in sorted_values:
			yield(None,(value))
			i+=1
			if i >= 10:
				break
	def reducer_sum(self,_,values):
		sorted_values = sorted(values,reverse = True, key= lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield(i, f"{value[0]},{value[1]}")
			i+=1
			if i >= 10:
				break
	def steps(self):
		return [MRStep(mapper=self.mapper, combiner=self.combiner,reducer=self.reducer), MRStep(mapper=self.mapper_agg, combiner = self.combiner_sum, reducer=self.reducer_sum)]

if __name__ == '__main__':
	miners.run()
