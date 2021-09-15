from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class average_gas(MRJob):
	def mapper(self,_,line):
		try:
			if len(line.split(",")) == 7: #is transaction dataset
				fields = line.split(",")
				gas = int(fields[4])
				time_epoch = int(fields[6])
				month = time.strftime("%m-%y", time.gmtime(time_epoch))
				join_key = str(fields[2]) #to address
				yield(join_key,(gas,month,1))
			elif len(line.split(",")) == 5:
				fields = line.split(",")
				join_key = str(fields[0])
				yield(join_key,("filler","fill",2))
		except:
			pass
	def reducer(self,addr,values):
		gas = None
		check = False
		for value in values:
			if value[2] == 2:
				check = True
			elif value[2] == 1:
				gas = value[0]
				month = value[1]
		if check and gas != None:
			yield(addr,(gas,month))
	
	def mapper_avg(self,key,values):
		addr = key #no longer needed
		gas = values[0]
		month = values[1]
		yield(month,(gas,1))
	def combiner_avg(self,key,values):
		count = 0
		sumGas = 0
		for value in values:
			count += value[1]
			sumGas += value[0]
		yield(key,(sumGas,count))

	def reducer_avg(self,key,values):
		count = 0
		sumGas = 0
		for value in values:
			count+= value[1]
			sumGas += value[0]
		yield(key, sumGas//count)

	def steps(self):
		return[MRStep(mapper=self.mapper,reducer=self.reducer),MRStep(mapper=self.mapper_avg,combiner=self.combiner_avg,reducer=self.reducer_avg)]

if __name__ == '__main__':
	average_gas.run()
