from mrjob.job import MRJob
import time
class gas_over_time(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(",")
			if len(fields) == 7:
				time_epoch = int(fields[6])
				month = time.strftime("%m-%Y", time.gmtime(time_epoch))
				gas_price = int(fields[5])
				count = 1
				yield(month,(gas_price,count))
		except:
			pass

	def combiner(self,key,values):
		sumofGas = 0
		count = 0
		for value in values:
			sumofGas += value[0]
			count += value[1]
		yield(key,(sumofGas,count))
	
	def reducer(self,key,values):
		sumofGas = 0
		count = 0
		for value in values:
			sumofGas += value[0]
			count += value[1]
		yield(key, sumofGas//count) #returns a whole number


if __name__ == '__main__':
	gas_over_time.run()

	
