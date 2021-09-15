from mrjob.job import MRJob
import time
class average_transaction(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(',')
			if len(fields) == 7:
				money = int(fields[3])
				time_epoch = int(fields[6])
				date = time.strftime("%m-%Y", time.gmtime(time_epoch))
				yield(date, (money,1))
		except:
			pass
	def combiner(self,date,values):
		total = 0
		count = 0
		for value in values:
			total += value[0]
			count += value[1]
		yield(date,(total,count))
	def reducer(self,date,values):
		total = 0
		count = 0
		for value in values:
			total += value[0]
			count += value[1]
		average = float(total/count)
		yield(date,average)		
if __name__ == '__main__':
	average_transaction.run()
