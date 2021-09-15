from mrjob.job import MRJob
import time
class no_monthly_transactions(MRJob):
	def mapper(self,_,line):
		try:
			fields=line.split(',')
			if len(fields) == 7:
				time_epoch = int(fields[6])
				month = time.strftime("%m-%Y", time.gmtime(time_epoch))
				yield(month,1)
		except:
			pass
	
	def combiner(self,month,count):
		yield(month,sum(count))
	def reducer(self,month,count):	
		yield(month,sum(count))

if __name__ == '__main__':
	no_monthly_transactions.run()
