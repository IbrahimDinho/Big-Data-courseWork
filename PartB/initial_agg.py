from mrjob.job import MRJob

class initial_agg(MRJob):
	def mapper(self,_,line):
		try:
			fields = line.split(',')
			if len(fields) == 7:
				address = str(fields[2])
				money = int(fields[3])
				yield(address,money)
		except:
			pass
	def combiner(self,address,value):
		yield(address, sum(value))
	def reducer(self,address,value):
		yield(address,sum(value))

if __name__ == '__main__':
	initial_agg.run()
