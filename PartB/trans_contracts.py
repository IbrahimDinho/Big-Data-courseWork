from mrjob.job import MRJob

class trans_contracts(MRJob):
	def mapper(self,_,line):
		try:
			if len(line.split('\t')) == 2:
				#This is initcw dataset
				fields = line.split('\t')
				join_key = str(fields[0].strip('"'))
				join_value = int(fields[1])
				yield(join_key,(join_value,1))
			elif len(line.split(',')) == 5:
				#this is contracts dataset
				fields = line.split(',')
				join_key = str(fields[0])
				join_value =int(fields[3])
				yield(join_key,(join_value,2))
		except:
			pass
				
	def reducer(self,address,values):
		try:

			money = None
			check = False
			for value in values:
				if value[1] == 1:
					money = value[0]
				elif value[1] == 2:
					check = True
			if check and money != None:
				yield(address, money)					
		except:
			pass
		
if __name__ == '__main__':
	trans_contracts.run()

