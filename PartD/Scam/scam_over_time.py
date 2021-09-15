from mrjob.job import  MRJob
from mrjob.step import MRStep
import time

class scam_over_time(MRJob):
	sector_table={}

	def mapper_join_init(self):
		with open("realscams.csv") as f:
			for line in f:
				fields = line.split(",")
				addresses = []
				for i in range(1,len(fields)):
					addresses.append(str(fields[i]))
				key = fields[0]
				self.sector_table[key] = addresses #key is scamID value is Addr
	def mapper_repl_join(self,_,line):
		try:
			fields = line.split(",")
			addr = str(fields[2])
			if addr in [x for v in self.sector_table.values() for x in v]:
				time_epoch = int(fields[6])
				month = time.strftime("%m-%Y", time.gmtime(time_epoch))
				money = int(fields[3])
				join_key = month + "," + addr
				yield(join_key,money)
		except:
			pass
	def mapper_length(self,key,values):
		yield(key,values)
	def reducer_agg(self,key,values):
		total_money = sum(values)
		yield(key,total_money)
	

	def mapper_money(self,key,values):
		fields = key.split(",")
		month = fields[0]
		addr = fields[1]
		sum = values
		yield(month,(addr,sum))
	
	def combiner_money(self,month,values):
		sorted_values = sorted(values,reverse=True,key= lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield(month,(value))
			i+=1
			if i >= 1:
				break
	def reducer_money(self,month,values):
		sorted_values = sorted(values,reverse = True, key=lambda tup:tup[1])
		i=0
		for value in sorted_values:
			yield("top",f"{month},{value[0]},{value[1]}")
			i+=1
			if i >= 1:
				break
	
	def steps(self):
		return[MRStep(mapper_init=self.mapper_join_init,mapper=self.mapper_repl_join),MRStep(mapper=self.mapper_length,reducer=self.reducer_agg),MRStep(mapper=self.mapper_money,combiner=self.combiner_money,reducer=self.reducer_money)] 



if __name__ == '__main__':
	scam_over_time.run()

