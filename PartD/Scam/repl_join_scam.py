from mrjob.job import  MRJob
from mrjob.step import MRStep

class repl_join_scam(MRJob):
    sector_table = {}

    def mapper_join_init(self):
        with open("realscams.csv") as f:
            for line in f:
                fields = line.split(",")
                addresses = []
                for i in range(1,len(fields)):
                    addresses.append(str(fields[i]))

                key = fields[0]
                self.sector_table[key] = addresses #key is ScamID- is not used.


    def mapper_repl_join(self,_,line):
        try:
            fields = line.split(",")
            join = str(fields[2]) #to_address of transactions
            if join in [x for v in self.sector_table.values() for x in v]:
                money = int(fields[3])
                yield(join,money)
        except:
            pass

    def mapper_length(self,key,money):
        yield(key,money)

    def reducer_agg(self,key,money):
        total = sum(money)
        yield(key,total)

    def steps(self):
        return[MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),

        MRStep(mapper=self.mapper_length,
                reducer=self.reducer_agg)]


if __name__ == '__main__':
    repl_join_scam.run()
