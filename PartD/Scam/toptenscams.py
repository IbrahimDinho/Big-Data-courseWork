from mrjob.job import MRJob

class topscam(MRJob):
    def mapper(self,_,line):
        try:
            fields = line.split('\t')
            money = int(fields[1])
            if money > 0:
                address = fields[0]
                yield(None,(address,money))
        except:
            pass

    def combiner(self, _, values):
        sorted_values = sorted(values,reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield(None, value)
            i +=1
            if i >= 10:
                break
    def reducer(self,_,values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield(i,f"{value[0]},{value[1]}".strip('"'))
            i+=1
            if i >= 10:
                break

if __name__ == '__main__':
    topscam.run()
