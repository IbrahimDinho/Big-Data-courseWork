dict = {}
with open("scams.csv" ,'r+') as fp:
    with open('scamsParseAgg.csv', 'w') as wf:
        for line in fp:
            fields = line.split(",")
            if fields[1] not in dict:
                fString = f'{fields[0]},{fields[1]}'
                wf.write(fString)
                print(fString)
                dict[fields[1]] = 1
            else:
                pass
