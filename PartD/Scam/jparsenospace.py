import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        addr=data["result"][k]["addresses"]
        #print(data["result"][k]["id"], end='')
        for a in addr:
            fString2 = f'{data["result"][k]["id"]},{a}'
            print(fString2)
            #print (",",a,end='')
