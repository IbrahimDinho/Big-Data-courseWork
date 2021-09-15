with open("scams.csv" ,'r+') as fp:
    with open('scamsreal2.csv', 'w') as wf:
        for line in fp:
            line_new = line.replace(" ", "")
            wf.write(line_new)
            print(line_new)