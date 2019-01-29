
cpdef finder(str filename):
    count = 0
    with open(filename) as file:
        for line in file:
            if "MEDICAL" in line.split("\t")[8]:
                count+=1
    return count
