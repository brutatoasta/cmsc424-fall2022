import json
import re

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
    global dummyrdd
    dummyrdd = rdd

def task1(postsRDD):
    res = postsRDD.filter(
        lambda x: x.get("tags")!= None).filter(
        lambda x: "postgresql-9.4" in x.get("tags")).map(
        lambda x: (x.get("id"), x.get("title"), x.get("tags"))
    )
    return res 
 

def task2(postsRDD):
    def task2FlatMapper(dic):
        res = []
        tags = dic.get("tags").replace("<", "").replace(">", " ").split(" ")
        tags.pop()
        for i in tags:
            res.append( (dic.get("id"), i)
            )
        return res
    return postsRDD.filter(
        lambda x: x.get("tags")!= None).flatMap(
        task2FlatMapper
    )

def task3(postsRDD):
    def task3MapA(dic):
        year = dic["creationdate"][:4]
        tags = dic.get("tags").replace("<", "").replace(">", " ").split(" ")
        tags.pop()
        res = (year, set(tags))
        return res
    def task3MapB(tu):
        x = tu[1]
        x = list(x)
        x.sort()
        return (tu[0], x[:5])
    return postsRDD.filter(
        lambda x: x.get("tags")!= None).map(
        task3MapA
    ).reduceByKey(lambda v1, v2: v1 | v2).map(task3MapB)




def task4(usersRDD, postsRDD):
    def userTup(dic):
        res = (dic["id"] , dic["displayname"])
        return res
    def postsTup(dic):
        res = (dic["owneruserid"], (dic["id"], dic["title"]))
        return res
    def combTup(tu):
        a = tu[0]
        b= tu[1][0]
        c = tu[1][1][0]
        d = tu[1][1][1]
        return (a, b, c, d)

    rdd1 = usersRDD.map(userTup)
    rdd2 = postsRDD.map(postsTup)
    rdd3 = rdd1.join(rdd2)
    return rdd3.map(combTup)


def task5(postsRDD):
    return dummyrdd
 
def task6(amazonInputRDD):
    def task6mapper(line):
        words = line.replace("user", "").replace("product", "").split(" ")
        return (int(words[0]), int(words[1]), float(words[2]))

    return amazonInputRDD.map(task6mapper)



def task7(amazonInputRDD):
    def task7mapA(line):
        words = line.split()
        return (words[0],  float(words[2]))
    aTuple = (0,0)
    cleaned = amazonInputRDD.map(task7mapA)
    rdd1 = cleaned.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                       lambda a,b: (a[0] + b[0], a[1] + b[1]))
    return rdd1.mapValues(lambda v: v[0]/v[1])

# First lambda expression for Within-Partition Reduction Step::
#    a: is a TUPLE that holds: (runningSum, runningCount).
#    b: is a SCALAR that holds the next Value

#    Second lambda expression for Cross-Partition Reduction Step::
#    a: is a TUPLE that holds: (runningSum, runningCount).
#    b: is a TUPLE that holds: (nextPartitionsSum, nextPartitionsCount).
# https://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth



# count the number of unique tuples (product 181, 4.0) 
def task8(amazonInputRDD):
    def task8mapA(line):
        words = line.split()

        return ((words[1],  float(words[2])) ,0)
    cleaned = amazonInputRDD.map(task8mapA)
    rdd1 = cleaned.groupByKey()\
    .mapValues(lambda vals: len(vals))\
    .sortByKey()
    # rdd1 ((name, rating), count)
    # rdd2, mode (name, count) 
    # rdd3 ((name, count), rating)
    # rdd4 ((name, count), 0)
    
    # get mode
    rdd2 = rdd1.map(lambda x: (x[0][0], x[1])) # contains the count of every key
    mode = rdd2.reduceByKey(max)
    rdd3 = rdd1.map(lambda x: ((x[0][0], x[1]), x[0][1]))
    # rdd3 = rdd1.map(lambda x: (x[0][0], x[1]), x[0][1])
    rdd4 = mode.map(lambda x: (x, 0))
    res = rdd3.join(rdd4) # join on same count
    res = res.map(lambda x: (x[0][0], x[1][0]) ).sortByKey()
    return res

def task9(logsRDD):
    return dummyrdd

def task10_flatmap(line):
    return line

def task11(playRDD):
    return dummyrdd

def task12(nobelRDD):
    return dummyrdd

def task13(logsRDD, l):
    return dummyrdd

def task14(logsRDD, day1, day2):
    return dummyrdd

def task15(bipartiteGraphRDD):
    return dummyrdd

def task16(nobelRDD):
    return dummyrdd
