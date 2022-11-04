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
    return dummyrdd

def task5(postsRDD):
    return dummyrdd


def task6(amazonInputRDD):
    return dummyrdd

def task7(amazonInputRDD):
    return dummyrdd

def task8(amazonInputRDD):
    return dummyrdd

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
