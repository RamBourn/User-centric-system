from assignment_12 import *

def test_Scan():
    F=Scan("tiny_data_friends.txt")
    assert F.get_next()==[ATuple([0,1]),ATuple([0,2]),ATuple([1,0]),ATuple([1,2]),ATuple([2,0]),ATuple([2,1])]

def test_Project():
    R=Scan("tiny_data_ratings.txt")
    projected=Project(R,[1,2])
    assert projected.get_next()==[ATuple([0,3]),ATuple([1,5]),ATuple([2,1]),ATuple([0,4]),ATuple([1,1])\
    ,ATuple([2,2]),ATuple([0,5]),ATuple([1,0]),ATuple([2,3])]

def test_Select():
    F=Scan("tiny_data_friends.txt")
    def func(tuple):
        if tuple.tuple[0]==1:
            return True
        else:
            return False
    selected=Select(F,func)
    assert selected.get_next()==[ATuple([1,0]),ATuple([1,2])]

def test_Join():
    F=Scan("tiny_data_friends.txt")
    R=Scan("tiny_data_ratings.txt")
    joined=Join(F,R,1,0)
    assert joined.get_next()==[ATuple([0,1,1,0,4]),ATuple([0,1,1,1,1]),ATuple([0,1,1,2,2]),ATuple([0,2,2,0,5])\
        ,ATuple([0,2,2,1,0]),ATuple([0,2,2,2,3]),ATuple([1,0,0,0,3]),ATuple([1,0,0,1,5]),ATuple([1,0,0,2,1])\
            ,ATuple([1,2,2,0,5]),ATuple([1,2,2,1,0]),ATuple([1,2,2,2,3]),ATuple([2,0,0,0,3]),ATuple([2,0,0,1,5])\
                ,ATuple([2,0,0,2,1]),ATuple([2,1,1,0,4]),ATuple([2,1,1,1,1]),ATuple([2,1,1,2,2])]

def test_OrderBy():
    R=Scan("tiny_data_ratings.txt")
    def cmp(tuple):
        return tuple.tuple[2]
    ordered=OrderBy(R,cmp,ASC=False)
    assert ordered.get_next()==[ATuple([0,1,5]),ATuple([2,0,5]),ATuple([1,0,4]),ATuple([0,0,3]),ATuple([2,2,3])\
        ,ATuple([1,2,2]),ATuple([0,2,1]),ATuple([1,1,1]),ATuple([2,1,0])]

def test_GroupBy():
    R=Scan("tiny_data_ratings.txt")
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    grouped=GroupBy(R,1,2,AVG)
    assert grouped.get_next()==[ATuple([0,4.0]),ATuple([1,2.0]),ATuple([2,2.0])]

def test_TopK():
    F=Scan("tiny_data_friends.txt")
    top3=TopK(F,3)
    assert top3.get_next()==[ATuple([0,1]),ATuple([0,2]),ATuple([1,0])]

def test_task1():
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    def funcF(tuple):
        if int(tuple.tuple[0])==0:
            return True
        else :
            return False
    def funcR(tuple):
        if int(tuple.tuple[1])==1:
            return True
        else :
            return False
    F=Scan("tiny_data_friends.txt")
    R=Scan("tiny_data_ratings.txt")
    selectedF=Select(F,funcF)
    selectedR=Select(R,funcR)
    joined=Join(selectedF,selectedR,1,0)
    grouped=GroupBy(joined,None,4,AVG)
    assert grouped.get_next()==[ATuple([0.5])]

def test_task2():
    F=Scan("tiny_data_friends.txt")
    R=Scan("tiny_data_ratings.txt")
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    def funcF(tuple):
        if int(tuple.tuple[0])==1:
            return True
        else :
            return False
    def cmp(tuple):
        return tuple.tuple[1]
    selectedF=Select(F,funcF)
    joined=Join(selectedF,R,1,0)
    projected=Project(joined,[3,4])
    grouped=GroupBy(projected,0,1,AVG)
    ordered=OrderBy(grouped,comparator=cmp,ASC=False)
    limited=TopK(ordered,1)
    result=Project(limited,[0])
    assert result.get_next()==[ATuple([0])]


def test_task3():
    F=Scan("tiny_data_friends.txt")
    R=Scan("tiny_data_ratings.txt")
    def funcF(tuple):
        if int(tuple.tuple[0])==0:
            return True
        else :
            return False
    def funcR(tuple):
        if int(tuple.tuple[1])==1:
            return True
        else :
            return False
    selectedF=Select(F,funcF)
    selectedR=Select(R,funcR)
    joined=Join(selectedF,selectedR,1,0)
    projected=Project(joined,[2,4])
    histogtamed=Histogram(projected,1)
    assert histogtamed.get_next()=={0:1,1:1}

