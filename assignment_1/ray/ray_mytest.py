from assignment_12_ray import *

ray.init()
def test_Scan():
    F=Scan.remote("tiny_data_friends.txt")
    assert ray.get(F.get_next.remote())==[[0,1],[0,2],[1,0],[1,2],[2,0],[2,1]]

def test_Project():
    R=Scan.remote("tiny_data_ratings.txt")
    projected=Project.remote(R,[1,2])
    assert ray.get(projected.get_next.remote())==[[0,3],[1,5],[2,1],[0,4],[1,1],[2,2],[0,5],[1,0],[2,3]]

def test_Select():
    F=Scan.remote("tiny_data_friends.txt")
    def func(arr):
        if arr[0]==1:
            return True
        else:
            return False
    selected=Select.remote(F,func)
    assert ray.get(selected.get_next.remote())==[[1,0],[1,2]]

def test_Join():
    F=Scan.remote("tiny_data_friends.txt")
    R=Scan.remote("tiny_data_ratings.txt")
    joined=Join.remote(F,R,1,0)
    assert ray.get(joined.get_next.remote())==[[0,1,1,0,4],[0,1,1,1,1],[0,1,1,2,2],[0,2,2,0,5],[0,2,2,1,0],[0,2,2,2,3],[1,0,0,0,3],[1,0,0,1,5]\
        ,[1,0,0,2,1],[1,2,2,0,5],[1,2,2,1,0],[1,2,2,2,3],[2,0,0,0,3],[2,0,0,1,5],[2,0,0,2,1],[2,1,1,0,4],[2,1,1,1,1],[2,1,1,2,2]]

def test_OrderBy():
    R=Scan.remote("tiny_data_ratings.txt")
    def cmp(arr):
        return arr[2]
    ordered=OrderBy.remote(R,cmp,ASC=False)
    assert ray.get(ordered.get_next.remote())==[[0, 1, 5], [2, 0, 5], [1, 0, 4], [0, 0, 3], [2, 2, 3], [1, 2, 2], [0, 2, 1], [1, 1, 1], [2, 1, 0]]

def test_GroupBy():
    R=Scan.remote("tiny_data_ratings.txt")
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    grouped=GroupBy.remote(R,1,2,AVG)
    assert ray.get(grouped.get_next.remote())==[[0, 4.0], [1, 2.0], [2, 2.0]]

def test_TopK():
    F=Scan.remote("tiny_data_friends.txt")
    top3=TopK.remote(F,3)
    assert ray.get(top3.get_next.remote())==[[0,1],[0,2],[1,0]]

def test_task1():
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    def funcF(arr):
        if int(arr[0])==0:
            return True
        else :
            return False
    def funcR(arr):
        if int(arr[1])==1:
            return True
        else :
            return False
    F=Scan.remote("tiny_data_friends.txt")
    R=Scan.remote("tiny_data_ratings.txt")
    selectedF=Select.remote(F,funcF)
    selectedR=Select.remote(R,funcR)
    joined=Join.remote(selectedF,selectedR,1,0)
    grouped=GroupBy.remote(joined,None,4,AVG)
    assert ray.get(grouped.get_next.remote())==0.5

def test_task2():
    F=Scan.remote("tiny_data_friends.txt")
    R=Scan.remote("tiny_data_ratings.txt")
    def AVG(arr):
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    def funcF(arr):
        if int(arr[0])==1:
            return True
        else :
            return False
    def cmp(arr):
        return arr[1]
    selectedF=Select.remote(F,funcF)
    joined=Join.remote(selectedF,R,1,0)
    projected=Project.remote(joined,[3,4])
    grouped=GroupBy.remote(projected,0,1,AVG)
    ordered=OrderBy.remote(grouped,comparator=cmp,ASC=False)
    limited=TopK.remote(ordered,1)
    result=Project.remote(limited,[0])
    assert ray.get(result.get_next.remote())==[[0]]


def test_task3():
    F=Scan.remote("tiny_data_friends.txt")
    R=Scan.remote("tiny_data_ratings.txt")
    def funcF(arr):
        if int(arr[0])==0:
            return True
        else :
            return False
    def funcR(arr):
        if int(arr[1])==1:
            return True
        else :
            return False
    selectedF=Select.remote(F,funcF)
    selectedR=Select.remote(R,funcR)
    joined=Join.remote(selectedF,selectedR,1,0)
    projected=Project.remote(joined,[2,4])
    histogtamed=Histogram.remote(projected,1)
    assert ray.get(histogtamed.get_next.remote())=={0:1,1:1}

