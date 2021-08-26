from assignment_4 import *

def test_task2():
    ray.init()                       #initializa the ray
    def funcF(tuple):              #filter of friends to filter out tuples that satisfy the predicate
        if int(tuple.tuple[0])==0:
            return True
        else :
            return False
    def funcR(tuple):              #filter of movie_ratings to filter out tuples that satisfy predicate
        if int(tuple.tuple[1])==1:
            return True
        else :
            return False
    def cmp(tuple):                #comparator to sort the tuples
        return tuple.tuple[1]
    def AVG(arr):                #the average aggregation function
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    with init_tracer('Recommendation').start_span('Recommendation') as recommendation:  
            F=Scan.remote('tiny_data_friends.txt',track_prov=True)         #scanner of friends.txt
            R=Scan.remote('tiny_data_ratings.txt',track_prov=True)         #scanner of movie_rating.txt
            selectedF=Select.remote(F,funcF,track_prov=True)
            joined=Join.remote(selectedF,R,1,0,track_prov=True)
            projected=Project.remote(joined,[3,4],track_prov=True)
            grouped=GroupBy.remote(projected,0,1,AVG,track_prov=True)
            ordered=OrderBy.remote(grouped,comparator=cmp,ASC=False,track_prov=True)
            limited=TopK.remote(ordered,1,track_prov=True)
            assert ray.get(limited.get_next.remote(recommendation.context))==[ATuple((0, 4.5))]


def test_task3():
    ray.init()                       #initializa the ray
    def funcF(tuple):              #filter of friends to filter out tuples that satisfy the predicate
        if int(tuple.tuple[0])==0:
            return True
        else :
            return False
    def funcR(tuple):              #filter of movie_ratings to filter out tuples that satisfy predicate
        if int(tuple.tuple[1])==1:
            return True
        else :
            return False
    def cmp(tuple):                #comparator to sort the tuples
        return tuple.tuple[1]
    def AVG(arr):                #the average aggregation function
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result
    with init_tracer('Recommendation').start_span('Recommendation') as recommendation:  
        with init_tracer('Lineage').start_span('Lineage') as lineage: 
            F=Scan.remote('tiny_data_friends.txt',track_prov=True)         #scanner of friends.txt
            R=Scan.remote('tiny_data_ratings.txt',track_prov=True)         #scanner of movie_rating.txt
            selectedF=Select.remote(F,funcF,track_prov=True)
            joined=Join.remote(selectedF,R,1,0,track_prov=True)
            projected=Project.remote(joined,[3,4],track_prov=True)
            grouped=GroupBy.remote(projected,0,1,AVG,track_prov=True)
            ordered=OrderBy.remote(grouped,comparator=cmp,ASC=False,track_prov=True)
            limited=TopK.remote(ordered,1,track_prov=True)
            result=ray.get(limited.get_next.remote(recommendation.context))[0]
            print(ray.get(result.lineage.remote(result,lineage.context)))        #output the lineage
            #assert ray.get(limited.get_next.remote(recommendation.context))==[ATuple((0, 4.5))]

test_task3()
