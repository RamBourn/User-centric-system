from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import csv
import logging
from typing import List, Tuple
import uuid
import argparse
import ray

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage() -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how() -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs() -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    def __init__(self, id=None, name=None, track_prov=False,
                                           propagate_prov=False):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self):
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

# Scan operator
@ray.remote
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """

    # Initializes scan operator
    def __init__(self, filepath, filter=None, track_prov=False,
                                              propagate_prov=False):
        Operator.__init__(self,name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.batchsize=1000        # size for every batch
        self.file=open(filepath)
        self.EOF=False
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        if self.EOF:           #check if the file is exhaustive 
            return None
        result=[]
        for i in range(self.batchsize):
            line=self.file.readline()
            if line:
                result.append([int(i) for i in line.split()])
            else:
                self.EOF=True
                return result
        return result

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Equi-join operator
@ray.remote
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_input (Operator): A handle to the left input.
        right_input (Operator): A handle to the left input.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes join operator
    def __init__(self, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        Operator.__init__(self,name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.left_input=left_input
        self.right_input=right_input
        self.left_attr=left_join_attribute
        self.right_attr=right_join_attribute
        pass

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        cur_left_input=ray.get(self.left_input.get_next.remote())
        cur_right_input=ray.get(self.right_input.get_next.remote())
        if cur_left_input is None or cur_right_input is None:
            return None
        result=[]
        for i in range(len(cur_left_input)):
            for j in range(len(cur_right_input)):
                if cur_left_input[i][self.left_attr]==cur_right_input[j][self.right_attr]:
                    result.append(cur_left_input[i]+cur_right_input[j])
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Project operator
@ray.remote
class Project(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, input, fields_to_keep=[], track_prov=False,
                                                 propagate_prov=False):
        Operator.__init__(self,name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        self.input=input
        self.keep=fields_to_keep
        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        cur_input=ray.get(self.input.get_next.remote())
        if cur_input is None:
            return None
        result=[]
        if len(self.keep)==0:
            return cur_input
        else:
            for i in range(len(cur_input)):
                n=[]
                for j in range(len(self.keep)):
                    n.append(cur_input[i][self.keep[j]])
                result.append(n)
            return result    
 

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Group-by operator
@ray.remote
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes average operator
    def __init__(self, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        Operator.__init__(self,name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        self.value=value
        self.agg_fun=agg_fun
        self.done=False
        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        if self.done:
            return None
        total=[]
        while True:
            cur_input=ray.get(self.input.get_next.remote())
            if cur_input is None:
                break
            else:
                total+=cur_input
        if self.key is None:
            group=[]
            for i in range(len(total)):
                group.append(total[i][self.value])
            result=self.agg_fun(group)
            return result
        else:
            keys=set()
            group={}
            def cmp(arr):
                return arr[self.key]
            total.sort(key=cmp)
            for i in range(len(total)):
                keys.add(total[i][self.key])
            keys=list(keys)
            for i in range(len(keys)):
                group[str(keys[i])]=[]
            for i in range(len(total)):
                group[str(total[i][self.key])].append(total[i][self.value])
            for i in range(len(keys)):
                group[str(keys[i])]=self.agg_fun(group[str(keys[i])])
            result=[]
            keys=list(group.keys())
            for i in range(len(keys)):
                m=[]
                m.append(int(keys[i]))
                m.append(group[keys[i]])
                result.append(m)
            return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Custom histogram operator
@ray.remote
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes histogram operator
    def __init__(self, input, key=0, track_prov=False, propagate_prov=False):
        Operator.__init__(self,name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        self.done=False
        pass

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        if self.done:
            return None
        total=[]
        while True:
            cur_input=ray.get(self.input.get_next.remote())
            if cur_input is None:
                self.done=True
                break
            else:
                total+=cur_input
        keys=set()
        for i in range(len(total)):
            keys.add(total[i][self.key])
        keys=list(keys)
        result={}
        for i in range(len(keys)):
            result[keys[i]]=0
        for i in range(len(total)):
            for j in range(len(keys)):
                if total[i][self.key]==keys[j]:
                    result[keys[j]]+=1
        return result
        pass

# Order by operator
@ray.remote
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        input (Operator): A handle to the input
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes order-by operator
    def __init__(self, input, comparator, ASC=True, track_prov=False,
                                                    propagate_prov=False):
        Operator.__init__(self,name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.cmp=comparator
        self.ASC=ASC
        self.done=False
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        if self.done:
            return None
        total=[]
        #while True:
        cur_input=ray.get(self.input.get_next.remote())
            #if cur_input is None:
            #    self.done=True
            #    break
            #else:
        total+=cur_input
        if self.ASC:
            total.sort(key=self.cmp)
        else:
            total.sort(key=self.cmp,reverse=True)
        return total
        # YOUR CODE HERE
        
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Top-k operator
@ray.remote
class TopK(Operator):
    """TopK operator.

    Attributes:
        input (Operator): A handle to the input.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes top-k operator
    def __init__(self, input, k=None, track_prov=False, propagate_prov=False):
        Operator.__init__(self,name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.k=k
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        cur_input=ray.get(self.input.get_next.remote())
        if cur_input is None:
            return None
        result=cur_input[:self.k]
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Filter operator
@ray.remote
class Select(Operator):
    """Select operator.

    Attributes:
        input (Operator): A handle to the input.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes select operator
    def __init__(self, input, predicate=None, track_prov=False,
                                         propagate_prov=False):
        Operator.__init__(self,name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.predicate=predicate
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        cur_input=ray.get(self.input.get_next.remote())
        if cur_input is None:
            return None
        result=[]
        for i in range(len(cur_input)):
            if self.predicate(cur_input[i]):
                result.append(cur_input[i])
        return result
        


if __name__ == "__main__":
    logger.info("Assignment #1")
    parser=argparse.ArgumentParser()
    parser.add_argument("--task",type=int,help='The number of task',default=2)
    parser.add_argument("--friends",type=str,help='The path of friends.txt',default="data_friends_test.txt")
    parser.add_argument("--ratings",type=str,help='The path of ratings.txt',default="data_movie_ratings_test.txt")
    parser.add_argument("--uid",type=int,help='The ID of user',default=0)
    parser.add_argument("--mid",type=int,help="The ID of movie",default=0)
    args=parser.parse_args()
    ray.init()
    F=Scan.remote(args.friends)     #scanner of friends.txt
    R=Scan.remote(args.ratings)     #scanner of movie_rating.txt
    def funcF(arr):                 #filter of friends to filter out tuples that satisfy the predicate
        if int(arr[0])==args.uid:
            return True
        else :
            return False
    def funcR(arr):                 #filter of movie_ratings to filter out tuples that satisfy predicate
        if int(arr[1])==args.mid:
            return True
        else :
            return False
    def cmp(arr):                   #comparator to sort the tuples
        return arr[1]
    def AVG(arr):                   #the average aggregation function
        result=sum(arr)/len(arr)
        result=round(result,2)
        return result

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    if args.task==1:                #task 1
        selectedF=Select.remote(F,funcF)
        selectedR=Select.remote(R,funcR)
        joined=Join.remote(selectedF,selectedR,1,0)
        grouped=GroupBy.remote(joined,None,4,AVG)
        print(ray.get(grouped.get_next.remote()))

    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    # YOUR CODE HERE
    elif args.task==2:              #task 2
        selectedF=Select.remote(F,funcF)
        joined=Join.remote(selectedF,R,1,0)
        projected=Project.remote(joined,[3,4])
        grouped=GroupBy.remote(projected,0,1,AVG)
        ordered=OrderBy.remote(grouped,comparator=cmp,ASC=False)
        limited=TopK.remote(ordered,1)
        #result=Project.remote(limited,[0])
        print(ray.get(limited.get_next.remote()))

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    elif args.task==3:              #task 3
        selectedF=Select.remote(F,funcF)
        selectedR=Select.remote(R,funcR)
        joined=Join.remote(selectedF,selectedR,1,0)
        projected=Project.remote(joined,[2,4])
        histogtamed=Histogram.remote(projected,1)
        print(ray.get(histogtamed.get_next.remote()))

    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
