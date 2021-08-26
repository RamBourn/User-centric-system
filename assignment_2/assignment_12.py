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
    def __init__(self, tuple, metadata=None, operator=None,line=None):
        self.tuple = tuple  # ID of a tuple is the combination of a letter of filename and line, e.g. f15, r36
        self.metadata = metadata   
        self.operator = operator
        self.line=line            #the line number of the tuple in data file
        
    
    # overridden the equal method to enable one ATuple to compare with the other Atuple
    def __eq__(self,other):
        if self.tuple==other.tuple:
            return True
        else:
            return False
    # overridden the toString method to make the output of a ATuple more readable
    def __repr__(self) -> str:
        return str(tuple(self.tuple)) 
    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        cur_input=[]
        cur_input.append(self)
        result=self.operator.lineage(cur_input)
        return result

        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self,att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        cur_input=[]
        cur_input.append(self)
        result=self.operator.where(att_index,cur_input)
        return result
        pass

    # Returns the How-provenance of self
    def how(self) -> str:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        return self.metadata
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        string=self.metadata.strip('AVG()')
        string=string.split(',')
        for i in range(len(string)):
            string[i]=string[i].strip('\"\'() ')          #separate two tuples and the value
        keys=[]
        values=[]
        occurrence={}
        for i in range(len(string)):
            keys.append(string[i].split('@')[0].split('*'))
            values.append(int(string[i].split('@')[1]))
        responsible=[]          
        def AVG(arr):                #the aggregation function
            result=sum(arr)/len(arr)
            result=round(result,2)
            return result
        before=AVG(values)            #original output
        for i in range(len(values)):
            responsible.append(0)
            arr=values.copy()
            del(arr[i])
            after=AVG(arr)            #value when some tuple is removed
            if after!=before:
                responsible[i]=1
            else:
                for j in range(len(arr)):
                    tmp=arr.copy()
                    del(tmp[j])
                    after=AVG(tmp)
                    if after!=before:
                        responsible[i]=0.5
                    else:
                        responsible[i]=-1       #set the responsible to -1 if the value is less than 0.5
        
        data=self.lineage()
        data_map={}
        responsible_map={}
        for i in range(len(keys)):
            for j in range(2):
                responsible_map[keys[i][j]]=responsible[i]
        for tuple_ in data:
            data_map[tuple_.metadata]=tuple_.tuple
        result=[]
        res_keys=list(responsible_map.keys())
        for k in res_keys:
            item=[]
            item.append(tuple(data_map[k]))
            item.append(responsible_map[k])
            if responsible_map[k]>=0.5:
                result.append(tuple(item))
        return result

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
        super(Scan, self).__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.filename=filepath
        self.batchsize=1000     # size for every batch
        self.file=open(filepath)
        self.EOF=False    #if the file has been used up
        self.count=0    #count of the line
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE        
        if self.EOF:            #check if the file is exhaustive
            return None
        result=[]
        for i in range(self.batchsize):
            line=self.file.readline()
            self.count+=1
            if line:
                if self.propagate_prov:
                    if len(line.split())==2:
                        ID='f'+str(self.count)
                    else:
                        ID='r'+str(self.count)
                    result.append(ATuple([int(i) for i in line.split()],operator=self,line=self.count,metadata=ID))
                else:
                    result.append(ATuple([int(i) for i in line.split()],operator=self,line=self.count))
            else:
                self.EOF=True
                return result
        return result

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples ==None:
            return None
        else:
            return tuples
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None:
            return None
        else:
            result=[]
            for tuple_ in tuples:
                string=[]
                string.append(self.filename)
                string.append(tuple_.line)
                string.append(tuple(tuple_.tuple))
                string.append(tuple_.tuple[att_index+2])
                result.append(tuple(string))
            return result
        pass

# Equi-join operator
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
        super(Join, self).__init__(name="Join", track_prov=track_prov,
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
        cur_left_input=self.left_input.get_next()
        cur_right_input=self.right_input.get_next()
        if cur_left_input is None or cur_right_input is None:
            return None
        result=[]
        for i in range(len(cur_left_input)):
            for j in range(len(cur_right_input)):
                if cur_left_input[i].tuple[self.left_attr]==cur_right_input[j].tuple[self.right_attr]:
                    if self.propagate_prov:        #join the metadata of two tuples in the appropriate format 
                        result.append(ATuple(cur_left_input[i].tuple+cur_right_input[j].tuple,operator=self\
                        ,line=cur_right_input[j].line,metadata='('+cur_left_input[i].metadata+'*'+cur_right_input[j]\
                            .metadata+'@'+str(cur_right_input[j].tuple[2])+')'))
                    else:
                        result.append(ATuple(cur_left_input[i].tuple+cur_right_input[j].tuple,operator=self\
                        ,line=cur_right_input[j].line))
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples==None or self.track_prov==False:
            return None
        else:
            intermediate_f=[]
            intermediate_r=[]
            for tuple in tuples:
                f=tuple.tuple[:2]
                r=tuple.tuple[2:]
                if self.propagate_prov:
                    string=tuple.how().strip('()').split('*')        #separate tuple and value
                    intermediate_f.append(ATuple(f,metadata=string[0]))
                    intermediate_r.append(ATuple(r,line=tuple.line,metadata=string[1].split('@')[0]))
                else:
                    intermediate_f.append(ATuple(f))
                    intermediate_r.append(ATuple(r,line=tuple.line))
            return self.left_input.lineage(intermediate_f)+self.right_input.lineage(intermediate_r)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None or self.track_prov==False:
            return None
        else:
            intermediate_f=[]
            intermediate_r=[]
            for tuple in tuples:
                f=tuple.tuple[:2]
                r=tuple.tuple[2:]
                intermediate_f.append(ATuple(f))
                intermediate_r.append(ATuple(r,line=tuple.line))
            return self.right_input.where(att_index,intermediate_r)
        pass

# Project operator
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
        super(Project, self).__init__(name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        self.input=input
        self.keep=fields_to_keep
        self.intermediate=[]
        self.intermediate_res=[]
        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        cur_input=self.input.get_next()
        if cur_input is None:
            return None
        result=[]
        if len(self.keep)==0:
            for tuple in cur_input:
                if self.propagate_prov:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line,metadata=tuple.metadata))
                else:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line))
        else:
            for i in range(len(cur_input)):
                n=[]
                for j in range(len(self.keep)):
                    n.append(cur_input[i].tuple[self.keep[j]])
                if self.propagate_prov:
                    result.append(ATuple(n,operator=self,line=cur_input[i].line,metadata=cur_input[i].metadata))
                else:
                    result.append(ATuple(n,operator=self,line=cur_input[i].line))
        if self.track_prov==True:
            self.intermediate+=cur_input
            self.intermediate_res+=result
        return result    
 

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples==None or self.track_prov==False:
            return None
        else:
            if(self.keep==0):
                return self.input.lineage(tuples)
            else:
                res=[]
                for tuple in tuples:
                    for i in range(len(self.intermediate_res)):
                        if self.intermediate_res[i]==tuple: 
                            res.append(self.intermediate[i])
                return self.input.lineage(res)
                
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None or self.track_prov==False:
            return None
        else:
            if(self.keep==0):
                return self.input.lineage(tuples)
            else:
                res=[]
                for tuple in tuples:
                    for i in range(len(self.intermediate_res)):
                        if self.intermediate_res[i]==tuple:
                            res.append(self.intermediate[i])
                return self.input.where(att_index,res)
        pass

# Group-by operator
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
        super(GroupBy, self).__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        self.value=value
        self.agg_fun=agg_fun
        self.done=False
        self.intermediate=None
        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        if self.done:
            return None
        total=[]
        while True:
            cur_input=self.input.get_next()
            if cur_input is None:
                break
            else:
                total+=cur_input
        if self.key is None:
            group=[]
            IDs=[]
            for i in range(len(total)):
                if self.propagate_prov:
                    IDs.append(total[i].metadata)
                    group.append(total[i].tuple[self.value])
                else:
                    group.append(total[i].tuple[self.value])
            if self.track_prov:
                self.intermediate=total
            res=[]
            res.append(self.agg_fun(group))
            result=[]
            ID='AVG'+str(tuple(IDs))
            if self.propagate_prov:
                result.append(ATuple(res,operator=self,metadata=ID))
            else:
                result.append(ATuple(res,operator=self))
            return result
        else:
            keys=set()
            group={}
            IDs={}
            if self.track_prov:
                self.intermediate={}
            def cmp(tuple):
                return tuple.tuple[self.key]
            total.sort(key=cmp)
            for i in range(len(total)):
                keys.add(total[i].tuple[self.key])
            keys=list(keys)
            for i in range(len(keys)):
                group[str(keys[i])]=[]
                if self.propagate_prov:
                    IDs[keys[i]]=[]
                if self.track_prov:
                    self.intermediate[keys[i]]=[]
            for i in range(len(total)):
                group[str(total[i].tuple[self.key])].append(total[i].tuple[self.value])
                if self.propagate_prov:
                    IDs[total[i].tuple[self.key]].append(total[i].metadata)  #aggregate metadatas of each group 
                if self.track_prov:
                    self.intermediate[total[i].tuple[self.key]].append(total[i])
            for i in range(len(keys)): 
                group[str(keys[i])]=self.agg_fun(group[str(keys[i])])
            result=[]
            keys=list(group.keys())
            for i in range(len(keys)):
                m=[]
                m.append(int(keys[i]))
                m.append(group[keys[i]])
                if self.propagate_prov:
                    ID='AVG'+str(tuple(IDs[int(keys[i])]))   #set the metadate of output tuple to the appropriate format
                    result.append(ATuple(m,operator=self,metadata=ID))
                else:
                    result.append(ATuple(m,operator=self))
            return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples==None:
            return None
        else:
            if self.key==None:
                return self.input.lineage(self.intermediate)
            else:
                for tuple in tuples:
                    key=tuple.tuple[0]
                    return self.input.lineage(self.intermediate[key])

        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None:
            return None
        else:
            if self.key==None:
                return self.input.where(att_index,self.intermediate)
            else:
                for tuple in tuples:
                    key=tuple.tuple[0]
                    return self.input.where(att_index,self.intermediate[key])
        pass

# Custom histogram operator
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
        super(Histogram, self).__init__(name="Histogram",
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
            cur_input=self.input.get_next()
            if cur_input is None:
                self.done=True
                break
            else:
                total+=cur_input
        keys=set()
        for i in range(len(total)):
            keys.add(total[i].tuple[self.key])
        keys=list(keys)
        result={}
        for i in range(len(keys)):
            result[keys[i]]=0
        for i in range(len(total)):
            for j in range(len(keys)):
                if total[i].tuple[self.key]==keys[j]:
                    result[keys[j]]+=1
        return result
        pass

# Order by operator
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
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.cmp=comparator
        self.ASC=ASC
        self.done=False
        self.intermediate=None
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        if self.done:
            return None
        total=[]
        result=[]
        cur_input=self.input.get_next()
        total+=cur_input
        if self.track_prov:
            self.intermediate=total
        if self.ASC:
            total.sort(key=self.cmp)
            for tuple in total:
                if self.propagate_prov:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line,metadata=tuple.metadata))
                else:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line))
        else:
            total.sort(key=self.cmp,reverse=True)
            for tuple in total:
                if self.propagate_prov:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line,metadata=tuple.metadata))
                else:
                    result.append(ATuple(tuple.tuple,operator=self,line=tuple.line))
        return result
        # YOUR CODE HERE
        
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples==None:
            return None
        else:
            return self.input.lineage(self.intermediate)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None:
            return None
        else:
            return self.input.where(att_index,self.intermediate)
        pass

# Top-k operator
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
        super(TopK, self).__init__(name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.k=k
        self.intermediate=None
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        cur_input=self.input.get_next()
        if cur_input is None:
            return None
        res=cur_input[:self.k]
        result=[]
        for tuple in res:
            if self.propagate_prov:
                result.append(ATuple(tuple.tuple,operator=self,line=tuple.line,metadata=tuple.metadata))
            else:
                result.append(ATuple(tuple.tuple,operator=self,line=tuple.line))
        if self.track_prov:
            self.intermediate=result
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        if tuples ==None:
            return None
        else:
            return self.input.lineage(self.intermediate)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples == None:
            return None
        else:
            return self.input.where(att_index,self.intermediate)
        pass

# Filter operator
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
        super(Select, self).__init__(name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.predicate=predicate
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        cur_input=self.input.get_next()
        if cur_input is None:
            return None
        result=[]
        for i in range(len(cur_input)):
            if self.predicate(cur_input[i]):
                if self.propagate_prov:
                    result.append(ATuple(cur_input[i].tuple,operator=self,line=cur_input[i].line\
                        ,metadata=cur_input[i].metadata))
                else:
                    result.append(ATuple(cur_input[i].tuple,operator=self,line=cur_input[i].line))
        return result

    def lineage(self, tuples):
        if tuples==None or self.track_prov==False:
            return None
        else:
            return self.input.lineage(tuples)
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        
        pass
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        if tuples==None or self.track_prov==False:
            return None
        else:
            return self.input.where(att_index,tuples)
        pass


if __name__ == "__main__":
    logger.info("Assignment #1")
    parser=argparse.ArgumentParser()
    parser.add_argument("--task",type=int,help='The number of task',default=5)
    parser.add_argument("--friends",type=str,help='The path of friends.txt',default="data_friends_test.txt")
    parser.add_argument("--ratings",type=str,help='The path of ratings.txt',default="data_movie_ratings_test.txt")
    parser.add_argument("--uid",type=int,help='The ID of user',default=0)
    parser.add_argument("--mid",type=int,help="The ID of movie",default=0)
    parser.add_argument("--att_index",type=int,help='The index of attribute',default=0)
    args=parser.parse_args()
    def funcF(tuple):              #filter of friends to filter out tuples that satisfy the predicate
        if int(tuple.tuple[0])==args.uid:
            return True
        else :
            return False
    def funcR(tuple):              #filter of movie_ratings to filter out tuples that satisfy predicate
        if int(tuple.tuple[1])==args.mid:
            return True
        else :
            return False
    def cmp(tuple):                #comparator to sort the tuples
        return tuple.tuple[1]
    def AVG(arr):                #the average aggregation function
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
    if args.task==1:             #task 1
        F=Scan(args.friends)         #scanner of friends.txt
        R=Scan(args.ratings)         #scanner of movie_rating.txt
        selectedF=Select(F,funcF)
        selectedR=Select(R,funcR)
        joined=Join(selectedF,selectedR,1,0)
        grouped=GroupBy(joined,None,4,AVG)
        print(grouped.get_next())

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
    elif args.task==2:           #task 2
        F=Scan(args.friends)         #scanner of friends.txt
        R=Scan(args.ratings)         #scanner of movie_rating.txt
        selectedF=Select(F,funcF)
        joined=Join(selectedF,R,1,0)
        projected=Project(joined,[3,4])
        grouped=GroupBy(projected,0,1,AVG)
        ordered=OrderBy(grouped,comparator=cmp,ASC=False)
        limited=TopK(ordered,1)
        result=Project(limited,[0])
        print(result.get_next())

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    elif args.task==3:           #task 3
        F=Scan(args.friends)         #scanner of friends.txt
        R=Scan(args.ratings)         #scanner of movie_rating.txt
        selectedF=Select(F,funcF)
        selectedR=Select(R,funcR)
        joined=Join(selectedF,selectedR,1,0)
        projected=Project(joined,[2,4])
        histogtamed=Histogram(projected,1)
        print(histogtamed.get_next())

    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE
    if args.task==5:
        F=Scan(args.friends,track_prov=True)         #scanner of friends.txt which can be trackd backward
        R=Scan(args.ratings,track_prov=True)         #scanner of movie_rating.txt which can be traced backward
        selectedF=Select(F,funcF,track_prov=True)
        joined=Join(selectedF,R,1,0,track_prov=True)
        projected=Project(joined,[3,4],track_prov=True)
        grouped=GroupBy(projected,0,1,AVG,track_prov=True)
        ordered=OrderBy(grouped,comparator=cmp,ASC=False,track_prov=True)
        limited=TopK(ordered,1,track_prov=True)
        result=Project(limited,[0],track_prov=True)
        print(limited.get_next()[0].lineage())        #output the lineage
        
        #print(projected.lineage(grouped.get_next()[0].operator.intermediate[5]))


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE
    elif args.task==6:
        F=Scan(args.friends,track_prov=True)         #scanner of friends.txt which can be trackd backward
        R=Scan(args.ratings,track_prov=True)         #scanner of movie_rating.txt which can be traced backward
        selectedF=Select(F,funcF,track_prov=True)
        selectedR=Select(R,funcR,track_prov=True)
        joined=Join(selectedF,selectedR,1,0,track_prov=True)
        grouped=GroupBy(joined,None,4,AVG,track_prov=True)
        print(grouped.get_next()[0].where(args.att_index))        #output where provenance of specific attribute
    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE
    elif args.task==7:
        F=Scan(args.friends,propagate_prov=True)         #scanner of friends.txt which can propagate
        R=Scan(args.ratings,propagate_prov=True)         #scanner of movie_rating.txt which can propagete
        selectedF=Select(F,funcF,propagate_prov=True)
        joined=Join(selectedF,R,1,0,propagate_prov=True)
        projected=Project(joined,[3,4],propagate_prov=True)
        grouped=GroupBy(projected,0,1,AVG,propagate_prov=True)
        ordered=OrderBy(grouped,comparator=cmp,ASC=False,propagate_prov=True)
        limited=TopK(ordered,1,propagate_prov=True)
        result=Project(limited,[0],propagate_prov=True)
        print(result.get_next()[0].how())               # output how provenance
    # TASK 4: Retrieve most responsible tuples for movie recommendation
    
    # YOUR CODE HERE
    elif args.task==8:
        F=Scan(args.friends,track_prov=True,propagate_prov=True)         #scanner of friends.txt which can propagate and can be tracked backward
        R=Scan(args.ratings,track_prov=True,propagate_prov=True)         #scanner of movie_rating.txt which can propagete and can be trace backward
        selectedF=Select(F,funcF,track_prov=True,propagate_prov=True)
        joined=Join(selectedF,R,1,0,track_prov=True,propagate_prov=True)
        projected=Project(joined,[3,4],track_prov=True,propagate_prov=True)
        grouped=GroupBy(projected,0,1,AVG,track_prov=True,propagate_prov=True)
        ordered=OrderBy(grouped,comparator=cmp,ASC=False,track_prov=True,propagate_prov=True)
        limited=TopK(ordered,1,track_prov=True,propagate_prov=True)
        result=Project(limited,[0],track_prov=True,propagate_prov=True)
        print(result.get_next()[0].responsible_inputs())          #output the list of tuples and their responsibility

