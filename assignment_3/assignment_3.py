import csv
import logging
from typing import List, Tuple
import uuid
import argparse
import numpy as np
import lightgbm as lgb
from numpy.core.fromnumeric import choose
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import lime
import lime.lime_tabular
import pandas as pd
import shap

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
        self.attributes=self.file.readline().strip('\n').split(',')
        self.EOF=False    #if the file has been used up
        self.count=0    #count of the line
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        if self.count==100000:         #make sure the size of the dataset
            return None        
        if self.EOF:            #check if the file is exhaustive
            return None
        result=[]
        for i in range(self.batchsize):
            line=self.file.readline().strip('\n')
            self.count+=1
            if line:
                if self.propagate_prov:
                    if len(line.split())==2:
                        ID='f'+str(self.count)
                    else:
                        ID='r'+str(self.count)
                    result.append(ATuple([int(i) for i in line.split()],operator=self,line=self.count,metadata=ID))
                else:
                    result.append(ATuple([i for i in line.split(',')],operator=self,line=self.count))
            else:
                self.EOF=True
                return result
        return result


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



# Distinct operator
class Distinct(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, input, track_prov=False,
                                                 propagate_prov=False):
        super(Distinct, self).__init__(name="Distinct", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        self.input=input
        self.done=False

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        cur_input=self.input.get_next()
        if cur_input==None:
            return None
        if self.done:
            return None
        result=[]
        for i in range(len(cur_input[0].tuple)):
            result.append(set())
        while not self.done:
            for line in cur_input:
                line=line.tuple
                for i in range(len(line)):
                    if line[i]!='':
                        result[i].add(line[i])
            cur_input=self.input.get_next()
            if cur_input==None:
                self.done=True
        for i in range(len(result)):
            result[i]=list(result[i])
        return result



# Map operator
class Map(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, input, map,track_prov=False,
                                                 propagate_prov=False):
        super(Map, self).__init__(name="Map", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        self.input=input
        self.map=map
        
    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        cur_input=self.input.get_next()
        if cur_input==None:
            return None
        result=[]
        for line in cur_input:
            line=line.tuple
            res=[]
            if line[2]=='':
                line[2]=0
            line[6]=self.map(line[6])          #map the attributes to int value
            line[7]=self.map(line[7])
            line[8]=self.map(line[8])
            line[2]=int(line[2])
            line[3]=int(line[3])
            line[4]=int(line[4])
            line[5]=int(line[5])
            line[9]=int(line[9])
            time=line[1].split()             #split the month day hour minute for time string
            month=int(time[0].split('-')[1])
            day=int(time[0].split('-')[2])
            hour=int(time[1].split(':')[0])
            minute=int(time[1].split(':')[1])
            res.append(month)
            res.append(day)
            res.append(hour)
            res.append(minute)
            res=res+line[2:]
            result.append(res)
        return result
            





if __name__=="__main__":
    logger.info("Assignment #3")
    parser=argparse.ArgumentParser()
    parser.add_argument("--task",type=int,help='The number of task',default=1)
    parser.add_argument("--subtask",type=int,help='The number of subtask',default=1)
    parser.add_argument("--dataset",type=str,help='The path of dataset',default="train.csv")
    args=parser.parse_args()
    def map(origin):           # user defined map funtion
        if origin=='' or len(origin)==0:
            return 0
        elif origin=='a':
            return 1
        elif origin=='b':
            return 2
        elif origin=='c':
            return 3
        elif origin=='d':
            return 4
        elif origin=='e':
            return 5
        elif origin=='f':
            return 6
        elif origin=='Firefox' or origin=='Mozilla' or origin=='Mozilla Firefox':
            return 1
        elif origin=='Safari':
            return 2
        elif origin=='IE' or origin=='InternetExplorer' or origin=='Internet Explorer':
            return 3
        elif origin=='Google Chrome' or origin=='Chrome':
            return 4
        elif origin=='Edge':
            return 5
        elif origin=='Mobile':
            return 1
        elif origin=='Desktop':
            return 2
        elif origin=='Tablet':
            return 3
        else:
            return 0
    #TASK 1: Basic Statistics
    if args.task==1:
        # SUBTASK 1: Missing values for each attribute of dataset
        if args.subtask==1:
            count=[]
            scanned=Scan(args.dataset)
            for i in range(len(scanned.attributes)):
                count.append(0)
            while True:
                cur_input=scanned.get_next()
                if cur_input==None:
                    break
                else:
                    for line in cur_input:
                        line=line.tuple
                        for i in range(len(line)):
                            if(line[i]==''):
                                count[i]+=1
            result=[]
            for i in range(len(count)):
                result.append((scanned.attributes[i],count[i]))
            print(result)


        #SUBTASK 2: Distinct values for attributes countrycode,browserid and devid
        if args.subtask==2:
            count=[]
            scanned=Scan(args.dataset)
            projected=Project(scanned)
            distincted=Distinct(projected)
            result=distincted.get_next()
            count.append(('countrycode',len(result[6])))
            count.append(('browserid',len(result[7])))
            count.append(('devid',len(result[8])))
            print(count)


    #Task 2: ETL Operations
    if args.task==2:
        scanned=Scan(args.dataset)
        mapped=Map(scanned,map)
        print(mapped.get_next())



    #TASK 3: Model training and validation
    if args.task==3:
        scanned=Scan(args.dataset)
        mapped=Map(scanned,map)
        dataset=[]
        target=[]
        while True:
            res=mapped.get_next()
            if res==None:
                break
            for t in res:
                dataset.append(t[:11])
                target.append(t[11])
        dataset=np.array(dataset)
        target=np.array(target)
        X_train,X_test,y_train,y_test=train_test_split(dataset,target,test_size=0.3,random_state=1)
        clf=lgb.LGBMClassifier(objective='binary',silent=False)
        clf.fit(X_train,y_train)
        y_pred=clf.predict(X_test)
        print(y_pred)
        print(classification_report(y_test,y_pred,target_names=['Not Click','Click']))
        
    


        


        
        

           



