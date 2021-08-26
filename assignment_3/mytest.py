from assignment_3 import *

#TASK 1: Basic Statistics

# SUBTASK 1: Missing values for each attribute of dataset
def test_task1_subtask1():
    count=[]
    scanned=Scan('data_for_test.csv')
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
    assert result==[('ID',0),('datetime',0),('siteid',0),('offerid',0),('category',0),('merchant',0),
    ('countrycode',0),('browserid',0),('devid',3),('click',0)]

#SUBTASK 2: Distinct values for attributes countrycode,browserid and devid
def test_task1_subtask2():    
    count=[]
    scanned=Scan('data_for_test.csv')
    projected=Project(scanned)
    distincted=Distinct(projected)
    result=distincted.get_next()
    count.append(('countrycode',len(result[6])))
    count.append(('browserid',len(result[7])))
    count.append(('devid',len(result[8])))
    assert count==[('countrycode',5),('browserid',4),('devid',3)]


#Task 2: ETL Operations
def test_task2():
    def map(origin):
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
    scanned=Scan('data_for_test.csv')
    mapped=Map(scanned,map)
    assert mapped.get_next()==[[1,14,9,42,4709696,887235,17714,20301556,5,1,0,0],
        [1,18,17,50,5189467,178235,21407,9434818,2,1,2,0],
        [1,11,12,46,98480,518539,25085,2050923,1,5,0,0],
        [1,17,10,18,8896401,390352,40339,72089744,3,1,1,0],
        [1,14,16,2,5635120,472937,12052,39507200,4,1,2,0], 
        [1,14,12,8,2729292,961176,33638,47079934,5,4,1,0], 
        [1,12,13,7,7007059,664666,68847,58604466,2,5,0,0],
        [1,13,5,58,7295565,144797,33638,23981625,2,1,1,0],
        [1,18,13,0,2116058,376073,15912,30860214,3,5,3,0],
        [1,14,12,38,5329483,952097,89680,74363610,1,5,3,0]]



#TASK 3: Model training and validation
def test_task3():
    def map(origin):
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
    scanned=Scan('train.csv')
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
    scan_test=Scan('data_for_test.csv')
    map_test=Map(scan_test,map)
    data_test=np.array(map_test.get_next())
    x_test=data_test[:,:11]
    y_test=data_test[:,11]
    y_pred=clf.predict(x_test)
    assert list(y_pred)==[0,0,0,0,0,0,0,0,0,0]
    


