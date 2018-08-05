
# A Cloud Guru - Data Generating Script - Section 4 - Data Model 4
# Author - Adrian Cantrill - 2016 - v1
# Aug 2018 problems with 'date' type data being passed to Boto3. Treatment by converting to string during generation RL.

## PREREQS - images/ folder in same directory as this script
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed faker module
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          either no data model loaded, or data model v1 loaded (this will remove v1)
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations
##          Datamodelv2.cfn cloud formation template applied, bucket details available
##              you will be asked to input.

import boto3, random, csv, tqdm, time, botocore
from botocore.exceptions import ClientError
from sys import version_info
from base64 import b64encode
from faker import Factory
from tqdm import trange
from tqdm import tqdm
fake = Factory.create()
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2
import uuid

# Boto init
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')
s3_c = loadmin_session.client('s3')
s3_r = loadmin_session.resource('s3')


#------------------------------------------------------------------------------
def c_table(TableName, **kwargs): # handles the creation of a tabale with error checking
                                # kwargs optionally passes in BOTO3 sessions for multithreading
    try:
        db_r.create_table(**t_conf(TableName))
        print "INFO :: Waiting for Table [%s] to complete..." % TableName
        db_r.Table(TableName).wait_until_exists()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            db_r.Table(TableName).delete()
            print "INFO :: Learning Online %s Table exists, waiting for delete ...." % TableName
            db_r.Table(TableName).wait_until_not_exists()
            c_table(TableName)
        else:
            raise
#------------------------------------------------------------------------------
def t_conf(t): # produces the config for a supplied TableName, used in c_table
    c={}
    c['TableName'] = t


    # table key configuration
    if t in ["lo_students", "lo_teachers", "lo_modules", "lo_courses", "lo_exams", "lo_bookings", ]:
        c['KeySchema'] = [{'AttributeName' : 'id', 'KeyType' : 'HASH'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'id', 'AttributeType' : 'S'}]
    if t == "lo_courseinstances":
        c['KeySchema'] = [{'AttributeName' : 'c_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'c_id', 'AttributeType' : 'S'}, {'AttributeName' : 'ci_id', 'AttributeType' : 'S'}]
    if t == "lo_courseregistration":
        c['KeySchema'] = [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'ci_id', 'AttributeType' : 'S'}, {'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    if t == "lo_coursemakeup":
        c['KeySchema'] = [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'm_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'ci_id', 'AttributeType' : 'S'}, {'AttributeName' : 'm_id', 'AttributeType' : 'S'}]
    if t == "lo_attendance":
        c['KeySchema'] = [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'b_id', 'AttributeType' : 'S'}, {'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    if t == "lo_bookings":
        c['KeySchema'] = [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'b_id', 'AttributeType' : 'S'}]
    # table performance configuration (RCU/WCU)
    c['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 100, 'WriteCapacityUnits' : 100 }
    # table index configuration
    if t== "lo_students": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'countryregion', \
            'KeySchema' : [{'AttributeName' : 'country', 'KeyType' : 'HASH'}, {'AttributeName' : 'state', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'city']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'idsearch', \
            'KeySchema' : [{'AttributeName' : 'govid', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'idscan']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'namesearch', \
            'KeySchema' : [{'AttributeName' : 'last_name', 'KeyType' : 'HASH'}, {'AttributeName' : 'first_name', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['idscan', 'govid', 'email']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['AttributeDefinitions'].append({'AttributeName' : 'country', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'state', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'first_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'last_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'govid', 'AttributeType' : 'S'})
    if t== "lo_teachers": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'countryregion', \
            'KeySchema' : [{'AttributeName' : 'country', 'KeyType' : 'HASH'}, {'AttributeName' : 'state', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'city']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'idsearch', \
            'KeySchema' : [{'AttributeName' : 'govid', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'idscan']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'namesearch', \
            'KeySchema' : [{'AttributeName' : 'last_name', 'KeyType' : 'HASH'}, {'AttributeName' : 'first_name', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['idscan', 'govid', 'email']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['AttributeDefinitions'].append({'AttributeName' : 'country', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'state', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'first_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'last_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'govid', 'AttributeType' : 'S'})
    if t=="lo_modules": # module table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 't_id', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'taughtby', \
            'KeySchema' : [{'AttributeName' : 't_id', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
    if t=="lo_courseinstances": #courseinstanceindexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'year', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'coursechedules', \
            'KeySchema' : [{'AttributeName' : 'year', 'KeyType' : 'HASH'}, {'AttributeName' : 'c_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_courseregistration":
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentcourses', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_coursemakeup":
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'moduleusafe', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_attendance":
        c['GlobalSecondaryIndexes'] = []
        c['LocalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'location', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentscans', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['LocalSecondaryIndexes'].append({\
            'IndexName' : 'attendancelocation', \
            'KeySchema' : [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}})

    if t== "lo_exams": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'void', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'ci_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 's_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 't_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'm_id', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'examvoids', \
            'KeySchema' : [{'AttributeName' : 'void', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 'entrancepic', 'physicalscan', 'location']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'courseexams', \
            'KeySchema' : [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 'm_id', 't_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'teacherperformance', \
            'KeySchema' : [{'AttributeName' : 't_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 's_id', 'm_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentperformance', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 't_id', 'm_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'moduleexams', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'grade', 't_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})

    if t== "lo_bookings": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'type', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'ci_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'location', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'm_id', 'AttributeType' : 'S'})

        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'bookingtype', \
            'KeySchema' : [{'AttributeName' : 'type', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'm_id', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'coursebookings', \
            'KeySchema' : [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'm_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['type', 'location', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'bookinglocation', \
            'KeySchema' : [{'AttributeName' : 'location', 'KeyType' : 'HASH'}, {'AttributeName' : 'type', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'm_id', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'modulebookings', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['type', 'location', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})

    # Stream configuration.. more on this later :)

    return c

#------------------------------------------------------------------------------
def u_table(Table, RCU, WCU): # Update table with RCU and WCU
    print "INFO :: Updating Capacity on table [%s]" % Table
    # create a dict for storing new intended config
    newconf={}
    # retrieve old config from t_conf function
    oldconf = t_conf(Table)
    # every update will need this - set RCU and WCU on the main table to intended values...
    newconf['ProvisionedThroughput'] = { 'ReadCapacityUnits' : RCU, 'WriteCapacityUnits' : WCU}

    #loop through any GSI's, add keys in newconf to adjust these to intended RCU and WCU
    try:
        for gsi in oldconf['GlobalSecondaryIndexes']:
            if 'GlobalSecondaryIndexUpdates' not in newconf:
                newconf['GlobalSecondaryIndexUpdates']=[]
            newconf['GlobalSecondaryIndexUpdates'].append({"Update" : { \
                "IndexName" : gsi['IndexName'], \
                "ProvisionedThroughput" : {"ReadCapacityUnits" : RCU, "WriteCapacityUnits" : WCU} \
                }})
    except:
        print "INFO :: Now GSI found in table [%s]- unchanged" % Table

    # update table and wait for completion
    db_r.Table(Table).update(**newconf)
    time.sleep(5)
    while True:
        if db_r.Table(Table).table_status == 'ACTIVE':
            break
        time.sleep(30)
        print "INFO :: Waiting for update on table [%s]" % Table
#------------------------------------------------------------------------------
def uuidpool(num, tablename): # generate 'num' uuid's, return array
    pool=[]
    for i in trange(num, desc=tablename):
        pool.append(str(uuid.uuid4()))
    return pool
#------------------------------------------------------------------------------
def addstream(Table, StreamConfig):
    print "INFO :: Creating Dynamo DB streams on table [%s]" % Table
    db_r.Table(Table).update(**StreamConfig)
#------------------------------------------------------------------------------
if __name__ == "__main__":

    c_table("lo_students")
    u_table("lo_students",2,2)
