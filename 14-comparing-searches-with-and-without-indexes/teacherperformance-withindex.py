# Find students within a country - using a GSI index
# A Cloud Guru - Dynamo DB Course\
# USES V3 DATA MODEL

import boto3, botocore, tqdm, random, time
from time import gmtime, strftime
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from sys import version_info
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2

lesson = '4-014' ## UDPATE ME
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')

## PREREQS - images/ folder in same directory as this scripts
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed faker module
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          Data Model v2 loaded.
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations

if __name__ == "__main__":
    teacher_id=""
    capacityconsumed = 0
    scannedcount = 0


    while True:
        if py3:
            teacher_id = str(input('Enter a teacher_id: '))
        else:
            teacher_id = str(raw_input('Enter a teacher_id: '))
        if teacher_id !="":
            break
    ## END WHILE LOOP
    starttime = time.time()
    response=db_r.Table("lo_exams").query(\
        ReturnConsumedCapacity='TOTAL', \
        ExpressionAttributeValues={':teacherid' : teacher_id}, \
        KeyConditionExpression="t_id = :teacherid", \
        IndexName='teacherperformance' \
    )
    capacityconsumed = response['ConsumedCapacity']['CapacityUnits']


    data=response['Items']
    while 'LastEvaluatedKey' in response:
        #print "hi"
        response=db_r.Table("lo_exams").query(\
            ReturnConsumedCapacity='TOTAL', \
            ExpressionAttributeValues={':teacherid' : teacher_id}, \
            ExclusiveStartKey=response['LastEvaluatedKey'], \
            KeyConditionExpression="t_id = :teacherid", \
            IndexName='teacherperformance' \
        )
        #print response
        capacityconsumed = capacityconsumed + response['ConsumedCapacity']['CapacityUnits']
        data.extend(response['Items'])

    totalgrade=0
    for d in data:
        totalgrade = totalgrade + int(d['grade'])

    average=totalgrade / int(len(data)+1)

    finishtime=time.time()
    timetocomplete = finishtime - starttime
    print "Script Started on %s, finished on %s, took %0.2fs to complete without indexes" % (time.ctime(starttime),\
                time.ctime(finishtime), timetocomplete)
    print "Operation required %.1f capacity units to complete" % capacityconsumed
    print "Average performance for teacher [%s] is %.2f percent over %i exams " % (teacher_id, average, int(len(data)))
