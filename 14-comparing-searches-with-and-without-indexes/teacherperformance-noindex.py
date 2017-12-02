# Find students within a country
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
    teacher_id = ""
    capacityconsumed = 0
    scannedcount = 0


    while True:
        if py3:
            teacher_id = str(input('Enter a teacher ID: '))
        else:
            teacher_id = str(raw_input('Enter a teacher ID: '))
        if teacher_id !="":
            break
    ## END WHILE LOOP
    starttime = time.time()
    response=db_r.Table("lo_exams").scan(\
        ReturnConsumedCapacity='TOTAL', \
        ExpressionAttributeValues={':teacher_id' : teacher_id}, \
        FilterExpression="t_id = :teacher_id" \
    )
    capacityconsumed = response['ConsumedCapacity']['CapacityUnits']
    scannedcount = response['ScannedCount']

    data=response['Items']
    while 'LastEvaluatedKey' in response:
        #print "hi"
        response=db_r.Table("lo_exams").scan(\
            ReturnConsumedCapacity='TOTAL', \
            ExpressionAttributeValues={':teacher_id' : teacher_id}, \
            ExclusiveStartKey=response['LastEvaluatedKey'], \
        FilterExpression="t_id = :teacher_id" \
        )
        #print response
        capacityconsumed = capacityconsumed + response['ConsumedCapacity']['CapacityUnits']
        scannedcount = scannedcount + response['ScannedCount']
        data.extend(response['Items'])

    totalgrade=0
    for d in data:
        totalgrade = totalgrade + int(d['grade'])

    average=totalgrade / int(len(data)+1)

    finishtime=time.time()
    timetocomplete = finishtime - starttime
    print "Script Started on %s, finished on %s, took %0.2fs to complete without indexes" % (time.ctime(starttime),\
                time.ctime(finishtime), timetocomplete)
    print "Operation required %i capacity units to complete" % capacityconsumed
    print "Total number of items scanned [%i]" % scannedcount
    print "Average performance for teacher [%s] is %.2f percent over %i exams " % (teacher_id, average, int(len(data)))
