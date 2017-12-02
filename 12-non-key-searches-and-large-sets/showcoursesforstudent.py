# Show all courses student is registered on
# A Cloud Guru - Dynamo DB Course\
# USES V2 DATA MODEL

import boto3, botocore, tqdm, random, time
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from sys import version_info
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2

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
    student_id = ""
    capacityconsumed = 0
    scannedcount = 0


    while True:
        if py3:
            student_id = str(input('Enter a student id [s_id]: '))
        else:
            student_id = str(raw_input('Enter a student id [s_id]: '))
        if student_id !="":
            break
    ## END WHILE LOOP

    response=db_r.Table("lo_courseinstances").scan(\
        ReturnConsumedCapacity='TOTAL', \
        ExpressionAttributeValues={':student' : student_id}, \
        FilterExpression="contains(students, :student)", \
        ProjectionExpression="ci_id", \
    )
    capacityconsumed = response['ConsumedCapacity']['CapacityUnits']
    scannedcount = response['ScannedCount']

    data=response['Items']
    while 'LastEvaluatedKey' in response:
        #print "hi"
        response=db_r.Table("lo_courseinstances").scan(\
            ReturnConsumedCapacity='TOTAL', \
            ExpressionAttributeValues={':student' : student_id}, \
            FilterExpression="contains(students, :student)", \
            ExclusiveStartKey=response['LastEvaluatedKey'], \
            ProjectionExpression="ci_id", \
        )
        #print response
        capacityconsumed = capacityconsumed + response['ConsumedCapacity']['CapacityUnits']
        scannedcount = scannedcount + response['ScannedCount']
        data.extend(response['Items'])

    print "Student [%s] is registered to %d course instances" % (student_id, len(data))
    print "Operation required %i capacity units to complete" % capacityconsumed
    print "Total number of items scanned [%i]" % scannedcount
    print "Press Q to quit... or any other key to list course instances student [%s] is registered for" % student_id

    if py3:
        entry = str(input()).lower()
    else:
        entry = str(raw_input()).lower()
    if entry == 'q':
        exit(0)

    for i, c_instance in enumerate(data):
        print "[%d]\t[%s]" % (i+1, c_instance['ci_id'])
