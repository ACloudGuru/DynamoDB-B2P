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
    course_id = ""
    capacityconsumed = 0.0

    while True:
        if py3:
            course_id = str(input('Enter a course id [c_id]: '))
        else:
            course_id = str(raw_input('Enter a course id [c_id]: '))
        if course_id !="":
            break
    ## END WHILE LOOP

    response=db_r.Table("lo_courseinstances").query(\
        ReturnConsumedCapacity='TOTAL', \
        ExpressionAttributeValues={':course' : course_id}, \
        KeyConditionExpression="c_id =:course",\
        ProjectionExpression="ci_id, students" \
    )

    capacityconsumed = response['ConsumedCapacity']['CapacityUnits']

    data=response['Items']
    while 'LastEvaluatedKey' in response:
        #print "hi"
        response=db_r.Table("lo_courseinstances").query(\
            ReturnConsumedCapacity='TOTAL', \
            ExpressionAttributeValues={':course' : course_id}, \
            KeyConditionExpression="c_id =:course",\
            ProjectionExpression="ci_id, students", \
            ExclusiveStartKey=response['LastEvaluatedKey'] \
        )

        #print response
        capacityconsumed = capacityconsumed + response['ConsumedCapacity']['CapacityUnits']
        data.extend(response['Items'])

    print "Course [%s] has [%i] instances" % (course_id, len(data))
    for i in range(len(data)):
        print "Course Instance [%s], has [%d] students" % (data[i]['ci_id'], len(data[i]['students']))

    print "Operation required %.1f capacity units to complete" % capacityconsumed


    print "Press Q to quit... or any other key to list students for course ID [%s] " % course_id

    if py3:
        entry = str(input()).lower()
    else:
        entry = str(raw_input()).lower()
    if entry == 'q':
        exit(0)

    for i in range(len(data)):
        for s, student in enumerate(data[i]['students']):
            print "[%i]\t[%s]\tStudent [%s]" % (s, data[i]['ci_id'], student)
