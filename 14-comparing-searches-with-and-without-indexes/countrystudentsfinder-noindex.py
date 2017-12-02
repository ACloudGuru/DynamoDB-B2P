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
    student_id = 0
    capacityconsumed = 0
    scannedcount = 0


    while True:
        if py3:
            country = str(input('Enter a country name: '))
        else:
            country = str(raw_input('Enter country name: '))
        if country !="":
            break
    ## END WHILE LOOP
    starttime = time.time()
    response=db_r.Table("lo_students").scan(\
        ReturnConsumedCapacity='TOTAL', \
        ExpressionAttributeValues={':country' : country}, \
        FilterExpression="country = :country" \
    )
    capacityconsumed = response['ConsumedCapacity']['CapacityUnits']
    scannedcount = response['ScannedCount']

    data=response['Items']
    while 'LastEvaluatedKey' in response:
        #print "hi"
        response=db_r.Table("lo_students").scan(\
            ReturnConsumedCapacity='TOTAL', \
            ExpressionAttributeValues={':country' : country}, \
            ExclusiveStartKey=response['LastEvaluatedKey'], \
            FilterExpression="country = :country" \
        )
        #print response
        capacityconsumed = capacityconsumed + response['ConsumedCapacity']['CapacityUnits']
        scannedcount = scannedcount + response['ScannedCount']
        data.extend(response['Items'])

    finishtime=time.time()
    timetocomplete = finishtime - starttime
    print "Script Started on %s, finished on %s, took %0.2fs to complete without indexes" % (time.ctime(starttime),\
                time.ctime(finishtime), timetocomplete)
    print "Operation required %i capacity units to complete" % capacityconsumed
    print "Total number of items scanned [%i]" % scannedcount
    print "Located [%i] students in [%s]" % (len(data), country)
    print "Press Q to quit... or any other key to list students in %s" % country

    if py3:
        entry = str(input()).lower()
    else:
        entry = str(raw_input()).lower()
    if entry == 'q':
        exit(0)
    print "#\tS_ID\t\t\t\t\tFIRSTNAME\tSURNAME\t\tCOUNTRY\tCITY\t\STATE\t\tEmail"
    for i, s_instance in enumerate(data):
        print "%d\t%s\t%s\t\t%s\t\t%s\t%s\t%s\t\t%s" % (i+1, \
        s_instance['id'], s_instance['first_name'], s_instance['last_name'], s_instance['country'],\
        s_instance['city'], s_instance['state'], s_instance['email'])
