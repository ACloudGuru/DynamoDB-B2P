# Course Registration script
# A Cloud Guru - Dynamo DB Course\
# USES V2 DATA MODEL

import boto3, botocore, tqdm, random, time
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from base64 import b64encode
from sys import version_info
from faker import Factory
from time import sleep
fake = Factory.create()
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2
import uuid

loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')

bucketname = 'YOURBUCKETNAMEHERE'

## PREREQS -
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed faker module
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          Data Model v2 loaded.
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations

#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
if __name__ == "__main__":
    print "INFO :: Course Registration Script"
    courseid = 0
    num_to_register=1
    while True:
        if py3:
            courseid = int(input('Enter the number of a valid course ID: '))
        else:
            courseid = int(raw_input('Enter the number of a valid course ID: '))
        if courseid !=0:
            break
    ## END WHILE LOOP
    while True:
        if py3:
            num_to_register = int(input('Enter the number of students to register: '))
            break
        else:
            num_to_register = int(raw_input('Enter the number of students to register: '))
            break
    ## END WHILE LOOP


    year = '2016' # starting year, use 2016
    course_instance = 1 # starting course instance, always use 1

    ci_id=str(courseid)+"#"+year+"#"+str(course_instance)

    retries=0
    error_cond=False
    for s in tqdm.tqdm(range(1, num_to_register+1)):
        while True:
            try:
                struct = db_r.Table("lo_courseinstances").update_item(\
                    Key = {'c_id' : str(courseid), 'ci_id' : ci_id},\
                    ExpressionAttributeNames={'#S' : 'students', '#RC' : 'remainingcapacity'},
                    ExpressionAttributeValues={':cap' : -1, ':sid' : [s], ':mincap' : 0, ':ciid' : ci_id}, \
                    ConditionExpression="(#RC > :mincap) AND (attribute_exists(ci_id)) AND (contains(ci_id, :ciid))", \
                    UpdateExpression="SET #RC = #RC + :cap, #S = list_append (#S, :sid)", \
                    ReturnValues='ALL_NEW'
                )
                retries=0
                error_cond=False
                break
            except ClientError as err:
                if err.response['Error']['Code'] not in ["ConditionalCheckFailedException", "ValidationException", "ProvisionedThroughputExceededException", "ThrottlingException"]:
                    raise
                if err.response['Error']['Code'] is ["ProvisionedThroughputExceededException", "ThrottlingException"]:
                    sleep(2 ** retries)
                    retries +=1
                if err.response['Error']['Code'] == "ValidationException":
                    raise
                if err.response['Error']['Code'] == "ConditionalCheckFailedException":
                    if error_cond == True:
                        print "ERROR :: Cant locate course instance with spaces"
                        exit(1)
                    course_instance = course_instance+1
                    ci_id=str(courseid)+"#"+year+"#"+str(course_instance) # this moves it onto the next course instance as long as its an expression fail.
                    error_cond=True

    
