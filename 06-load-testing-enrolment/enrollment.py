# Enrollment benchmarking script
# A Cloud Guru - Dynamo DB Course

import boto3, botocore, tqdm, random, time
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from base64 import b64encode
from faker import Factory
from time import sleep
fake = Factory.create()

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
##          Data Model v1 loaded.
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations

#------------------------------------------------------------------------------
def item_gen(type, id):
    p = fake.profile()
    # build a list object to store one ITEMS attributes
    i={}
    if type=='student':
        i['s_id'] = str(id)
        i['first_name'] = fake.first_name()
        i['last_name'] = fake.last_name()
        i['email'] = p['mail']
        # i['birthdate'] = p['birthdate']
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = p['sex']
        i['street_address'] = fake.street_address()
        i['state'] = fake.state()
        i['city'] = fake.city()
        i['zipcode'] = fake.zipcode()
        i['country'] = fake.country()
        i['govid'] = fake.ssn()
        idpic = "images/student-id-"+str(random.randrange(1,5))+".jpg"
        with open(idpic, mode='rb') as file:
            binary_data = file.read()
        i['idscan'] =  b64encode(binary_data)
    return i;

#==============================================================================
if __name__ == "__main__":
    tests = [5,10,25,100,1000]
    retries = 0 # used for backoff function
    RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')
    for n in range(len(tests)):
        print "INFO :: Starting load testing on students table, test set is "+str(tests)
        print "INFO :: Starting enrollment test [%d] - %d students" % (n+1, tests[n])
        with db_r.Table("lo_students").batch_writer() as batch:
            for c in tqdm.tqdm(range(1, tests[n]+1)):
                ## find current s_id
                while True:
                    try:
                        current_id = db_r.Table("lo_counters").get_item(Key={'countername' : 's_id'},\
                            ConsistentRead=True)['Item']['value']
                        retries = 0
                        break
                    except ClientError as err:
                        if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                            raise
                        sleep(2 ** retries)
                        retries +=1

                ## incriment it
                id_to_use = current_id + 1
                while True:
                    try:
                        db_r.Table("lo_counters").put_item(Item=\
                            {'countername' : 's_id', 'value' : id_to_use}
                        )
                        retries = 0
                        break
                    except ClientError as err:
                        if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                            raise
                        sleep(2 ** retries)
                        retries +=1

                ## use it - add item

                student = item_gen(type='student', id=id_to_use)
                while True:
                    try:
                        db_r.Table("lo_students").put_item(Item=student)
                        retries = 0
                        break
                    except ClientError as err:
                        if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                            raise
                        sleep(2 ** retries)
                        retries +=1

            print "INFO :: Test [%d] finished..." % n

#--
