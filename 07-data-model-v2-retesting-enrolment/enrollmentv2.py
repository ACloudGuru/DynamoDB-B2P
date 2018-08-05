# Enrollment benchmarking script
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
s3_c = loadmin_session.client('s3')
s3_r = loadmin_session.resource('s3')

bucketname = 'YOURBUCKETNAMEHERE'

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
def get_counter(keyname, **keywordparams): #given id pkey name, return id to use
    RETRY_EXCEPTIONS = ()  # defined variable with null to avoid > NameError: name 'RETRY_EXCEPTIONS' is not defined
    while True:
        try:
            current_id = db_r.Table("lo_counters").get_item(Key={'countername' : keyname},\
                            ConsistentRead=True)['Item']['value']
            retries = 0
            break
        except ClientError as err:
            if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                raise
            sleep(2 ** retries)
            retries +=1
    id_to_use = current_id + 1
    if ('batchsize' in keywordparams):
        id_to_store = current_id + keywordparams['batchsize']
    else:
        id_to_store = current_id + 1

    while True:
        try:
            db_r.Table("lo_counters").put_item(Item=\
                    {'countername' : keyname, 'value' : id_to_store})
            retries = 0
            break
        except ClientError as err:
            if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                raise
            sleep(2 ** retries)
            retries +=1

    return id_to_use
#------------------------------------------------------------------------------
def s3_upload(BucketName, Sourcefile): # given a file, upload and return url
    # take the bucket name and source file
    # generate a random key name
    # create a key, with the binary data from the param
    # return the URL to the file
    randomname = str(uuid.uuid4())+".jpg"
    s3_c.upload_file(Sourcefile, BucketName, randomname)
    return s3_c.generate_presigned_url('get_object', {'Bucket' : BucketName, 'Key' : randomname}, ExpiresIn=7776000)
#------------------------------------------------------------------------------
def item_gen(Type, Counter, Idbucket): # Generate ITEM for type
    p = fake.profile()
    # build a list object to store one ITEMS attributes
    i={}
    if Type=='student':
        i['s_id'] = str(Counter)
        i['first_name'] = fake.first_name()
        i['last_name'] = fake.last_name()
        i['email'] = p['mail']
        i['state'] = fake.state()
        # i['birthdate'] = p['birthdate']
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = p['sex']
        i['street_address'] = fake.street_address()
        i['city'] = fake.city()
        i['zipcode'] = fake.zipcode()
        i['country'] = fake.country()
        i['govid'] = fake.ssn()
        idpicfilename = "images/student-id-"+str(random.randrange(1,5))+".jpg"
        i['idscan'] = s3_upload(BucketName=Idbucket, Sourcefile=idpicfilename)
    if Type=='teacher':
        i['t_id'] = str(Counter)
        i['first_name'] = fake.first_name()
        i['last_name'] = fake.last_name()
        i['email'] = p['mail']
        i['state'] = fake(state)
        # i['birthdate'] = p['birthdate']
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = p['sex']
        i['street_address'] = fake.street_address()
        i['city'] = fake.city()
        i['zipcode'] = fake.zipcode()
        i['country'] = fake.country()
        i['govid'] = fake.ssn()
        idpicfilename = "images/teacher-id-"+str(random.randrange(1,5))+".jpg"
        i['idscan'] = s3_upload(BucketName=Idbucket, Sourcefile=idpicfilename)
    if Type=='exam':
        t_location_a = ['remote', 'remote', 'remote', 'remote', fake.city() ]
        t_location = t_location_a[random.randrange(5)]
        t_studentid = str(random.randrange(1,2500))
        t_module = str(random.randrange(1,250))
        i['e_id'] = str(Counter)
        i['s_id'] = t_studentid
        i['b_id'] = str(random.randrange(1,1500))
        i['dateandtimestarted'] = randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random())
        i['location'] = t_location
        i['module'] = t_module
        i['comments'] = "random comments about student["+str(t_studentid)+"] for module ["+str(t_module)+"]"
        i['duration'] = random.randrange(60,180)
        if t_location !='remote':
            i['physicalscan'] = s3_upload(BucketName=Idbucket, Sourcefile="images/exam.jpg")
            i['entrancepic'] = s3_upload(BucketName=Idbucket, Sourcefile="images/student-id-"+str(random.randrange(1,5))+".jpg")
    return i;
#------------------------------------------------------------------------------
if __name__ == "__main__":
    tests = [5,10,25,100,1000]
    retries = 0 # used for backoff function
    RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException',
                    'ThrottlingException')
    s3bucket = ""
    while True:
        if py3:
            s3bucket = input('Enter the name of the S3 bucket created by IDBucket.json: ')
        else:
            s3bucket = raw_input('Enter the name of the S3 bucket created by IDBucket.json: ')
        if s3bucket is not None:
            break
    ## END WHILE LOOP
    for n in range(len(tests)):
        print "INFO :: Starting load testing on students table, test set is "+str(tests)
        print "INFO :: Starting enrollment test [%d] - %d students" % (n+1, tests[n])
        with db_r.Table("lo_students").batch_writer() as batch:
            counter = get_counter(keyname='s_id', batchsize=tests[n])
            for c in tqdm.tqdm(range(tests[n])):
                ## find current s_id
                ## use it - add item
                batch.put_item(Item=item_gen(Type='student', Counter=counter+c, Idbucket=s3bucket))
            
