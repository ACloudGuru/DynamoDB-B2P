
# A Cloud Guru - Resource Deletion Script
# Author - Adrian Cantrill - 2016 - v1
# Aug 2018 problems with 'date' type data being passed to Boto3. Treatment by converting to string during generation RL.

## PREREQS -
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          'loadmin' AWS configuration profile - with admin rights
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries

import boto3, tqdm
from botocore.exceptions import ClientError
from sys import version_info
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2
from tqdm import trange
from tqdm import tqdm

# Boto init
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')
s3_c = loadmin_session.client('s3')
s3_r = loadmin_session.resource('s3')

#------------------------------------------------------------------------------
if __name__ == "__main__":
    s3bucket = ""

    print "WARNING :: ACG Dynamo DB Course - Data Clear Script"
    print "WARNING :: Ctrl + C Now to Exit - continuing will remove all the data from data model 1, 2, 3 or 4"
    print "WARNING :: The bucket name you enter below will be cleared of all objects...."
    print "WARNING :: Any tables starting with lo_ will be removed from this region in your account"

    if py3:
        s3bucket = input('Enter the name of the S3 bucket created by IDBucket.json, or enter if using v1 datamodel: ')
    else:
        s3bucket = raw_input('Enter the name of the S3 bucket created by IDBucket.json, or enter if using v1 datamodel: ')

    ## If bucket exists, remove keys
    if s3bucket !="":
        for object in tqdm(s3_r.Bucket(s3bucket).objects.all(), desc='Deleting S3 Objects'):
            object.delete()
    else:
        print "No S3 Bucket Name entered.... ignoring S3 object delete"

    ## Remove any lo_ tables
    for table in db_r.tables.all():
        if str(table.table_name).startswith('lo_'):
            table.delete()
            print "INFO :: Deleting Table [%s]" % table.table_name
            db_r.Table(table.table_name).wait_until_not_exists()
