import boto3, string, tqdm, random, time, uuid
from tqdm import trange
from time import sleep, gmtime, strftime
from sys import version_info
py3 = version_info[0] > 2
loadmin_session = boto3.Session(profile_name='loadmin')
db_r = loadmin_session.resource('dynamodb')

## PREREQS - images/ folder in same directory as this scripts
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed faker module
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          Data Model v4 loaded.
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations

def uuidpool(num, tablename): # generate 'num' uuid's, return array
    pool=[]
    for i in trange(num, desc=tablename):
        pool.append(str(uuid.uuid4()))
    return pool

def exam_update(uuidkey, bookingkey, location, stage):

        if stage==0:
            db_r.Table('lo_exams').update_item(Key={'id' : uuidkey},
                UpdateExpression='set b_id=:b, ci_id=:ci, comments=:c, dateandtimestarted=:stime, #L=:l, m_id=:m, s_id=:s, entrancepic=:p',
                ExpressionAttributeValues={
                    ':s' : str(uuid.uuid4()),
                    ':b' : bookingkey,
                    ':ci' : str(uuid.uuid4()),
                    ':c' : 'just testing...',
                    ':stime' : strftime('%Y-%m-%d %H:%M:%S', gmtime()),
                    ':m' : str(uuid.uuid4()),
                    ':p' : 'http://blah.com',
                    ':l' : location
                },
                ExpressionAttributeNames={'#L':'location'}
            )
        elif stage==1:
            db_r.Table('lo_exams').update_item(Key={'id' : uuidkey},
                UpdateExpression='set #D=:d',
                ExpressionAttributeValues={
                    ':d' : random.randrange(60,180)
                },
                ExpressionAttributeNames={'#D':'duration'}
            )
        elif stage==2:
            db_r.Table('lo_exams').update_item(Key={'id' : uuidkey},
                UpdateExpression='set grade=:g',
                ExpressionAttributeValues={
                    ':g' : random.randrange(30,95)
                }
            )

if __name__ == "__main__":
    print "INFO :: Exam simulation"
    pool=[]
    while True:
        if py3:
            location = str(input('Enter a location: '))
        else:
            location = str(raw_input('Enter a location: '))
        if location !="":
            break
    pool=uuidpool(25, 'lo_exams')
    pool2=uuidpool(3, 'b_id')

    print "INFO :: Adding 3 dummy exam entries with location %s, and 1 of 3 booking ID's" % location
    for x in range(3):
        db_r.Table('lo_exams').update_item(Key={'id' : str(uuid.uuid4())},
            UpdateExpression='set b_id=:b, ci_id=:ci, comments=:c, dateandtimestarted=:stime, #L=:l, m_id=:m, s_id=:s, entrancepic=:p',
            ExpressionAttributeValues={
                ':s' : str(uuid.uuid4()),
                ':b' : pool2[x],
                ':ci' : str(uuid.uuid4()),
                ':c' : 'just testing...',
                ':stime' : strftime('%Y-%m-%d %H:%M:%S', gmtime()),
                ':m' : str(uuid.uuid4()),
                ':p' : 'http://blah.com',
                ':l' : location
            },
            ExpressionAttributeNames={'#L':'location'}
        )
    sleep(30)
    for i in trange(3, desc='stage 0 entry, 1 exit, 2 grade'):
        for j in trange(25, desc='student'):
            exam_update(pool[j],pool2[random.randrange(3)], location, i)
            sleep(1)
