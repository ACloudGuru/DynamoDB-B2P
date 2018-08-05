
# A Cloud Guru - Data Generating Script - Section 4 - Data Model 4
# Author - Adrian Cantrill - 2016 - v1
# Aug 2018 problems with 'date' type data being passed to Boto3. Treatment by converting to string during generation RL.

## PREREQS - images/ folder in same directory as this script
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed faker module
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          either no data model loaded, or data model v1 loaded (this will remove v1)
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations
##          Datamodelv2.cfn cloud formation template applied, bucket details available
##              you will be asked to input.

import boto3, random, csv, tqdm, time, botocore
from botocore.exceptions import ClientError
from sys import version_info
from base64 import b64encode
from faker import Factory
from tqdm import trange
from tqdm import tqdm
fake = Factory.create()
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2
import uuid

# Boto init
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')
s3_c = loadmin_session.client('s3')
s3_r = loadmin_session.resource('s3')

# numebr of each type to create
student_count = 25000
teacher_count = 500
course_count = 250
courseinstance_count = 500
module_count = 250
exam_count=10000
booking_count=200
attendance_count=2000

#------------------------------------------------------------------------------
def strTimeProp(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.
    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))
#------------------------------------------------------------------------------
def randomDate(start, end, prop):
    return strTimeProp(start, end, '%m/%d/%Y %I:%M %p', prop)
#------------------------------------------------------------------------------
def c_table(TableName, **kwargs): # handles the creation of a tabale with error checking
                                # kwargs optionally passes in BOTO3 sessions for multithreading
    try:
        db_r.create_table(**t_conf(TableName))
        print "INFO :: Waiting for Table [%s] to complete..." % TableName
        db_r.Table(TableName).wait_until_exists()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            db_r.Table(TableName).delete()
            print "INFO :: Learning Online %s Table exists, waiting for delete ...." % TableName
            db_r.Table(TableName).wait_until_not_exists()
            c_table(TableName)
        else:
            raise
#------------------------------------------------------------------------------
def t_conf(t): # produces the config for a supplied TableName, used in c_table
    c={}
    c['TableName'] = t


    # table key configuration
    if t in ["lo_students", "lo_teachers", "lo_modules", "lo_courses", "lo_exams", "lo_bookings", ]:
        c['KeySchema'] = [{'AttributeName' : 'id', 'KeyType' : 'HASH'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'id', 'AttributeType' : 'S'}]
    if t == "lo_courseinstances":
        c['KeySchema'] = [{'AttributeName' : 'c_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'c_id', 'AttributeType' : 'S'}, {'AttributeName' : 'ci_id', 'AttributeType' : 'S'}]
    if t == "lo_courseregistration":
        c['KeySchema'] = [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'ci_id', 'AttributeType' : 'S'}, {'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    if t == "lo_coursemakeup":
        c['KeySchema'] = [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'm_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'ci_id', 'AttributeType' : 'S'}, {'AttributeName' : 'm_id', 'AttributeType' : 'S'}]
    if t == "lo_attendance":
        c['KeySchema'] = [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'b_id', 'AttributeType' : 'S'}, {'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    if t == "lo_bookings":
        c['KeySchema'] = [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}]
        c['AttributeDefinitions'] = [{'AttributeName' : 'b_id', 'AttributeType' : 'S'}]
    # table performance configuration (RCU/WCU)
    c['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 100, 'WriteCapacityUnits' : 100 }
    # table index configuration
    if t== "lo_students": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'countryregion', \
            'KeySchema' : [{'AttributeName' : 'country', 'KeyType' : 'HASH'}, {'AttributeName' : 'state', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'city']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'idsearch', \
            'KeySchema' : [{'AttributeName' : 'govid', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'idscan']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'namesearch', \
            'KeySchema' : [{'AttributeName' : 'last_name', 'KeyType' : 'HASH'}, {'AttributeName' : 'first_name', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['idscan', 'govid', 'email']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['AttributeDefinitions'].append({'AttributeName' : 'country', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'state', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'first_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'last_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'govid', 'AttributeType' : 'S'})
    if t== "lo_teachers": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'countryregion', \
            'KeySchema' : [{'AttributeName' : 'country', 'KeyType' : 'HASH'}, {'AttributeName' : 'state', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'city']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'idsearch', \
            'KeySchema' : [{'AttributeName' : 'govid', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['first_name', 'last_name', 'email', 'idscan']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'namesearch', \
            'KeySchema' : [{'AttributeName' : 'last_name', 'KeyType' : 'HASH'}, {'AttributeName' : 'first_name', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['idscan', 'govid', 'email']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['AttributeDefinitions'].append({'AttributeName' : 'country', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'state', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'first_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'last_name', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'govid', 'AttributeType' : 'S'})
    if t=="lo_modules": # module table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 't_id', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'taughtby', \
            'KeySchema' : [{'AttributeName' : 't_id', 'KeyType' : 'HASH'}], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
    if t=="lo_courseinstances": #courseinstanceindexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'year', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'coursechedules', \
            'KeySchema' : [{'AttributeName' : 'year', 'KeyType' : 'HASH'}, {'AttributeName' : 'c_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_courseregistration":
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentcourses', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_coursemakeup":
        c['GlobalSecondaryIndexes'] = []
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'moduleusafe', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 100}})
    if t=="lo_attendance":
        c['GlobalSecondaryIndexes'] = []
        c['LocalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'location', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentscans', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['LocalSecondaryIndexes'].append({\
            'IndexName' : 'attendancelocation', \
            'KeySchema' : [{'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'} ], \
            'Projection' : {'ProjectionType' : 'ALL'}})

    if t== "lo_exams": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'void', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'ci_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 's_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 't_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'm_id', 'AttributeType' : 'S'})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'examvoids', \
            'KeySchema' : [{'AttributeName' : 'void', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 'entrancepic', 'physicalscan', 'location']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'courseexams', \
            'KeySchema' : [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 'm_id', 't_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'teacherperformance', \
            'KeySchema' : [{'AttributeName' : 't_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 's_id', 'm_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'studentperformance', \
            'KeySchema' : [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['grade', 't_id', 'm_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'moduleexams', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 's_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'grade', 't_id']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})

    if t== "lo_bookings": # student table indexes
        c['GlobalSecondaryIndexes'] = []
        c['AttributeDefinitions'].append({'AttributeName' : 'type', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'ci_id', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'location', 'AttributeType' : 'S'})
        c['AttributeDefinitions'].append({'AttributeName' : 'm_id', 'AttributeType' : 'S'})

        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'bookingtype', \
            'KeySchema' : [{'AttributeName' : 'type', 'KeyType' : 'HASH'}, {'AttributeName' : 'location', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'm_id', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'coursebookings', \
            'KeySchema' : [{'AttributeName' : 'ci_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'm_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['type', 'location', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'bookinglocation', \
            'KeySchema' : [{'AttributeName' : 'location', 'KeyType' : 'HASH'}, {'AttributeName' : 'type', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['ci_id', 'm_id', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})
        c['GlobalSecondaryIndexes'].append({\
            'IndexName' : 'modulebookings', \
            'KeySchema' : [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}, {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'}], \
            'Projection' : {'ProjectionType' : 'INCLUDE', 'NonKeyAttributes' : ['type', 'location', 'startdateandtime', 'finishdateandtime']}, \
            'ProvisionedThroughput' : {'ReadCapacityUnits' : 50, 'WriteCapacityUnits' : 50}})

    # Stream configuration.. more on this later :)

    return c
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def p_table (Table, idbucket, uuiddict, **kwargs): # Populate Tables

    if Table == "lo_students":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(student_count, desc='Generating Student Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_teachers":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(teacher_count, desc='Generating Teacher Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_modules":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(module_count, desc='Generating Module Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_courses":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(course_count, desc='Generating Course Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_courseinstances":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(courseinstance_count, desc='Generating Course Instance Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_exams":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(exam_count, desc='Generating Exam Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))
    if Table == "lo_bookings":
        with db_r.Table(Table).batch_writer() as batch:
            for c in trange(booking_count, desc='Generating Bookings Data'):
                batch.put_item(Item=item_gen(Type=Table, uuid=uuiddict[Table][c],uuiddict=uuiddict, Idbucket=idbucket))

    if Table == "lo_attendance":
        with db_r.Table(Table).batch_writer() as batch:
            random.shuffle(uuiddict["lo_bookings"])
            for c in trange(attendance_count, desc='Generating Attendance Data'):
                location = ['remote', 'remote', 'remote', 'remote', fake.city()][random.randrange(5)]
                while True:
                    try:
                        batch.put_item(Item={\
                            'b_id' : uuiddict["lo_bookings"][random.randrange(50)],\
                            's_id' : uuiddict["lo_students"][random.randrange(250)],\
                            'location' : location,\
                            'dateandtimeentered' : randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random()),\
                            'dateandtimeleave' : randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random()) \
                            })
                        break
                    except:
                        pass




#------------------------------------------------------------------------------
def s3_upload(BucketName, Sourcefile): # given a file, upload and return url
    # take the bucket name and source file
    # generate a random key name
    # create a key, with the binary data from the param
    # return the URL to the file
    randomname = str(uuid.uuid4())+".jpg"
    #s3_c.upload_file(Sourcefile, BucketName, randomname)
    #sourceimg = {'Bucket' : BucketName, 'Key' : Sourcefile}
    #s3_r.Bucket(BucketName).Object(randomname).copy(sourceimg)
    #return s3_c.generate_presigned_url('get_object', {'Bucket' : BucketName, 'Key' : Sourcefile}, ExpiresIn=0)
    return "http://s3.adddress/"+randomname
#------------------------------------------------------------------------------
def s3_prepare(BucketName): # upload the student, teacher and exam images to s3
    for x in range(1,6):
        s3_c.upload_file("images/student-id-"+str(x)+".jpg", BucketName, "images/student-id-"+str(x)+".jpg")
        s3_c.upload_file("images/teacher-id-"+str(x)+".jpg", BucketName, "images/teacher-id-"+str(x)+".jpg")
    s3_c.upload_file("images/exam.jpg", BucketName, "images/exam.jpg")
#------------------------------------------------------------------------------
def item_gen(Type, uuid, uuiddict, Idbucket): # Generate ITEM for type
    p = fake.profile()
    # build a list object to store one ITEMS attributes
    i={} # item structure to be generated
    if Type=='lo_students':
        i['id'] = uuid
        i['first_name'] = str(fake.first_name())
        i['last_name'] = str(fake.last_name())
        i['email'] = str(p['mail'])
        # i['birthdate'] = str(p['birthdate'])
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = str(p['sex'])
        i['street_address'] = str(fake.street_address())
        i['city'] = str(fake.city())
        i['zipcode'] = str(fake.zipcode())
        i['state'] = str(fake.state())
        i['country'] = str(fake.country())
        i['govid'] = str(fake.ssn())
        idpicfilename = "images/student-id-"+str(random.randrange(1,6))+".jpg"
        i['idscan'] = s3_upload(BucketName=Idbucket, Sourcefile=idpicfilename)
    if Type=='lo_teachers':
        i['id'] = uuid
        i['first_name'] = str(fake.first_name())
        i['last_name'] = str(fake.last_name())
        i['email'] = str(p['mail'])
        # i['birthdate'] = str(p['birthdate'])
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = str(p['sex'])
        i['street_address'] = str(fake.street_address())
        i['city'] = str(fake.city())
        i['zipcode'] = str(fake.zipcode())
        i['state'] = str(fake.state())
        i['country'] = str(fake.country())
        i['govid'] = str(fake.ssn())
        idpicfilename = "images/teacher-id-"+str(random.randrange(1,6))+".jpg"
        i['idscan'] = s3_upload(BucketName=Idbucket, Sourcefile=idpicfilename)
    if Type=='lo_modules':
        i['id'] = uuid
        i['description'] = "Random Description for module ["+uuid+"]"
        i['passmark'] = random.randrange(65,80)
        i['t_id'] = uuiddict["lo_teachers"][random.randrange(50)] ## UUID of a valid teacher, sampler small so we have duplicates for index
    if Type=='lo_courses':
        i['id'] = uuid
        i['description'] = "Random Description for Course "+uuid
        i['passmark'] = random.randrange(65,80)
        modlist=[]
        for m in range(1,random.randrange(1,25)):
            modlist.append(uuiddict["lo_modules"][random.randrange(50)])
        i['standardmodules'] = modlist
    if Type=='lo_courseinstances':
        numofstudents = random.randrange(5,500) # token number of students to add to each course
        numofmodules = random.randrange(1,25) # token number of modules ot add to each course
        i['c_id'] = uuiddict["lo_courses"][random.randrange(100)] #small sample size of courses, so we have M:1 numbers
        i['ci_id'] = uuid
        i['year'] = str("201"+ str(random.randrange(5,7)))
        i['remainingcapacity'] = str( 250 -  numofstudents)
        with db_r.Table("lo_coursemakeup").batch_writer() as batch:
            for x in range(numofmodules):
                batch.put_item(Item={'ci_id' : uuid, 'm_id' : uuiddict["lo_modules"][x]})
        with db_r.Table("lo_courseregistration").batch_writer() as batch:
            for x in range(numofstudents):
                batch.put_item(Item={'ci_id' : uuid, 's_id' : uuiddict["lo_students"][x]})
    if Type=='lo_exams':
        t_location = ['remote', 'remote', 'remote', 'remote', fake.city()][random.randrange(5)]
        t_studentid = uuiddict['lo_students'][random.randrange(1000)]
        t_bookingid = uuiddict['lo_bookings'][random.randrange(100)]
        t_moduleid = uuiddict['lo_modules'][random.randrange(50)]
        t_ciid = uuiddict['lo_courseinstances'][random.randrange(100)]
        i['id'] = uuid
        i['s_id'] = t_studentid
        i['ci_id'] = t_ciid
        i['m_id'] = t_moduleid
        i['b_id'] = t_bookingid
        i['dateandtimestarted'] = randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random())
        i['location'] = t_location
        i['comments'] = "random comments about student["+str(t_studentid)+"] for module ["+str(t_moduleid)+"]"
        i['duration'] = random.randrange(60,180)
        i['grade'] = random.randrange(20,100)
        if t_location !='remote':
            i['physicalscan'] = s3_upload(BucketName=Idbucket, Sourcefile="images/exam.jpg")
            i['entrancepic'] = s3_upload(BucketName=Idbucket, Sourcefile="images/student-id-"+str(random.randrange(1,5))+".jpg")
        if random.randint(0,100) > 95:
            i['void'] = "true"
        i['t_id'] = uuiddict['lo_teachers'][random.randrange(250)]
    if Type=='lo_bookings':
        i['b_id']=uuid
        i['ci_id'] = uuiddict['lo_courseinstances'][random.randrange(courseinstance_count)]
        i['m_id']  =uuiddict['lo_modules'][random.randrange(module_count)]
        i['startdateandtime']=randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random())
        i['finishdateandtime']=randomDate("1/1/2015 12:00 AM", "6/30/2016 11:59 PM", random.random())
        if random.randint(0,100) >= 50:
            i['location']='remote'
            i['venuecapacity']='NA'
        else:
            i['location']=str(fake.city())
            i['venuecapacity'] = random.randint(100,500)
        if random.randint(0,100) >= 90:
            i['type'] = 'EXAM'
        else:
            i['type'] = 'LECTURE'
    return i # retrurn the completed item.
#------------------------------------------------------------------------------
def u_table(Table, RCU, WCU): # Update table with RCU and WCU
    print "INFO :: Updating Capacity on table [%s]" % Table
    # create a dict for storing new intended config
    newconf={}
    # retrieve old config from t_conf function
    oldconf = t_conf(Table)
    # every update will need this - set RCU and WCU on the main table to intended values...
    newconf['ProvisionedThroughput'] = { 'ReadCapacityUnits' : RCU, 'WriteCapacityUnits' : WCU}

    #loop through any GSI's, add keys in newconf to adjust these to intended RCU and WCU
    try:
        for gsi in oldconf['GlobalSecondaryIndexes']:
            if 'GlobalSecondaryIndexUpdates' not in newconf:
                newconf['GlobalSecondaryIndexUpdates']=[]
            newconf['GlobalSecondaryIndexUpdates'].append({"Update" : { \
                "IndexName" : gsi['IndexName'], \
                "ProvisionedThroughput" : {"ReadCapacityUnits" : RCU, "WriteCapacityUnits" : WCU} \
                }})
    except:
        print "INFO :: Now GSI found in table [%s]- unchanged" % Table

    # update table and wait for completion
    db_r.Table(Table).update(**newconf)
    time.sleep(5)
    while True:
        if db_r.Table(Table).table_status == 'ACTIVE':
            break
        time.sleep(30)
        print "INFO :: Waiting for update on table [%s]" % Table
#------------------------------------------------------------------------------
def uuidpool(num, tablename): # generate 'num' uuid's, return array
    pool=[]
    for i in trange(num, desc=tablename):
        pool.append(str(uuid.uuid4()))
    return pool
#------------------------------------------------------------------------------
def addstream(Table, StreamConfig):
    print "INFO :: Creating Dynamo DB streams on table [%s]" % Table
    db_r.Table(Table).update(**StreamConfig)
#------------------------------------------------------------------------------
if __name__ == "__main__":
    s3bucket = ""
    while True:
        if py3:
            s3bucket = input('Enter the name of the S3 bucket created by IDBucket.json: ')
        else:
            s3bucket = raw_input('Enter the name of the S3 bucket created by IDBucket.json: ')
        if s3bucket is not None:
            break
    ## If bucket exists, remove keys

    for object in tqdm(s3_r.Bucket(s3bucket).objects.all(), desc='Deleting S3 Objects'):
        object.delete()
    ##
    s3_prepare(s3bucket) ## upload 5 id pics for students, 5 for teachers, and 1 exam


    print "INFO :: Creating UUID Pools"
    uuid_dict={}
    uuid_dict['lo_students'] = uuidpool(num=student_count, tablename="lo_students")
    uuid_dict['lo_teachers'] = uuidpool(num=teacher_count, tablename="lo_teachers")
    uuid_dict['lo_modules'] = uuidpool(num=module_count, tablename="lo_modules")
    uuid_dict['lo_courses'] = uuidpool(num=course_count, tablename="lo_courses")
    uuid_dict['lo_courseinstances'] = uuidpool(num=courseinstance_count, tablename="lo_courseinstances")
    uuid_dict['lo_exams'] = uuidpool(num=exam_count, tablename="lo_exams")
    uuid_dict['lo_attendance'] = uuidpool(num=attendance_count, tablename="lo_attendance")
    uuid_dict['lo_bookings'] = uuidpool(num=booking_count, tablename="lo_bookings")

    for x in ["lo_students", "lo_teachers", "lo_modules", "lo_courses", "lo_attendance", "lo_exams", "lo_bookings"]:
        c_table(x)
        p_table(x, s3bucket, uuid_dict)
        u_table(x,2,2)

    c_table("lo_courseinstances")
    c_table("lo_courseregistration")
    c_table("lo_coursemakeup")
    p_table("lo_courseinstances", s3bucket, uuid_dict) ## this also does courseregistration & Makeup
    u_table("lo_courseinstances",2,2)
    u_table("lo_courseregistration",2,2)
    u_table("lo_coursemakeup",2,2)

    ## Add streams to tables - after the data has been added.... so we dont need to process it all.

    conf={}
    conf['StreamSpecification'] = {'StreamEnabled' : True, 'StreamViewType' : 'NEW_AND_OLD_IMAGES'}
    addstream("lo_exams", conf)
    addstream("lo_attendance", conf)
    addstream("lo_bookings", conf)
    print "INFO :: V4 Data load completed..."
