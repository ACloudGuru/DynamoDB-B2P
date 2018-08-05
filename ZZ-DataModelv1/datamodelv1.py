
# A Cloud Guru - Data Generating Script - Data Model version 1
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

import boto3, random, csv, tqdm, time, botocore, uuid
from botocore.exceptions import ClientError
from sys import version_info
from base64 import b64encode
from faker import Factory
fake = Factory.create()
py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2

# Boto init
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')
s3_c = loadmin_session.client('s3')
s3_r = loadmin_session.resource('s3')

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
def d_table(): # define table configuration
    table_config={}
    for x in ['lo_students', 'lo_teachers', 'lo_modules', 'lo_courses', 'lo_courseinstances', 'lo_attendance', \
                'lo_exams', 'lo_bookings', 'lo_counters', 'lo_studentgroups']:
                table_config[x]={}
    ## provisioned throughput settings for each table
    table_config['lo_students']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 5 }
    table_config['lo_teachers']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 500 }
    table_config['lo_modules']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }
    table_config['lo_courses']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }
    table_config['lo_courseinstances']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }
    table_config['lo_attendance']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }
    table_config['lo_exams']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 500 }
    table_config['lo_bookings']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }
    table_config['lo_counters']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 100, 'WriteCapacityUnits' : 100}
    table_config['lo_studentgroups']['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 2, 'WriteCapacityUnits' : 100 }

    ## Key Schemas for each table_config
    table_config['lo_students']['KeySchema'] = [{'AttributeName' : 's_id', 'KeyType' : 'HASH'}]
    table_config['lo_teachers']['KeySchema'] = [{'AttributeName' : 't_id', 'KeyType' : 'HASH'}]
    table_config['lo_modules']['KeySchema'] = [{'AttributeName' : 'm_id', 'KeyType' : 'HASH'}]
    table_config['lo_courses']['KeySchema'] = [{'AttributeName' : 'c_id', 'KeyType' : 'HASH'}]
    table_config['lo_courseinstances']['KeySchema'] = [ \
        {'AttributeName' : 'c_id', 'KeyType' : 'HASH'}, \
        {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} \
        ]
    table_config['lo_attendance']['KeySchema'] = [ \
        {'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, \
        {'AttributeName' : 's_id', 'KeyType' : 'RANGE'} \
        ]
    table_config['lo_exams']['KeySchema'] = [ {'AttributeName' : 'e_id', 'KeyType' : 'HASH'}]
    table_config['lo_bookings']['KeySchema'] = [ \
        {'AttributeName' : 'b_id', 'KeyType' : 'HASH'}, \
        {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} \
        ]

    table_config['lo_studentgroups']['KeySchema'] = [ \
        {'AttributeName' : 'g_id', 'KeyType' : 'HASH'}, \
        {'AttributeName' : 'ci_id', 'KeyType' : 'RANGE'} \
        ]

    table_config['lo_counters']['KeySchema'] = [{'AttributeName' : 'countername', 'KeyType' : 'HASH'}]

    ## Attribute Definitions for the keys for the tables
    table_config['lo_students']['AttributeDefinitions'] = [{'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    table_config['lo_teachers']['AttributeDefinitions'] = [{'AttributeName' : 't_id', 'AttributeType' : 'S'}]
    table_config['lo_modules']['AttributeDefinitions'] = [{'AttributeName' : 'm_id', 'AttributeType' : 'S'}]
    table_config['lo_courses']['AttributeDefinitions'] = [{'AttributeName' : 'c_id', 'AttributeType' : 'S'}]
    table_config['lo_courseinstances']['AttributeDefinitions'] = [\
        {'AttributeName' : 'c_id', 'AttributeType' : 'S'}, {'AttributeName' : 'ci_id', 'AttributeType' : 'S'}]
    table_config['lo_attendance']['AttributeDefinitions'] = [\
        {'AttributeName' : 'b_id', 'AttributeType' : 'S'}, {'AttributeName' : 's_id', 'AttributeType' : 'S'}]
    table_config['lo_exams']['AttributeDefinitions'] = [{'AttributeName' : 'e_id', 'AttributeType' : 'S'}]
    table_config['lo_bookings']['AttributeDefinitions'] = [\
        {'AttributeName' : 'b_id', 'AttributeType' : 'S'}, {'AttributeName' : 'ci_id', 'AttributeType' : 'S'}]
    table_config['lo_studentgroups']['AttributeDefinitions'] = [\
        {'AttributeName' : 'g_id', 'AttributeType' : 'S'}, {'AttributeName' : 'ci_id', 'AttributeType' : 'S'}]
    table_config['lo_counters']['AttributeDefinitions'] = [{'AttributeName' : 'countername', 'AttributeType' : 'S'}]

    return table_config
#------------------------------------------------------------------------------
def c_table (Table, t_config): # create dynamo DB tables
    """
    try to create table, if it errors tables exist,
    drop the tables, and then rerun the function to create again.
    """
    try:
        print "INFO :: Creating %s Table....." % Table
        db_r.create_table(
            AttributeDefinitions = t_config[Table]['AttributeDefinitions'],
            TableName=Table,
            KeySchema = t_config[Table]['KeySchema'],
            ProvisionedThroughput=t_config[Table]['ProvisionedThroughput']
        )
        print "INFO :: Waiting for completion..."
        db_r.Table(Table).wait_until_exists()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print "INFO :: Learning Online %s Table exists, deleting ...." % Table
            db_r.Table(Table).delete()
            print "INFO :: Waiting for delete.."
            db_r.Table(Table).wait_until_not_exists()
            c_table (Table, t_config)
        else:
            print "Unknown Error"
#------------------------------------------------------------------------------
def p_table (Table): # Populate Tables
    numofrows = ( 2500 if Table=='lo_students' \
                else 500 if Table=='lo_teachers' \
                else 500 if Table=='lo_courses' \
                else 500 if Table=='lo_courseinstances' \
                else 250 if Table=='lo_modules' \
                else 500 if Table=='lo_exams' \
                else 0)

    print "INFO :: Starting upload to [%s] table.." % Table
    if Table in ["lo_students", "lo_teachers"]:
        with db_r.Table(Table).batch_writer() as batch:
            for c in tqdm.tqdm(range(1, numofrows+1)):
                if Table=='lo_students':
                    counter = get_counter(keyname='s_id')
                    batch.put_item(Item=item_gen(Type="student", Counter=counter))
                else:
                    counter = get_counter(keyname='t_id')
                    batch.put_item(Item=item_gen(Type="teacher", Counter=counter))

    if Table == "lo_exams":
        with db_r.Table(Table).batch_writer() as batch:
            for c in tqdm.tqdm(range(1,numofrows+1)):
                counter = get_counter(keyname='e_id')
                batch.put_item(Item=item_gen(Type="exam", Counter=counter))

    if Table == "lo_modules":
        with db_r.Table(Table).batch_writer() as batch:
            for c in tqdm.tqdm(range(1,numofrows+1)):
                counter = get_counter(keyname='m_id')
                batch.put_item(Item=item_gen(Type="module", Counter=counter))

    if Table == "lo_courses":
        with db_r.Table(Table).batch_writer() as batch:
            for c in tqdm.tqdm(range(1, numofrows+1)):
                counter = get_counter(keyname='c_id')
                batch.put_item(Item=item_gen(Type="course", Counter=counter))

    if Table == "lo_courseinstances":
        with db_r.Table(Table).batch_writer() as batch:
            for c in tqdm.tqdm(range(1, numofrows+1)):
                studentlist1=[]
                studentlist2=[]
                numofstudents1 = random.randrange(1,501)
                numofstudents2 = random.randrange(1,501)
                remainingcapacity1 = 500 - numofstudents1
                remainingcapacity2 = 500 - numofstudents2

                for s1 in range(1,numofstudents1+1):
                    studentlist1.append(str(random.randrange(1,2500)))
                for s2 in range(1,numofstudents2+1):
                    studentlist2.append(str(random.randrange(1,2500)))

                temp_item1 = {'c_id' : str(c), 'ci_id' : str(c)+"#"+"2015"+"#1", 'year' : '2015', \
                    'remainingcapacity' : random.randrange(387,450), 'students' : studentlist1 }
                temp_item2 = {'c_id' : str(c), 'ci_id' : str(c)+"#"+"2016"+"#1", 'year' : '2016', \
                    'remainingcapacity' : random.randrange(387,450), 'students' : studentlist2 }
                batch.put_item(Item=temp_item1)
                batch.put_item(Item=temp_item2)

    if Table == "lo_counters":
        with db_r.Table(Table).batch_writer() as batch:
            batch.put_item(Item={'countername' : 's_id', 'value' : 0})
            batch.put_item(Item={'countername' : 't_id', 'value' : 0})
            batch.put_item(Item={'countername' : 'm_id', 'value' : 0})
            batch.put_item(Item={'countername' : 'c_id', 'value' : 0})
            batch.put_item(Item={'countername' : 'b_id', 'value' : 0})
            batch.put_item(Item={'countername' : 'g_id', 'value' : 0})
            batch.put_item(Item={'countername' : 'e_id', 'value' : 0})
#------------------------------------------------------------------------------
def item_gen(Type, Counter): # Generate ITEM for type
    p = fake.profile()
    # build a list object to store one ITEMS attributes
    i={}
    if Type=='student':
        i['s_id'] = str(Counter)
        i['first_name'] = fake.first_name()
        i['last_name'] = fake.last_name()
        i['email'] = p['mail']
        # i['birthdate'] = p['birthdate']
        i['birthdate'] = p['birthdate'].isoformat()
        i['sex'] = p['sex']
        i['state'] = fake.state()
        i['street_address'] = fake.street_address()
        i['city'] = fake.city()
        i['zipcode'] = fake.zipcode()
        i['country'] = fake.country()
        i['govid'] = fake.ssn()
        idpicfilename = "images/student-id-"+str(random.randrange(1,5))+".jpg"
        with open(idpicfilename, mode='rb') as file:
            binary_data = file.read()
        i['idscan'] = b64encode(binary_data)
    if Type=='teacher':
        i['t_id'] = str(Counter)
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
        idpicfilename = "images/teacher-id-"+str(random.randrange(1,5))+".jpg"
        with open(idpicfilename, mode='rb') as file:
            binary_data = file.read()
        i['idscan'] = b64encode(binary_data)
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
        i['comments'] = "random comments about student["+t_studentid+"] for module ["+t_module+"]"
        i['duration'] = random.randrange(60,180)
        i['grade'] = random.randrange(20,100)
        if t_location !='remote':
            idpicfilename = "images/student-id-"+str(random.randrange(1,5))+".jpg"
            with open("images/exam.jpg", mode='rb') as exam:
                binary_exam_data = exam.read()
            with open(idpicfilename, mode='rb') as idpicfile:
                binary_idpic_data = idpicfile.read()
            i['physicalscan'] = b64encode(binary_exam_data)
            i['entrancepic'] = b64encode(binary_idpic_data)

    if Type=='module':
        i['m_id'] = str(Counter)
        i['description'] = "Random Description for module "+str(Counter)
        i['passmark'] = random.randrange(65,80)
        i['t_id'] = str(random.randrange(1,500))
    if Type=='course':
        i['c_id'] = str(Counter)
        i['description'] = "Random Description for Course "+str(Counter)
        i['passmark'] = random.randrange(65,80)
        modlist=[]
        for m in range(1,random.randrange(1,25)):
            modlist.append(str(random.randrange(1,500)))
        i['standardmodules'] = modlist

    return i;
#------------------------------------------------------------------------------
def get_counter(keyname): #given id pkey name, return id to use
    RETRY_EXCEPTIONS = () # defined variable with null to avoid > NameError: name 'RETRY_EXCEPTIONS' is not defined
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
    while True:
        try:
            db_r.Table("lo_counters").put_item(Item=\
                    {'countername' : keyname, 'value' : id_to_use})
            retries = 0
            break
        except ClientError as err:
            if err.response['Error']['Code'] not in RETRY_EXCEPTIONS:
                raise
            sleep(2 ** retries)
            retries +=1

    return id_to_use
#------------------------------------------------------------------------------
def u_table(Table, RCU, WCU): # Update table with RCU and WCU
    print "INFO :: Updating Capacity on table [%s]" % Table
    db_r.Table(Table).update( \
        ProvisionedThroughput = { 'ReadCapacityUnits' : RCU, 'WriteCapacityUnits' : WCU}
    )
    time.sleep(5)
    while True:
        if db_r.Table(Table).table_status == 'ACTIVE':
            break
        time.sleep(30)
        print "INFO :: Waiting for update on table [%s]" % Table
#------------------------------------------------------------------------------
if __name__ == "__main__":

    """
    table_config structure contains the JSON options for the table creation.
    """
    print "INFO :: Starting table creation and adding sample data..."
    table_config = d_table() # create table config.
    c_table(Table="lo_counters", t_config=table_config)
    p_table(Table="lo_counters")

    tables_to_create=[]
    tables_to_create.append("lo_students")
    tables_to_create.append("lo_teachers")
    tables_to_create.append("lo_modules")
    tables_to_create.append("lo_courses")
    tables_to_create.append("lo_courseinstances")
    tables_to_create.append("lo_attendance")
    tables_to_create.append("lo_exams")
    tables_to_create.append("lo_bookings")
    tables_to_create.append("lo_studentgroups")

    for x in tables_to_create:
                c_table(Table=x, t_config=table_config)
                p_table(Table=x)
                u_table(Table=x, RCU=2, WCU=2)

    u_table(Table="lo_counters", RCU=2, WCU=2)
