
# A Cloud Guru - Data Generating Script - Weatherstation_data
# Author - Adrian Cantrill - 2016 - v1
# Aug 2018 problems with 'date' type data being passed to Boto3. Treatment by converting to string during generation RL.
# Nov 2018 Added u_table to reduce idle CU

## PREREQS
##          configured AWS tools
##          installed python2.7+
##          installed boto3 (pip install boto3)
##          Installed tqdm module
##          'loadmin' AWS configuration profile - with admin rights
##          _retry.json file from lesson files - adjusted for 1mil+ auto retries
##              for retryable operations

import boto3, random, tqdm, time, botocore, uuid
from botocore.exceptions import ClientError
from tqdm import trange
from tqdm import tqdm

# Boto init
loadmin_session = boto3.Session(profile_name='loadmin')
db_c = loadmin_session.client('dynamodb')
db_r = loadmin_session.resource('dynamodb')


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
    return strTimeProp(start, end, '%Y%m%d%H%M', prop)
#------------------------------------------------------------------------------
def d_table(): # define table configuration
    table_config={}
    ## starting provisioned throughput settings for each table
    table_config['ProvisionedThroughput'] = { 'ReadCapacityUnits' : 5, 'WriteCapacityUnits' : 5 }
    table_config['KeySchema'] = [
            {'AttributeName' : 'station_id', 'KeyType' : 'HASH'}, \
            {'AttributeName' : 'dateandtime', 'KeyType' : 'RANGE'}, \
    ]
    table_config['AttributeDefinitions'] = [
        {'AttributeName' : 'station_id', 'AttributeType' : 'S'},\
        {'AttributeName' : 'dateandtime', 'AttributeType' : 'S'},\
    ]
    table_config['TableName'] = 'weatherstation_data'

    return table_config
#------------------------------------------------------------------------------
def c_table (c): # create dynamo DB tables
    try:
        print "INFO :: Creating %s Table....." % c['TableName']
        db_r.create_table(**c)
        print "INFO :: Waiting for completion..."
        db_r.Table(c['TableName']).wait_until_exists()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "ResourceInUseException":
            print "INFO :: WeatherstationInc %s Table exists, deleting ...." % c['TableName']
            db_r.Table(c['TableName']).delete()
            print "INFO :: Waiting for delete.."
            db_r.Table(c['TableName']).wait_until_not_exists()
            c_table (c)
        else:
            print "Unknown Error"
#------------------------------------------------------------------------------
def p_table (stations, datapoints): # Populate Table
    with db_r.Table('weatherstation_data').batch_writer() as batch:
        for station in trange(stations, desc='Stations'):
            for datapoint in trange(datapoints, desc='Datapoints'):
                item = item_gen(station)
                batch.put_item(Item=item)
#------------------------------------------------------------------------------
def item_gen(station_id): # Generate ITEM for a given station ID
    i={}
    i['station_id'] = str(station_id)
    i['dateandtime'] = str(randomDate("201601010000", "201606302359", random.random()))
    i['rainfall'] = random.randrange(0,10)
    i['temperature'] = random.randrange(10,30)
    i['uvindex'] = random.randrange(1,9)
    i['windspeed'] = random.randrange(1,20)
    i['lightlevel'] = random.randrange(1,100)
    return i;
#------------------------------------------------------------------------------
def u_table(Table, RCU, WCU):  # Update table with RCU and WCU
  print "INFO :: Updating Capacity on table [%s]" % Table
  db_r.Table(Table).update( \
    ProvisionedThroughput={'ReadCapacityUnits': RCU, 'WriteCapacityUnits': WCU}
  )
  time.sleep(5)
  while True:
    if db_r.Table(Table).table_status == 'ACTIVE':
      break
    time.sleep(30)
    print "INFO :: Waiting for update on table [%s]" % Table

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    num_of_stations=10
    num_of_datapoints=100
    print "Re-creating weatherstation_data table,"
    table_config = d_table() # create table config.
    t_conf=d_table() # generate table config
    c_table(t_conf) # create table, with the above config
    p_table(num_of_stations, num_of_datapoints) # populate the table with X rows
    print("")
    print("INFO :: Rest CU to minimum")

    u_table(Table="weatherstation_data", RCU=1, WCU=1)
    print('INFO :: Data Entry Complete')
