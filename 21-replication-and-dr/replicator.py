import boto3, json, sys
from boto3.dynamodb.types import TypeDeserializer
print ('Loading Lambda Trigger - DR Replicator')

desttable = 'CHANGEME'
region = 'CHANGEME'
snsarn = 'CHANGEME'

deser = TypeDeserializer()
db_r = boto3.resource('dynamodb', region_name=destregion)
lo_table = db_r.Table(desttable)
def lambda_handler(event, context):
    for i, r in enumerate(event['Records']):
        d={}
        try:
            print 'Processing record %d, record type [%s], keys [%s]' % (i, r['eventName'], r['dynamodb']['Keys'])
            if r['eventName'] == 'REMOVE':
                lo_table.delete_item(Key=deserialize(r['dynamodb']['Keys']))
                continue
            if r['eventName'] in ["INSERT", "MODIFY"]:
                lo_table.put_item(Item=deserialize(r['dynamodb']['NewImage']))
                continue
        except:
            # any errors, notify humans
            sns = boto3.client('sns', region_name=r['awsRegion'])
            sns.publish(TargetArn=snsarn, Subject='replication error', Message=str(sys.exc_info()[0]))

def deserialize (obj):
    temp={}
    for key in obj:
        temp[key] = deser.deserialize(obj[key])
    return temp
