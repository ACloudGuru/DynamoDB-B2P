
snsarn = 'CHANGEME'

print ('Loading Trigger - Teacher Performance')
import boto3
def lambda_handler(event, context):
    for r in event['Records']: # loop through all incoming stread records
        record=r['dynamodb']
        if 'NewImage' in record.keys(): # make sure there IS NewImage data
            if ('grade' in record['NewImage'].keys()) and (int(record['NewImage']['grade']['N']) < 65):
                grade = int(record['NewImage']['grade']['N'])
                print 'Parsing exam record [%s] - Grade [%i] recieved' % (record['NewImage']['id']['S'], grade)
                db_r = boto3.resource('dynamodb', region_name=r['awsRegion']) # create a connection to dynamo DB
                t_id = record['NewImage']['t_id']['S']
                t_perf = get_teacher_average(t_id, db_r)
                if t_perf < 65: # at this point, THIS grade is <65 and the teahers avg < 65
                    s_id =  record['NewImage']['s_id']['S']
                    sns = boto3.client('sns', region_name=r['awsRegion']) # create a connection to sns
                    m = 'Teacher ['+t_id+'] achieved a low grade from student ['+s_id+']'
                    m += 'with ['+str(grade)+'%] and has an average of ['+str(t_perf)+'%]'
                    print 'Sending Message :: %s' % m
                    sns.publish(TargetArn=snsarn, Subject='Teacher Low Grade Average', Message=m)
                else:
                    print 'Teacher [%s] is perfoming above requirements at [%d]% - no notifications required' % (t_id, t_perf)
    return('Function Completed')

def get_teacher_average(t_id, conn):
    # connect to the exam table, teacher performance index.
    # query all exam results for t_id
    # find average - # of results / total grade
    # paginate through results if more than 1MB data return.
    r = conn.Table('lo_exams').query(\
        ReturnConsumedCapacity='TOTAL', ExpressionAttributeValues={':teacherid' : t_id}, \
        KeyConditionExpression='t_id = :teacherid', IndexName='teacherperformance' )
    data=r['Items']
    while 'LastEvaluatedKey' in r:
        r=db_r.Table('lo_exams').query(\
            ReturnConsumedCapacity='TOTAL', ExpressionAttributeValues={':teacherid' : t_id}, \
            ExclusiveStartKey=r['LastEvaluatedKey'],KeyConditionExpression='t_id = :teacherid', \
            IndexName='teacherperformance' )
        data.extend(r['Items'])
    t_total=0
    for d in data:
        t_total = t_total + int(d['grade'])
    t_avg=t_total / int(len(data)+1)

    return t_avg
