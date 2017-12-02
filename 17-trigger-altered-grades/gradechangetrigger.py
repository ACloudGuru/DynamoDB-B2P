import boto3
print ('Loading Lambda Trigger - GradeChange')
snsarn = "CHANGEME"

def lambda_handler(event, context):
    for r in event['Records']: # loop through all incoming stread records
        record=r['dynamodb']
        if ('NewImage' in record.keys()) and ('OldImage' in record.keys()): # make sure there is an Old & New Image
            if ('grade' in record['NewImage'].keys()) and ('grade' in record['OldImage'].keys()):
                    newgrade = int(record['NewImage']['grade']['N'])
                    oldgrade = int(record['OldImage']['grade']['N'])
                    print 'Parsing exam record [%s]' % (record['NewImage']['id']['S'])
                    if oldgrade != newgrade: # we only care if the grade has been changed, not added or removed
                        sns = boto3.client('sns', region_name=r['awsRegion']) # create a connection to sns
                        message = 'Exam grade change detected, exam [%s]' % record['NewImage']['id']['S']
                        message += 'old grade [%d], new grade [%d], ' % (oldgrade, newgrade)
                        message += 'student [%s]' % record['NewImage']['s_id']['S'])
                        print 'Sending Message :: %s' % message
                        sns.publish(TargetArn=snsarn, Subject='Grade Change Detected', Message=message)
                    else:
                        print 'No Grade-Change Detected .. no strange activity detected'

    return('Function Completed')
