dashboardbucket = 'CHANGEME'
dashboardbucketurl = 'CHANGEME'

import boto3, string
print ('Loading Trigger - New Exam')

def lambda_handler(event, context):
    for r in event['Records']:
        record=r['dynamodb']
        region=r['awsRegion']
        if ('NewImage' in record.keys()):
            update_html(record['NewImage']['b_id']['S'], region, dashboardbucket, 'booking')
            update_html(record['NewImage']['location']['S'], region, dashboardbucket, 'location')

def update_html (loc_or_id, region, bucket, updatetype):
    html = generate_html(updatetype, loc_or_id, region)
    writes3file(bucket, loc_or_id+'.html', html, region)
    print '%s HTML file updated, %s/%s.html' % (updatetype, bucket, loc_or_id)

def writes3file(bucket, objectname, body, region):
    boto3.client('s3', region_name=region).put_object(Bucket=bucket, Key=objectname, Body=body.encode(), ContentType='text/html')

def generate_html (htmltype, id, region):
    data = query(htmltype, id, region)
    html = '<!DOCTYPE html>\n<html>\n'
    html += html_head()
    html +='\t<body>\n'
    html +='\t\t<h1>Results for %s [%s]</h1>\n' % (htmltype.title(), id)
    html +='\t\t<h1>[%d] Full or partial result(s)..</h1><br>\n' % len(data)
    html +='\t\t<table style=\"width:100%\">\n'
    html +='\t\t\t<tr>\n'
    html +='\t\t\t\t<th>Exam ID</th>\n'
    html +='\t\t\t\t<th>Student ID</th>\n'
    html +='\t\t\t\t<th>Course ID</th>\n'
    html +='\t\t\t\t<th>Module ID</th>\n'
    html +='\t\t\t\t<th>Started</th>\n'
    html +='\t\t\t\t<th>Duration</th>\n'
    html +='\t\t\t\t<th>Grade</th>\n'
    html +='\t\t\t</tr>\n'

    attr_list = ['id', 's_id', 'ci_id', 'm_id', 'dateandtimestarted', 'duration', 'grade']
    for x, item in enumerate(data):
        line='\t\t\t<tr>\n'
        for attr in attr_list:
            line+=('\t\t\t\t<td>%s</td>\n' % str(item[attr]) if attr in item.keys() else '\t\t\t\t<td>N/A</td>\n')
        line+='\t\t\t</tr>\n'
        html +=line
    html +='</table>\n'
    html += '</body></html>'
    return html

def html_head(): # generate HTML header - simple style sheet
    html = '\t<head>\n'
    html += '\t\t<meta http-equiv=\"refresh\" content=\"5\">\n'
    html += '\t\t<link href="http://fonts.googleapis.com/css?family=Playfair+Display" rel="stylesheet" type="text/css">\n'
    html += '\t\t<link href="http://fonts.googleapis.com/css?family=Muli" rel="stylesheet" type="text/css">\n'
    html += '\t\t<style>\n'
    html += '\t\t\th1 {font: 400 40px/1.5 "Playfair Display", Georgia, serif;}\n'
    html += '\t\t\tbody {font: 400 16px/1.6 "Muli", Verdana, Helvetica, sans-serif;}\n'
    html += '\t\t</style>\n\t</head>\n'
    return html

def query(q, id, region):
    exams = boto3.resource('dynamodb', region_name=region).Table('lo_exams') # create db connection
    v = {':id' : id} # set value substitution
    f = '#A = :id' # set filter
    n=({'#A' : 'b_id'} if q=='booking' else {'#A' : 'location'}) # set name substituion
    r = exams.scan(ExpressionAttributeValues=v,FilterExpression=f,ExpressionAttributeNames=n)
    data=r['Items']
    while 'LastEvaluatedKey' in r:
        r = exams.scan(ExpressionAttributeValues=v,FilterExpression=f,ExclusiveStartKey=r['LastEvaluatedKey'], ExpressionAttributeNames=n)
        data.extend(r['Items'])
    return data
