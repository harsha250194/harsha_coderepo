import boto3
import csv
import sys
import json
import re
bucket_name="motionauto-partner"
pattern=".json"
s3= boto3.client('s3')
result = s3.list_objects(Bucket=bucket_name,Prefix='tracking',Delimiter='*.json',MaxKeys=5)
keys=[]
'''
print (result['Contents'])
for obj in result['Contents']:
  keys.append(obj['Key'])
#  print(keys)
#print(result)
#bucket=s3.Bucket(bucket_name)'''

def json2csv(jsonpath):
  s3_obj =boto3.client('s3')
  print(jsonpath)

  s3_clientobj = s3_obj.get_object(Bucket='', Key=jsonpath)
  item = s3_clientobj['Body'].read()
  print(item)
  print(type(item))



  item=json.loads(item)
  print("json loaded data")
  print(item)
  print(type(item))


  partner=item['partner']
  print(partner)
  clickkey=item['clickKey']
  print(clickkey)
  page=item['page']
  print(page)
  timestamp=item['timestamp']
  print(timestamp)
  url=item['url']
  print(url)
  campaignid=item['campaignId']
  print(campaignid)
  channel=item['channel']
  print(channel)
  campaign=item['campaign']
  print(campaign)
  medium=item['medium']
  print(medium)
  
  click=clickkey+".csv"
  with open(click, 'w') as csvfile:
      fieldnames = ['partner', 'clickkey', 'page', 'timestamp', 'url', 'campaignid', 'channel','campaign', 'medium' ]
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

      writer.writeheader()
      writer.writerow({'partner': partner, 'clickkey': clickkey, 'page': page, 'timestamp': timestamp, 'url': url, 'campaignid': campaignid, 'channel': channel,'campaign': campaign, 'medium': medium })
  bucket = ''
  s3 = boto3.resource('s3')
  key_name="tracking"+"/"+click+".csv"
  s3.meta.client.upload_file( "names.csv" , bucket,key_name)
  print("sucess")

f=open('tracking.txt')
line=f.readline()
while line:
    line=str(line.rstrip("\n"))
    print(line)
    json2csv(line)
    line=f.readline()
f.close()

#for obj in bucket.objects.all():
#result=one.match(pattern,obj.key)
#print(result)
#  print(obj)
#  break
  #if 'tracking' in obj.key:
    #print(obj.key)
    #key=str(obj.key)
    #print(key)
    #print(type(key))
   # break
    #json2csv(key)

