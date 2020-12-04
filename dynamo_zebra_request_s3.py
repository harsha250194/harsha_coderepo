import boto3
import decimal
import json
import boto3.dynamodb.types
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as jsondb
import uuid


#transaction_id='0c86438d-faa9-40a0-b7c3-90b725e7498b'
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

s3 = boto3.resource('s3')
dynamodb=boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('q2b_offer')
#response = table.query(KeyConditionExpression= Key('transaction_id').eq(transaction_id))

# Scan operations are limited to 1 MB at a time.
# Iterate until all records have been scanned.
response = None
while True:
    if not response:
        # Scan from the start.
        response = table.scan()
    else:
        # Scan from where you stopped previously.
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    for i in response["Items"]:
        item = json.loads(json.dumps(response['Items'][0], cls=DecimalEncoder))
        #print(item)  
        transaction_id=item['transaction_id']
        #print(transaction_id)
	key_name="zebra_request"+"/"+transaction_id+".json"
        s3data = s3.Object("ma-snowflake", key_name)
        s3data.put(
        Body=(json.dumps(item))
        )
        print("success")

    if 'LastEvaluatedKey' not in response:
        break
