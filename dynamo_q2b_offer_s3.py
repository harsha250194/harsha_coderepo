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
        #item = response['Items']
        #item= jsondb.loads(item)
        #item=json.dumps(item)
        #item= json.loads(item)
        item = json.loads(json.dumps(response['Items'][0], cls=DecimalEncoder))
        print(type(item))
        print('declaring the columns')
        try:
            data1=item['offer']

            policy_id=item['policy_id']
            if policy_id is None:
              print("null value found for policy id " + str(item['transaction_id']) )
         
            else:
              policy_id=item['policy_id']
        except:
            print("in exception loop for transaction id " + str(item['transaction_id'])) 
            policy_id={}
            data1=item['offer']


        rating_config=item['rating_config']

        transaction_id=item['transaction_id']

        base=data1.get("base")
        best=data1.get("best")
        requested=data1.get("requested")
        good=data1.get("good")

        data = {
            "offer": {

                    "Premium": [
                        {
                            "base": base,
                            "best": best,
                            "requested": requested,
                            "good": good
                        }
                    ],
                    "Factors": [ 
                        {
                            "base": [
                                {
                                    "base_deductible": data1.get("base_deductible"),
                                    "base_factors": data1.get("base_factors"),
                                    "base_factors_initial": data1.get("base_factors_initial"),
                                    "base_factors_monthly": data1.get("base_factors_monthly"),
                                    "base_factors_pif": data1.get("base_factors_pif"),
                                    "base_factors_vehicles": data1.get("base_factors_vehicles"),
                                    "base_factors_vehicles EXP": data1.get("base_factors_vehicles EXP"),
                                    "base_factors_vehicles PIF": data1.get("base_factors_vehicles PIF"),
                                    "base_factors_vehicles PIF EXP": data1.get("base_factors_vehicles PIF EXP"),
                                    "base_fee": data1.get("base_fee"),
                                    "base_monthly_factors_vehicles EXP": data1.get("base_monthly_factors_vehicles EXP"),
                                    "base_pif": data1.get("base_pif"),
                                    "base_pif_fee": data1.get("base_pif_fee"),
                                    "base_pif_savings": data1.get("base_pif_savings"),
                                    "base_premium_vehicles EXP": data1.get("base_premium_vehicles EXP"),
                                    "base_premium_vehicles PIF EXP": data1.get("base_premium_vehicles PIF EXP")
                                }
                            ],
                            
                            "best": [
                                {
                                    "best_deductible": data1.get("best_deductible"),
                                    "best_factors": data1.get("best_factors"),
                                    "best_factors_initial": data1.get("best_factors_initial"),
                                    "best_factors_monthly": data1.get("best_factors_monthly"),
                                    "best_factors_pif": data1.get("best_factors_pif"),
                                    "best_factors_vehicles": data1.get("best_factors_vehicles"),
                                    "best_factors_vehicles EXP": data1.get("best_factors_vehicles EXP"),
                                    "best_factors_vehicles PIF": data1.get("best_factors_vehicles PIF"),
                                    "best_factors_vehicles PIF EXP": data1.get("best_factors_vehicles PIF EXP"),
                                    "best_fee": data1.get("best_fee"),
                                    "best_monthly_factors_vehicles EXP": data1.get("best_monthly_factors_vehicles EXP"),
                                    "best_pif": data1.get("best_pif"),
                                    "best_pif_fee": data1.get("best_pif_fee"),
                                    "best_pif_savings": data1.get("best_pif_savings"),
                                    "best_premium_vehicles EXP": data1.get("best_premium_vehicles EXP"),
                                    "best_premium_vehicles PIF EXP": data1.get("best_premium_vehicles PIF EXP")
                                }
                            ],
                            
                            "good": [
                                {
                                
                                    "good_deductible": data1.get("good_deductible"),
                                    "good_factors": data1.get("good_factors"),
                                    "good_factors_initial": data1.get("good_factors_initial"),
                                    "good_factors_monthly": data1.get("good_factors_monthly"),
                                    "good_factors_pif": data1.get("good_factors_pif"),
                                    "good_factors_vehicles": data1.get("good_factors_vehicles"),
                                    "good_factors_vehicles EXP": data1.get("good_factors_vehicles EXP"),
                                    "good_factors_vehicles PIF": data1.get("good_factors_vehicles PIF"),
                                    "good_factors_vehicles PIF EXP": data1.get("good_factors_vehicles PIF EXP"),
                                    "good_fee": data1.get("good_fee"),
                                    "good_monthly_factors_vehicles EXP": data1.get("good_monthly_factors_vehicles EXP"),
                                    "good_pif": data1.get("good_pif"),
                                    "good_pif_fee": data1.get("good_pif_fee"),
                                    "good_pif_savings": data1.get("good_pif_savings"),
                                    "good_premium_vehicles EXP": data1.get("good_premium_vehicles EXP"),
                                    "good_premium_vehicles PIF EXP": data1.get("good_premium_vehicles PIF EXP")
                                }
                            ],
                            
                            "requested": [
                                {
                                    "requested_deductible": data1.get("requested_deductible"),
                                    "requested_factors": data1.get("requested_factors"),
                                    "requested_factors_initial": data1.get("requested_factors_initial"),
                                    "requested_factors_monthly": data1.get("requested_factors_monthly"),
                                    "requested_factors_pif": data1.get("requested_factors_pif"),
                                    "requested_factors_vehicles": data1.get("requested_factors_vehicles"),
                                    "requested_factors_vehicles EXP": data1.get("requested_factors_vehicles EXP"),
                                    "requested_factors_vehicles PIF": data1.get("requested_factors_vehicles PIF"),
                                    "requested_factors_vehicles PIF EXP": data1.get("requested_factors_vehicles PIF EXP"),
                                    "requested_fee": data1.get("requested_fee"),
                                    "requested_monthly_factors_vehicles EXP": data1.get("requested_monthly_factors_vehicles EXP"),
                                    "requested_pif": data1.get("requested_pif"),
                                    "requested_pif_fee": data1.get("requested_pif_fee"),
                                    "requested_pif_savings": data1.get("requested_pif_savings"),
                                    "requested_premium_vehicles EXP": data1.get("requested_premium_vehicles EXP"),
                                    "requested_premium_vehicles PIF EXP": data1.get("requested_premium_vehicles PIF EXP")
                                }
                            ]
                        }
                    ],
                    
                    "payments": [
                        {
                            "initial_payment_base":  data1.get("initial_payment_base"),
                            "initial_payment_base_fee": data1.get("initial_payment_base_fee"),
                            "initial_payment_best": data1.get("initial_payment_best"),
                            "initial_payment_best_fee": data1.get("initial_payment_best_fee"),
                            "initial_payment_good": data1.get("initial_payment_good"),
                            "initial_payment_good_fee": data1.get("initial_payment_good_fee"),
                            "initial_payment_good_fee": data1.get("initial_payment_good_fee"),
                            "initial_payment_requested": data1.get("initial_payment_requested"),
                            "initial_payment_requested_fee": data1.get("initial_payment_requested_fee"),
                            "monthly_base": data1.get("monthly_base"),
                            "monthly_base_fee": data1.get("monthly_base_fee"),
                            "monthly_best": data1.get("monthly_best"),
                            "monthly_best_fee": data1.get("monthly_best_fee"),
                            "monthly_good": data1.get("monthly_good"),
                            "monthly_good_fee": data1.get("monthly_good_fee"),
                            "monthly_requested": data1.get("monthly_requested"),
                            "monthly_requested_fee": data1.get("monthly_requested_fee")
                            
                        }
                    ],
                    
                    "offer_id": data1.get("offer_id"),
                    "paperless": data1.get("paperless"),
                    "policy_term": data1.get("policy_term"),
                    "transaction_id": data1.get("transaction_id")

            },
                
            "policy_id": policy_id,
            "rating_config": rating_config,
            "transaction_id": transaction_id	
        }    
        #data = json.dumps(data)
        print(type(data))

        
        print("success")
        
    if 'LastEvaluatedKey' not in response:
        break
