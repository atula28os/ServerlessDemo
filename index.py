import boto3
import json
import sys
import os
from botocore.exceptions import ClientError

DYNAMODB = boto3.resource('dynamodb')
TABLE = "zenser-order-table"
QUEUE = "Zenser-Order-Track-SQS"
SQS = boto3.client("sqs")

#SETUP LOGGING
import logging


LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
logHandler = logging.StreamHandler()


import boto3
import json
import sys
import os


DDB_TABLE_NAME = "zenser-order-table"
QUEUE = "Zenser-Order-Track-SQS"
BUCKET_NAME = 'zenser-demo-task'


sqs = boto3.resource('sqs')

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(DDB_TABLE_NAME)

_s3 = boto3.client('s3')

def amount_inc_tax(amount, tax_rate):
    return (amount + amount*tax_rate/100)

def dicounted_amount(amount, discount_percenatge):
    return (amount - amount*discount_percenatge/100)

def put_json_object_to_s3(json_object, bucket, key):
    try:
        response = _s3.put_object(Bucket=bucket, Key=key, Body=json_object)
        print("JSON object uploaded to S3:", response)
    except Exception as e:
        print("Error:", e)

def lambda_handler(event, context):
 
    LOG.info("Lambda Starts")
    receipt_handle = event["Records"][0]["receiptHandle"]  
    print(receipt_handle)
    for record in event["Records"]:
        invoice_data = {}
        
        body = json.loads(record["body"])
        invoice_data['order_id'] = body['order_id']
        invoice_data['customer_name'] = body['customer_name']
        invoice_data['items'] = body['items']
        invoice_data['amount'] = body['amount']
        invoice_data['discount_price'] = int(dicounted_amount(int(body['amount']), 15)) #15% discount
        invoice_data['final_amount'] = int(amount_inc_tax(int(invoice_data['discount_price']) , 18)) #18% tax rate
        
    
        try:
            # Add data to Dynamodb
            ddb_table.put_item(Item=invoice_data)
            
            
            invoice_json = json.dumps(invoice_data)
            file_name = "order/"+ str(invoice_data["order_id"]) + "-order" + ".json"
        
            # Upload the data to s3 as json file
            put_json_object_to_s3(invoice_json, BUCKET_NAME, key=file_name)
            
            
        except ClientError:
            LOG.error("Issue with data")
            

    
         
        
        
    
        
        
       