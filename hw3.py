import boto3
import os
import csv
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.resource('s3',
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
)

bucket_name = 'yaow-for-cloud-infra'
try:
    s3.create_bucket(Bucket = bucket_name, CreateBucketConfiguration = {
        'LocationConstraint': 'us-west-2'
    })
except Exception as e:
    print(e)

bucket = s3.Bucket(bucket_name)
bucket.Acl().put(ACL = 'public-read')

dyndb = boto3.resource('dynamodb',
    region_name = 'us-west-2',
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
)

try:
    table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema = [
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Temp',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'Id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Temp',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print(e)

print("Table status:", table.table_status)

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter = ',', quotechar = '|')
    is_header = False
    for item in csvf:
        if is_header == False:
            is_header = True
            continue
        body = open(item[4], 'rb')
        s3.Object(bucket_name, item[4]).put(Body = body)
        md = s3.Object(bucket_name, item[4]).Acl().put(ACL='public-read')

        url = 'https://s3-us-west-2.amazonaws.com/' + bucket_name + '/' + item[4]
        metadata_item = {'Id': item[0], 'Temp': item[1], 
                'Conductivity': item[2], 'Concentration': item[3], 'url': url}
        try:
            table.put_item(Item = metadata_item)
        except Exception as e:
            print("item may already be there or another failure " + e)

response = table.get_item(
    Key = {
        'Id': '1',
        'Temp': '-1'
    }
)
item = response['Item']
print(item)

response