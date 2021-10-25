import boto3 
import csv 
import os

# use environment variables for AWS creds with os.environ
s3 = boto3.resource('s3', 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'] 
) 

try: 
    s3.create_bucket(Bucket='chm171-bucket', CreateBucketConfiguration={ 
        'LocationConstraint': 'us-east-2'}) 
except Exception as e: 
    print (e)

bucket = s3.Bucket("chm171-bucket") 
bucket.Acl().put(ACL='public-read') 

#upload a new object into the bucket 
body = open('./experiments.csv', 'rb') 
o = s3.Object('chm171-bucket', 'experiments.csv').put(Body=body ) 
s3.Object('chm171-bucket', 'experiments.csv').Acl().put(ACL='public-read')

body = open('./exp1.csv', 'rb') 
o = s3.Object('chm171-bucket', 'exp1.csv').put(Body=body ) 
s3.Object('chm171-bucket', 'exp1.csv').Acl().put(ACL='public-read')

body = open('./exp2.csv', 'rb') 
o = s3.Object('chm171-bucket', 'exp2.csv').put(Body=body ) 
s3.Object('chm171-bucket', 'exp2.csv').Acl().put(ACL='public-read')

body = open('./exp3.csv', 'rb') 
o = s3.Object('chm171-bucket', 'exp3.csv').put(Body=body ) 
s3.Object('chm171-bucket', 'exp3.csv').Acl().put(ACL='public-read')

# Creating the dynamodb table
dyndb = boto3.resource('dynamodb', 
    region_name='us-east-2', 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], 
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'] 
 ) 

try: 
    table = dyndb.create_table( 
        TableName='DataTable', 
        KeySchema=[ 
            { 
                'AttributeName': 'Id', 
                'KeyType': 'HASH' 
            }
        ], 
        AttributeDefinitions=[ 
            { 
                'AttributeName': 'Id', 
                'AttributeType': 'S' 
            }, 
 
        ], 
        ProvisionedThroughput={ 
            'ReadCapacityUnits': 5, 
            'WriteCapacityUnits': 5 
        } 
    ) 
except Exception as e: 
    print (e) 
    #if there is an exception, the table may already exist.   if so... 
    table = dyndb.Table("DataTable") 

#wait for the table to be created 
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable') 
print(table.item_count) 


# reading csv file, uploading blobs, creating table
with open("./experiments.csv", 'r') as csvfile: 
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|') 
    item_index = 0
    for item in csvf: 
        print(item)
        if (item_index == 0):
            item_index += 1
            continue
        else:
            item_index += 1
        body = open("./"+item[4], 'rb') 
        s3.Object('chm171-bucket', item[4]).put(Body=body ) 
        md = s3.Object('chm171-bucket', item[4]).Acl().put(ACL='public-read') 
         
        url = " https://s3-us-east-2.amazonaws.com/chm171-bucket/"+item[4] 
        metadata_item = {'Id': item[0], 'Temp': item[1],  
                 'Conductivity' : item[2], 'Concentration' : item[3], 'url':url}  
        try: 
            table.put_item(Item=metadata_item) 
        except: 
            print("item may already be there or another failure")