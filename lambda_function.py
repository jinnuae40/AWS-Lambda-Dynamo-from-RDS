import json
import pymysql
import boto3

with open('config.json', 'r') as f:
    config = json.load(f)

RDS_endpoint = config['RDS_endpoint']
UserName = config['UserName']
Password = config['Password']
DatabaseName = config['DatabaseName']
DynamoDB_Table_Name = config['DynamoDB_Table_Name']
Query = config['Query']
DynamoDB_Region = config['DynamoDB_Region']


def lambda_handler(event, context):
    try:
        connectionString = pymysql.connect(
            host=RDS_endpoint,
            user=UserName,
            passwd=Password,
            db=DatabaseName)
        pointer = connectionString.cursor()
        pointer.execute(Query)

        table_rows = pointer.fetchall()
        print(table_rows)

        # Client connection to DynamoDB
        try:
            dynamodbConnection = boto3.resource(
                "dynamodb", region_name=DynamoDB_Region)
            dynamodbClient = dynamodbConnection.Table(DynamoDB_Table_Name)

            # Put item to DynamoDB Table
            try:
                for r in table_rows:
                    item_response = dynamodbClient.put_item(
                        Item={
                            'studentId': str(r[0]),
                            'studentName': r[1],
                            'Course': r[2],
                            'Semester': r[3]
                        }
                    )
                return {"Status": 200, "Message": "Successfully took RDS Backup to DynamoDB"}
            except Exception as e:
                print("Insert Item to DynamoDB Table failed because ", e)
        except Exception as e:
            print("Client connection to DynamoDB Failed because ", e)
    except Exception as e:
        print("RDS Table connection failed because ", e)
