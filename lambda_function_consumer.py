import json
import boto3
from datetime import datetime
import pandas as pd
import io

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')
sns_arn = "arn:aws:sns:us-east-1:058264373160:airbnb-booking-notification"
sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/058264373160/AirbnbBookingQueue"
s3_upload_bucket = "airbnb-records-booking"
s3_upload_object_key = "file.txt"


def lambda_handler(event, context):

    print("Event is : ", event)

    response = sqs_client.receive_message(
        QueueUrl = sqs_queue_url,
        MaxNumberOfMessages = 1,
        WaitTimeSeconds = 1
    )

    print("response is : ", response)

    if "Records" in event:

       for message in event['Records']:
          actual_message = json.loads(message['body'])
          startDate = datetime.strptime(actual_message['startDate'], "%Y-%m-%d")
          endDate = datetime.strptime(actual_message['endDate'], "%Y-%m-%d")

          date_difference = endDate - startDate 

          print("Difference in Days : ", date_difference)

          if date_difference > 1:
             airbnb_df = pd.DataFrame(actual_message)
             csv_buffer = io.StringIO()
             airbnb_df.to_csv(csv_buffer, index = False)
             s3_client.put_object(Bucket = s3_upload_bucket, Key = s3_upload_object_key, Body = csv_buffer.getvalue())

             sns_client.publish(
                Subject = f"Luxurious AIRBNB Spotted",
                TopicArn = sns_arn,
                Message = (
                  "One of our Guest stayed at our property situated in " + actual_message['location'] +
                  " and was so mesmerized by the view that they couldn't resist themselves and stayed for a total of " +
                  str(date_difference.days) + " days.\n" +
                  "Whenever planning your next trip, consider this as your first priority.\n" +
                  "Refer to the property details for future reference:\n" +
                  "Property ID: " + actual_message['propertyId'] + ", " +
                  "Location: " + actual_message['location']
                  ),
                MessageStructure = "text"
               )
          else:
             print("User didn't stayed for more than a day.")
          
    
    else:
       print("No messages received")

    try:
       receipt_handle = response['ReceiptHandle']
       sqs_client.delete_message(
          QueueUrl = sqs_queue_url,
          ReceiptHandle = receipt_handle
       )
       print("Message deleted successfully")

    except Exception as err:
       print(f"Error deleting message: {err}")

    return {
       "statusCode":200,
       "Body":json.dumps("Message Processed Successfully")
    }

