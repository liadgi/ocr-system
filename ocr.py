import requests
import boto3
import os


try:
    import Image
except ImportError:
    from PIL import Image
import pytesseract

sqs = boto3.client('sqs', region_name='us-east-1')

def extractText(url, filename):
	try:
		img_data = requests.get(url).content
		with open(filename, 'wb') as handler:
			handler.write(img_data)

		# Simple image to string
		text = pytesseract.image_to_string(Image.open(filename))
		os.remove(filename)
	except Exception as e:
		print e
		text = "couldn't extract text."
	
	return text.encode('utf-8')

def sendMessage(text):
	response = sqs.get_queue_url(
	    QueueName='WorkersToManagerQueue'
	)

	queue_url = response["QueueUrl"]
	# Send message to SQS queue
	response = sqs.send_message(
	    QueueUrl=queue_url,
	    DelaySeconds=10,
	    MessageBody=(
	        "done image task " + text
	    )
	)


def receiveMessage():
	response = sqs.get_queue_url(
	    QueueName="ManagerToWorkersQueue"
	)

	queue_url = response["QueueUrl"]

	while not 'Messages' in response:
		response = sqs.receive_message(
		    QueueUrl=queue_url,
		    AttributeNames=[
		        'SentTimestamp'
		    ],
		    MaxNumberOfMessages=1,
		    MessageAttributeNames=[
		        'All'
		    ],
		    VisibilityTimeout=0,
		    WaitTimeSeconds=20
		)

	message = response['Messages'][0]
	receipt_handle = message['ReceiptHandle']

	# Delete received message from queue
	sqs.delete_message(
	    QueueUrl=queue_url,
	    ReceiptHandle=receipt_handle
	)
	print('Received and deleted message: %s' % message["Body"])
	return message

if __name__ == "__main__":
	while True:
		message = receiveMessage()
		words = message["Body"].split("new image task ")
		url = words[1];
		filename = url.rsplit('/', 1)[-1]
		imageText = extractText(url, filename)
		sendMessage(url + " " + imageText)
