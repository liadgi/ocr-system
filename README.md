# OCR Project

A simple Java application illustrating usage of the AWS SDK for Java.

## Modules

LocalApp - Executable running from the local machine, executes the remote manager.

Manager - The node that runs on an EC2 instance and generates worker nodes by demand.

Shared - Common interface for AWS API, used by both LocalApp and Manager

Worker (ocr.py) - A script which extract text from images while using OCR library

## Prerequisites

install on each worker:

	wget https://s3.amazonaws.com/dsp-ocr/ocr.py -O ocr.py
    set IAM role - myrole

ubuntu user-data script:

	wget https://s3.amazonaws.com/dsp-ocr/ocr.py -O ocr.py
	export LC_ALL=C
    sudo apt-get update
	sudo apt -y install python
	sudo apt-get -y install python-pip
	sudo pip install boto3
	pip install pytesseract
	pip install requests
	sudo apt-get -y install tesseract-ocr
	python ocr.py

console output:

	cat /var/log/cloud-init-output.log

## Libraries

Java 8:

	aws-java-sdk-1.11.315.jar + third party lib

Python 2.7:

	requests - http client
	boto3 - aws sdk
	pytesseract - OCR library

## Details

Credentials are stored in an IAM role which is located on Amazon servers.

The program is not scalable will not work properly with 1 million workers, because there is only one manager which maps the tasks for all the workers and also aggregates its results. Therefore, its necessary to add more managers where each of them handles a different portion of the big file.

Not all fail-cases are handled.

Threads - ideal when multiple actions are required while waiting for a certain resource or executing a blocking action. For example, if sending a message to a queue would block until a result is returned, and we need to send messages to several queues, then using threads is the prefered way to go. In case that there is no need to wait for a resource, threads can cause overhead because of expensive context switch.

Manager node terminates all worker nodes, and Local application terminates Manager node.

There is a limit on the number of runnable instances on the free tier program.

The tasks of each node are implemented as defined in the instructions.

The manager waits for all the workers to finish, and the local application waits for the manager to give its cue.