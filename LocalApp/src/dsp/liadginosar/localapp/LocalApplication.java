package dsp.liadginosar.localapp;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Date;
import java.util.List;

public class LocalApplication {

    private static final String QUEUE_APP_TO_MANAGER = "LocalAppToManagerQueue" + new Date().getTime();
    private AmazonSQS sqs;
    private String queueUrl;

    private String getKeyName(String file_path) {
        return Paths.get(file_path).getFileName().toString();
    }

    private void uploadImagesLinksFile(String file_path) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            String key_name = getKeyName(file_path);
            s3.putObject("dsp-ocr", key_name, file_path);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private void initQueue() {
        sqs = AmazonSQSClientBuilder.defaultClient();
        try {
            CreateQueueResult create_result = sqs.createQueue(QUEUE_APP_TO_MANAGER);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }

        queueUrl = sqs.getQueueUrl(QUEUE_APP_TO_MANAGER).getQueueUrl();

        // Enable long polling on an existing queue
        SetQueueAttributesRequest set_attrs_request = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        sqs.setQueueAttributes(set_attrs_request);
    }


    private void initManagerInstance(int numOfWorkers) {
        String userDataInitScript = "#!/bin/bash\nwget https://s3.amazonaws.com/dsp-ocr/Manager.jar -O ~/Manager.jar\njava -jar ~/Manager.jar " + numOfWorkers;

        byte[] encodedBytes = Base64.getEncoder().encode(userDataInitScript.getBytes());

        String userDataInitScriptBase64 = new String(encodedBytes);

        RunInstancesRequest runInstancesRequest =
                new RunInstancesRequest();

        runInstancesRequest.withImageId("ami-1853ac65")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                .withUserData(userDataInitScriptBase64)
                .withKeyName("dist-sys-course")
                .withSecurityGroups("launch-wizard-1");

        AmazonEC2 amazonEC2Client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();

        RunInstancesResult result = amazonEC2Client.runInstances(
                runInstancesRequest);
    }

    private void notifyManagerDownloadImagesFile(String file_path) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("new task " + getKeyName(file_path))
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
    }

    public static void main(String[] args) throws IOException, Exception {

        if (args.length == 2) {

            String file_path = args[0];
            int numOfWorkers = Integer.parseInt(args[1]);

            System.out.println("File path: " + file_path);
            System.out.println("workers #:" + numOfWorkers);

            LocalApplication app = new LocalApplication();

            app.uploadImagesLinksFile(file_path);
            app.initQueue();
            app.initManagerInstance(numOfWorkers);
            app.notifyManagerDownloadImagesFile(file_path);

            String s3FileLocation = app.retrieveOutputFileLocation();
            app.WriteFileToDisk(s3FileLocation);

            System.out.println("Done.");

        } else {
            throw new Exception("Incorrect number of arguments.");
        }

    }

    private String retrieveOutputFileLocation() {
        String s3FileLocation = null;
        // Enable long polling on a message receipt
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);
        List<Message> messages = sqs.receiveMessage(receive_request).getMessages();

        for (Message m : messages) {
            if (m.getBody().startsWith("done task")) {
                String[] arr = m.getBody().split("done task");
                s3FileLocation = arr[1];
                sqs.deleteMessage(queueUrl, m.getReceiptHandle());
            }
        }
        return s3FileLocation;
    }

    private void WriteFileToDisk(String s3FileLocation) {

    }
}
