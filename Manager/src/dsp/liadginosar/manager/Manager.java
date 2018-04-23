package dsp.liadginosar.manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.io.*;
import java.util.List;

//import com.amazonaws.services.sqs.model.AmazonSQSException;

public class Manager {
    private static final String QUEUE_APP_TO_MANAGER = "LocalAppToManagerQueue";
    Message m;
    private AmazonSQS sqs;
    private String queueUrl;

    private void initReceivingQueue() {
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);
        AmazonSQS sqs2;
        sqs = AmazonSQSClientBuilder.defaultClient();
        CreateQueueResult create_result = sqs.createQueue(QUEUE_APP_TO_MANAGER);
        try {


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

    private List<Message> retreiveMessagesFromQueue() {
        // Enable long polling on a message receipt
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);
        List<Message> messages = sqs.receiveMessage(receive_request).getMessages();

        return messages;
    }

    private void downloadInputFile(String key_name) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            S3Object o = s3.getObject("dsp-ocr", key_name);
            S3ObjectInputStream s3is = o.getObjectContent();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3is));
            String line = null;

            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            s3is.close();

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int numOfWorkers = Integer.parseInt(args[0]);
        Manager manager = new Manager();
        System.out.println("Manager started, # of workers: " + numOfWorkers);

        System.out.println("started listening on SQS:");
        manager.initReceivingQueue();
        List<Message> messages = manager.retreiveMessagesFromQueue();
        String inputFileLocation = manager.retrieveInputFileLocation(messages);

        if (inputFileLocation == null) {
            System.out.println("No input file.");
        } else {
            System.out.println("Download input file from S3:");
            manager.downloadInputFile(inputFileLocation);
        }
        // read file and map urls to workers

        

        System.out.println("Manager stopped.");
    }

    private String retrieveInputFileLocation(List<Message> messages) {
        String fileLocation = null;
        for (Message m : messages) {
            if (m.getBody().startsWith("new task")) {
                System.out.println("Message arrived: new task");
                String[] arr = m.getBody().split("new task ");
                fileLocation = arr[1];
                sqs.deleteMessage(queueUrl, m.getReceiptHandle());
            }
        }
        return fileLocation;
    }
}
