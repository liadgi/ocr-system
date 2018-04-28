package dsp.liadginosar.manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.model.Message;
import dsp.liadginosar.shared.Configuration;

import java.io.*;
import java.util.List;

//import com.amazonaws.services.sqs.model.AmazonSQSException;

public class Manager {

    private SQSManager sqsManager;
    private EC2Creator ec2Creator;

    private int imagesPerWorker;
    private int totalNumOfImages;

    public Manager(int imagesPerWorker){
        ec2Creator = new EC2Creator();
        sqsManager = new SQSManager();
        this.imagesPerWorker = imagesPerWorker;
    }


    private void splitWorkToWorkers(String key_name) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            S3Object o = s3.getObject("dsp-ocr", key_name);
            S3ObjectInputStream s3is = o.getObjectContent();

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s3is));
            String line;

            int lineCounter = imagesPerWorker + 1;
            int totalNumOfImages = 0;

            while ((line = bufferedReader.readLine()) != null) {
                if(lineCounter > imagesPerWorker) {
                    ec2Creator.runInstance();
                    totalNumOfImages++;
                    lineCounter = 0;
                }
                sqsManager.sendMessageToQueue(Configuration.QUEUE_MANAGER_TO_WORKERS,"new image task " + line);
                lineCounter++;
            }
            this.totalNumOfImages = totalNumOfImages;

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

    private void deactivateWorkers() {

    }

    private void startManager(){
        System.out.println("Manager started, # of images per worker: " + imagesPerWorker);

        System.out.println("started listening on SQS:");
        List<Message> messages = sqsManager.retreiveMessagesFromQueue(Configuration.QUEUE_APP_TO_MANAGER);
        String inputFileLocation = retrieveInputFileLocation(messages);

        if (inputFileLocation == null) {
            System.out.println("No input file.");
        } else {
            System.out.println("Download input file from S3:");

            splitWorkToWorkers(inputFileLocation);

            File file = createSummaryFile();

            deactivateWorkers();

            uploadFile(file);

            notifyLocalApp();
        }

        System.out.println("Manager stopped.");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int imagesPerWorker = Integer.parseInt(args[0]);
        Manager manager = new Manager(imagesPerWorker);
        manager.startManager();
    }

    private void notifyLocalApp() {
        sqsManager.sendMessageToQueue(Configuration.QUEUE_MANAGER_TO_APP, "done task");
    }

    private void uploadFile(File file) {

    }

    private File createSummaryFile() {
        List<Message> messages = sqsManager.retreiveMessagesFromQueue(Configuration.QUEUE_WORKERS_TO_MANAGER);

        StringBuilder builder = new StringBuilder();

        for (Message m : messages) {
            if (m.getBody().startsWith("done image task")) {
                System.out.println("Message arrived: done image task");
                String[] arr = m.getBody().split("done image task ");

                builder.append(arr[1]).append("\n");

                sqsManager.deleteMessage(Configuration.QUEUE_WORKERS_TO_MANAGER, m);
            }
        }
        return null;
    }

    private String retrieveInputFileLocation(List<Message> messages) {
        String fileLocation = null;
        for (Message m : messages) {
            if (m.getBody().startsWith("new task")) {
                System.out.println("Message arrived: new task");
                String[] arr = m.getBody().split("new task ");
                fileLocation = arr[1];
                sqsManager.deleteMessage(Configuration.QUEUE_APP_TO_MANAGER, m);
            }
        }
        return fileLocation;
    }
}
