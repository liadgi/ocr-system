package dsp.liadginosar.manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.model.Message;
import dsp.liadginosar.shared.Configuration;
import dsp.liadginosar.shared.EC2Manager;
import dsp.liadginosar.shared.SQSManager;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Manager {

    private SQSManager sqsManager;
    private EC2Manager ec2Manager;

    private int imagesPerWorker;
    private int totalNumOfImages;

    private List<String> workerIds;

    public Manager(int imagesPerWorker){
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

            int lineCounter = 0;
            ArrayList<String> messages = new ArrayList<>();
            while (((line = bufferedReader.readLine())) != null) {
                messages.add("new image task " + line);
                lineCounter++;
            }
            this.totalNumOfImages = lineCounter;
            int numOfWorkers = (lineCounter / imagesPerWorker)
                        + ((lineCounter % imagesPerWorker == 0) ? 0 : 1);

            // init script for each worker
            String userData =
                    "#!/bin/bash\n" +
                            "wget https://s3.amazonaws.com/dsp-ocr/ocr.py -O ocr.py\n" +
                            "export LC_ALL=C\n" +
                            "sudo apt-get update\n" +
                            "sudo apt -y install python\n" +
                            "sudo apt-get -y install python-pip\n" +
                            "sudo pip install boto3\n" +
                            "pip install pytesseract\n" +
                            "pip install requests\n" +
                            "pip install pyopenssl\n" +
                            "sudo apt-get -y install tesseract-ocr\n" +
                            "python ocr.py";

            ec2Manager = new EC2Manager(userData, "ami-43a15f3e", numOfWorkers, "Worker");
            this.workerIds = ec2Manager.runInstances();

            sqsManager.sendMessageBatchToQueue(Configuration.QUEUE_MANAGER_TO_WORKERS, messages);

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
        ec2Manager.deactivateInstances(this.workerIds);
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

            uploadFile(file);

            notifyLocalApp();

            deactivateWorkers();
        }

        System.out.println("Manager stopped.");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int imagesPerWorker = Integer.parseInt(args[0]);
        Manager manager = new Manager(imagesPerWorker);
        manager.startManager();
    }

    private void notifyLocalApp() {
        sqsManager.sendMessageToQueue(Configuration.QUEUE_MANAGER_TO_APP, "done task output.html");
    }

    private void uploadFile(File file) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            s3.putObject(new PutObjectRequest("dsp-ocr", "output.html", file));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private File createSummaryFile() {
        File file = new File("output.html");
        try {
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write("<html>\n");
            output.write("<title>OCR</title>\n");
            output.write("<body>\n");
            int numOfImagesReceived = 0;

            while (numOfImagesReceived < this.totalNumOfImages) {
                List<Message> messages = sqsManager.retreiveMessagesFromQueue(Configuration.QUEUE_WORKERS_TO_MANAGER);

                if (messages != null) {
                    for (Message m : messages) {
                        String doneText = "done image task ", failedText = "failed image task ";
                        if (m.getBody().startsWith(doneText)) {
                            System.out.println("Message #" + numOfImagesReceived + " arrived: " + doneText);
                            String[] arr = m.getBody().split(doneText,2);
                            arr = arr[1].split(" ",2);
                            String url = arr[0];
                            String text = arr[1];
                            String elem = createElement(url, text);
                            output.write(elem);
                            output.flush();

                        }
                        else if (m.getBody().startsWith(failedText)) {
                            String[] arr = m.getBody().split(failedText,2);
                            System.out.println("Message #" + numOfImagesReceived + " arrived: " + failedText + ":\n" + arr[1]);
                        }

                        sqsManager.deleteMessage(Configuration.QUEUE_WORKERS_TO_MANAGER, m);
                        numOfImagesReceived++;
                    }
                }
            }
            output.write("</body>\n");
            output.write("</html>");
            output.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return file;
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

    private String createElement(String url, String text) {
        String elem =
                "<p>\n" +
                    "<img src=\"" + url + "\" style=\"max-height: 500px; max-width: 500px;\"><br/>\n" +
                        text +
                "</p>\n";
        return elem;
    }
}
