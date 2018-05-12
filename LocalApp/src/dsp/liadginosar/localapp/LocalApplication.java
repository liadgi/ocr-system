package dsp.liadginosar.localapp;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.model.Message;
import dsp.liadginosar.shared.Configuration;
import dsp.liadginosar.shared.EC2Manager;
import dsp.liadginosar.shared.SQSManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class LocalApplication {

    private SQSManager sqsManager;
    private EC2Manager ec2Manager;
    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private List<String> instanceIds;

    public LocalApplication() {
        this.sqsManager = new SQSManager();
    }
    private String getKeyName(String file_path) {
        return Paths.get(file_path).getFileName().toString();
    }

    private void uploadImagesLinksFile(String file_path) {
        System.out.println("uploadImagesLinksFile");
        try {
            String key_name = getKeyName(file_path);

            PutObjectRequest request = new PutObjectRequest("dsp-ocr", key_name, new File(file_path));

            s3.putObject(request.withCannedAcl(CannedAccessControlList.PublicRead));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private void initManagerInstance(int imagesPerWorker) {
        String userDataInitScript =
                "#!/bin/bash\n" +
                "sudo yum install java-1.8.0 -y\n" +
                "sudo yum remove java-1.7.0-openjdk -y\n" +
                "wget https://s3.amazonaws.com/dsp-ocr/Manager.jar -O ~/Manager.jar\n" +
                "echo executing manager... \n" +
                "java -jar ~/Manager.jar " + imagesPerWorker;

        String amazonAMIImageId = "ami-1853ac65";
        this.ec2Manager = new EC2Manager(userDataInitScript, amazonAMIImageId, 1, "Manager");

        this.instanceIds =  this.ec2Manager.runInstances();

    }

    private void notifyManagerToDownloadImagesFile(String file_path) {
        this.sqsManager.sendMessageToQueue(Configuration.QUEUE_APP_TO_MANAGER, "new task " + getKeyName(file_path));
    }

    public static void main(String[] args) throws IOException, Exception {

        if (args.length == 2) {

            String file_path = args[0];
            int imagesPerWorker = Integer.parseInt(args[1]);

            System.out.println("File path: " + file_path);
            System.out.println("Images per worker #:" + imagesPerWorker);

            LocalApplication app = new LocalApplication();

            app.uploadImagesLinksFile(file_path);
            app.initManagerInstance(imagesPerWorker);
            app.notifyManagerToDownloadImagesFile(file_path);

            String s3FileKey = app.retrieveOutputFileKey();
            app.WriteFileToDisk(s3FileKey);
            app.deactivateManager();


            System.out.println("Done.");

        } else {
            throw new Exception("Incorrect number of arguments.");
        }

    }


    private void deactivateManager() {
        this.ec2Manager.deactivateInstances(this.instanceIds);
    }

    private String retrieveOutputFileKey() {
        System.out.println("Retrieving output file location");


        String s3FileLocation = null;

        List<Message> messages;
        while ((messages = sqsManager.retreiveMessagesFromQueue(Configuration.QUEUE_MANAGER_TO_APP)).size() == 0);

        for (Message m : messages) {
            if (m.getBody().startsWith("done task")) {
                String[] arr = m.getBody().split("done task ");
                s3FileLocation = arr[1];
                this.sqsManager.deleteMessage(Configuration.QUEUE_MANAGER_TO_APP, m);
            }
        }
        return s3FileLocation;
    }

    private void WriteFileToDisk(String s3FileKey) {
        try {
            S3Object o = s3.getObject("dsp-ocr", s3FileKey);
            S3ObjectInputStream s3is = o.getObjectContent();

            // Create file
            FileOutputStream fos = new FileOutputStream(new File(s3FileKey));
            byte[] read_buf = new byte[1024];
            int read_len;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();

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
}
