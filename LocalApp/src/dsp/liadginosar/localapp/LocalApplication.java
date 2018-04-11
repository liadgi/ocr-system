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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Base64;

public class LocalApplication {

    private void uploadImagesLinksFile(String file_path) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            String key_name = Paths.get(file_path).getFileName().toString();
            s3.putObject("dsp-ocr", key_name, file_path);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    private void updateQueue(){

    }

    private void startManager(int numOfWorkers) {
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

    public static void main(String[] args) throws IOException, Exception {

        if (args.length == 2) {

            String file_path = args[0];
            int numOfWorkers = Integer.parseInt(args[1]);

            System.out.println("File path: " + file_path);
            System.out.println("workers #:" + numOfWorkers);

            LocalApplication app = new LocalApplication();

            app.uploadImagesLinksFile(file_path);
            app.updateQueue();
            app.startManager(numOfWorkers);

            System.out.println("Done.");

        } else {
            throw new Exception("Incorrect number of arguments.");
        }

    }
}
