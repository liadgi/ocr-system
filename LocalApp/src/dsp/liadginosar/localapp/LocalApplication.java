package dsp.liadginosar.localapp;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

import java.io.IOException;

public class LocalApplication {
    public static void main(String[] args) throws IOException, Exception {
        if (args.length == 2) {
            String filename = args[0];
            int numOfWorkers = Integer.parseInt(args[1]);

            System.out.println("Filename: " + filename);
            System.out.println("workers #:" + numOfWorkers);

            RunInstancesRequest runInstancesRequest =
                    new RunInstancesRequest();

            runInstancesRequest.withImageId("ami-1853ac65")
                    .withInstanceType(InstanceType.T2Micro)
                    .withMinCount(1)
                    .withMaxCount(1)
                    .withKeyName("dist-sys-course")
                    .withSecurityGroups("launch-wizard-1");

            AmazonEC2 amazonEC2Client =
            AmazonEC2ClientBuilder.standard()
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();

            RunInstancesResult result = amazonEC2Client.runInstances(
                    runInstancesRequest);

            System.out.println("Done.");

        } else {
            throw new Exception("Incorrect number of arguments.");
        }

    }
}
