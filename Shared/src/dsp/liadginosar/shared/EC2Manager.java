package dsp.liadginosar.shared;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import java.util.Base64;

public class EC2Manager {
    private AmazonEC2 amazonEC2Client;

    private RunInstancesRequest runInstancesRequest;

    public EC2Manager(String userData){
        byte[] encodedBytes = Base64.getEncoder().encode(userData.getBytes());
        String userDataInitScriptBase64 = new String(encodedBytes);

        runInstancesRequest =
                new RunInstancesRequest();

        runInstancesRequest.withImageId("ami-1853ac65")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                //.withUserData(userDataInitScriptBase64)
                .withKeyName("dist-sys-course")
                //.withIamInstanceProfile(new IamInstanceProfileSpecification()
                //        .withName("myrole"))
                .withSecurityGroups("launch-wizard-1");

        if (userDataInitScriptBase64 != null) {
            runInstancesRequest.withUserData(userDataInitScriptBase64);
        }

        amazonEC2Client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .withRegion("us-east-1")
                        .build();
    }

    public String runInstance() {
        RunInstancesResult result = amazonEC2Client.runInstances(
                runInstancesRequest);

        return result.getReservation().getInstances().get(0).getInstanceId();
    }


    public void deactivateInstance(String workerInstanceId) {
        TerminateInstancesRequest request = new TerminateInstancesRequest()
                .withInstanceIds(workerInstanceId);

        amazonEC2Client.terminateInstances(request);
    }
}
