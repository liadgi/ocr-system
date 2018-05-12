package dsp.liadginosar.shared;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class EC2Manager {
    private AmazonEC2 amazonEC2Client;

    private RunInstancesRequest runInstancesRequest;
    private String INSTANCE_TYPE = "Type";

    public EC2Manager(String userData, String imageId, int numberOfInstances, String type){
        byte[] encodedBytes = Base64.getEncoder().encode(userData.getBytes());
        String userDataInitScriptBase64 = new String(encodedBytes);

        runInstancesRequest =
                new RunInstancesRequest();

        runInstancesRequest.withImageId(imageId)
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(numberOfInstances)
                .withMaxCount(numberOfInstances)
                .withTagSpecifications(new TagSpecification()
                        .withResourceType(ResourceType.Instance)
                        .withTags(new Tag(INSTANCE_TYPE, type)))
                .withInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate)
                .withUserData(userDataInitScriptBase64)
                .withKeyName("dist-sys-course")
                .withIamInstanceProfile(new IamInstanceProfileSpecification()
                        .withName("myrole"))
                .withSecurityGroups("launch-wizard-1");


        amazonEC2Client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .withRegion("us-east-1")
                        .build();
    }

    public List<String> runInstances() {
        RunInstancesResult result = amazonEC2Client.runInstances(
                runInstancesRequest);

        return result.getReservation().getInstances()
                .stream().map(worker -> worker.getInstanceId()).collect(Collectors.toList());
    }


    public void deactivateInstances(List<String> workerInstanceIds) {
        TerminateInstancesRequest request = new TerminateInstancesRequest()
                .withInstanceIds(workerInstanceIds);

        amazonEC2Client.terminateInstances(request);
    }

    public boolean isInstanceTypeUp(String instanceType) {

        boolean done = false;
        int pending = 0, running = 16;

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        while(!done) {
            DescribeInstancesResult response = amazonEC2Client.describeInstances(request);
            for(Reservation reservation : response.getReservations()) {
                for(Instance instance : reservation.getInstances()) {
                    Tag typeTag = instance.getTags().get(0);
                    if (typeTag.getKey().equals(INSTANCE_TYPE) && typeTag.getValue().equals(instanceType)) {
                        int state = instance.getState().getCode();
                        return (state == pending || state == running);
                    }
                }
            }

            request.setNextToken(response.getNextToken());

            if(response.getNextToken() == null) {
                done = true;
            }
        }
        return false;
    }
}
