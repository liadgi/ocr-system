package dsp.liadginosar.manager;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class EC2Creator {
    private AmazonEC2 amazonEC2Client;

    private RunInstancesRequest runInstancesRequest;

    private void prepareEC2(){
        runInstancesRequest =
                new RunInstancesRequest();

        runInstancesRequest.withImageId("ami-1853ac65")
                .withInstanceType(InstanceType.T2Micro)
                .withMinCount(1)
                .withMaxCount(1)
                //.withUserData(userDataInitScriptBase64)
                .withKeyName("dist-sys-course")
                .withSecurityGroups("launch-wizard-1");

        amazonEC2Client =
                AmazonEC2ClientBuilder.standard()
                        .withCredentials(new DefaultAWSCredentialsProviderChain())
                        .build();
    }

    public EC2Creator(){
        prepareEC2();
    }

    public RunInstancesResult runInstance() {
        RunInstancesResult result = amazonEC2Client.runInstances(
                runInstancesRequest);

        return result;
    }


}
