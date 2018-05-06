package dsp.liadginosar.shared;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.List;

public class SQSManager {

    private AmazonSQS sqs;

    public SQSManager(){
        sqs = AmazonSQSClientBuilder.defaultClient();
        initQueues();
    }

    private void initQueues() {
        try {
            CreateQueueResult create_result = sqs.createQueue(Configuration.QUEUE_APP_TO_MANAGER);
            create_result = sqs.createQueue(Configuration.QUEUE_MANAGER_TO_WORKERS);
            create_result = sqs.createQueue(Configuration.QUEUE_WORKERS_TO_MANAGER);
            create_result = sqs.createQueue(Configuration.QUEUE_MANAGER_TO_APP);

            initReceivingQueue(Configuration.QUEUE_APP_TO_MANAGER);
            initReceivingQueue(Configuration.QUEUE_MANAGER_TO_WORKERS);
            initReceivingQueue(Configuration.QUEUE_WORKERS_TO_MANAGER);
            initReceivingQueue(Configuration.QUEUE_MANAGER_TO_APP);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
    }

    public void sendMessageToQueue(String queueName, String message) {
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message)
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
    }

    public List<Message> retreiveMessagesFromQueue(String queueName) {
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();

        // Enable long polling on a message receipt
        ReceiveMessageRequest receive_request = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);
        List<Message> messages = sqs.receiveMessage(receive_request).getMessages();

        return messages;
    }

    public void initReceivingQueue(String queueName) {
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();

        // Enable long polling on an existing queue
        SetQueueAttributesRequest set_attrs_request = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        sqs.setQueueAttributes(set_attrs_request);
    }

    public void deleteMessage(String queueName, Message m) {
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        sqs.deleteMessage(queueUrl, m.getReceiptHandle());
    }
}
