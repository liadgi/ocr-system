package dsp.liadginosar.shared;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SQSManager {

    private AmazonSQS sqs;
    private Map<String, String> queueNameToUrl;
    private SendMessageRequest sendMessageRequest = new SendMessageRequest();

    // Enable long polling on a message receipt
    private ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
            .withMaxNumberOfMessages(10)
            .withWaitTimeSeconds(20);

    public SQSManager(){
        queueNameToUrl = new HashMap<>();
        sqs = AmazonSQSClientBuilder.defaultClient();
        initQueues();
    }


    private void initQueues() {
        try {
            //CreateQueueResult create_result = sqs.createQueue(Configuration.QUEUE_APP_TO_MANAGER);
            //create_result = sqs.createQueue(Configuration.QUEUE_MANAGER_TO_WORKERS);
            //create_result = sqs.createQueue(Configuration.QUEUE_WORKERS_TO_MANAGER);
            //create_result = sqs.createQueue(Configuration.QUEUE_MANAGER_TO_APP);

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

    public void initReceivingQueue(String queueName) {
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        queueNameToUrl.put(queueName, queueUrl);

        // Enable long polling on an existing queue
        /*SetQueueAttributesRequest set_attrs_request = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        sqs.setQueueAttributes(set_attrs_request);*/
    }

    public void sendMessageToQueue(String queueName, String message) {
        String queueUrl = queueNameToUrl.get(queueName);
        sendMessageRequest
                .withQueueUrl(queueUrl)
                .withMessageBody(message);
        sqs.sendMessage(sendMessageRequest);

        // check this out:
        //sqs.sendMessageBatch()
    }

    public void sendMessageBatchToQueue(String queueName, ArrayList<String> messages) {
        String queueUrl = queueNameToUrl.get(queueName);

        int index = 0, totalMessages = messages.size(), messagesLeft;
        while (index < totalMessages) {
            messagesLeft = Math.min(10, totalMessages - index);
            Collection<SendMessageBatchRequestEntry> entries =
                    IntStream.range(index, index + messagesLeft)
                            .mapToObj(i -> new SendMessageBatchRequestEntry(Integer.toString(i), messages.get(i)))
                            .collect(Collectors.toList());

            // Send multiple messages to the queue
            SendMessageBatchRequest send_batch_request = new SendMessageBatchRequest()
                    .withQueueUrl(queueUrl)
                    .withEntries(entries);
                        //.withDelaySeconds(10));
            sqs.sendMessageBatch(send_batch_request);

            index+=10;
        }

    }

    public List<Message> retreiveMessagesFromQueue(String queueName) {
        String queueUrl = queueNameToUrl.get(queueName);

        receiveMessageRequest.withQueueUrl(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

        return messages;
    }

    public void deleteMessage(String queueName, Message m) {
        String queueUrl = queueNameToUrl.get(queueName);
        sqs.deleteMessage(queueUrl, m.getReceiptHandle());
    }
}
