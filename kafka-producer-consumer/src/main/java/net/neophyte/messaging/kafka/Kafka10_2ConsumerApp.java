package net.neophyte.messaging.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author shuvro
 *
 */
public class Kafka10_2ConsumerApp extends AbstractKafkaClient {
	private final Logger logger = LoggerFactory.getLogger(Kafka10_2ConsumerApp.class);
	private final static String topicName = "test_topic";
	private static long TEST_RUN_TIME = 1000 * 60 * 2;// two minutes
	
	public static void main(String[] args) {
		Kafka10_2ConsumerApp kafkaConsumer = new Kafka10_2ConsumerApp();
		try {
			/* consumes at max 1000 messages, if available */
			kafkaConsumer.receiveNumOfMessages(topicName, 1000);
			/* consumes messages for TEST_RUN_TIME minutes */
			kafkaConsumer.receiveMessagesFor(topicName, TEST_RUN_TIME);
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void receiveNumOfMessages(String topicName, int messageCount) throws Exception {
		receiveMessages(topicName, messageCount, -1);
	}

	private void receiveMessagesFor(String topicName, long runTime) throws Exception {
		receiveMessages(topicName, -1, runTime);
	}

	private boolean moreMessagesToReceive(long messageCount) {
		if (messageCount == -1) {
			return true;
		}
		return msgs.get() < messageCount;
	}
	
	public void receiveMessages(String topicName, int messageCount, long runTime) throws Exception {
	    logger.info("Starting...");
        
		startTime = System.currentTimeMillis();
		
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
				getConsumerProperties(BROKER_CLUSTER.CLUSTER1, false, false));
	    
	    consumer.subscribe(Arrays.asList(topicName));
	    
	    while(runTimeRemains(runTime) && moreMessagesToReceive(messageCount)) {
    	    try {
    	        ConsumerRecords<String, String> records = consumer.poll(10000);
    	        msgs.addAndGet(records.count());
    	        records.forEach(r -> System.out.println(" Msg: " + r.toString()));
    	        records.forEach(r -> System.out.println("Consumed from Partition: " 
    	        		+ r.partition() + " Offset: " + r.offset()
    	        		+ " Msg: " + r.toString()));    	        
    	        consumer.commitSync();
    	    }catch(Exception e){
    	        errors.incrementAndGet();
    	        logger.info(e.getMessage());
    	    }
	    }

        long totalRunTime = System.currentTimeMillis() - startTime;
        logger.info("Total Run Time: " + Util.getHh_Mm_Ss_ssss_Time(totalRunTime) 
                + ", Total Messages received: " + msgs.get());
        logger.info("Total errors: " + errors.get());
        
        if(Util.isNotNull(consumer)) {
            consumer.close();
        }
        logger.info("Exiting...");
	}
}


