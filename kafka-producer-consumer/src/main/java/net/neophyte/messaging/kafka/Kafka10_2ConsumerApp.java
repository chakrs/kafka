package net.neophyte.messaging.kafka;

import java.util.Arrays;

import net.neophyte.messaging.kafka.utils.Util;

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
	private final static Logger logger = LoggerFactory.getLogger(Kafka10_2ConsumerApp.class);

	public static void main(String[] args) {
		Kafka10_2ConsumerApp kafkaConsumer = new Kafka10_2ConsumerApp();
		try {
			/* consumes at max configured # of events, if available */
			kafkaConsumer.receiveNumOfEvents(BROKER_CLUSTER.CLUSTER1, Configuration.getTopicName(), 
					Configuration.getEventCount());
			
			/* consumes events for configured run time minutes */
			kafkaConsumer.receiveEventsForDuration(BROKER_CLUSTER.CLUSTER1, Configuration.getTopicName(), 
					Configuration.getRuntime());
		}catch(Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Receive number of specified events
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param numOfEvents The number of events to produce
	 * @throws Exception An exception in case of failure
	 */
	private void receiveNumOfEvents(BROKER_CLUSTER brokerCluster, String topicName, int numOfEvents) throws Exception {
		receiveEvents(brokerCluster, topicName, numOfEvents, Configuration.IGNORE);
	}

	/**
	 * Receive events for the specified run time
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param runTime The run time in mili seconds
	 * @throws Exception An exception in case of failure
	 */
	private void receiveEventsForDuration(BROKER_CLUSTER brokerCluster, String topicName, long runTime) throws Exception {
		receiveEvents(brokerCluster, topicName, Configuration.IGNORE, runTime);
	}

	/**
	 * 
	 * @param numOfEvents The number of events to produce
	 * @return <code>true</code> if not all events produced otherwise
	 * <code>false</code>
	 */
	private boolean moreEventsToReceive(long numOfEvents) {
		if (numOfEvents == Configuration.IGNORE) {
			return false;
		}
		return msgs.get() < numOfEvents;
	}
	
	/**
	 * Receive events from kafka broker cluster on the specified topic
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param numOfEvents The number of events to produce
	 * @param runTime The run time in mili seconds
	 * @throws Exception An exception in case of failure
	 */
	public void receiveEvents(BROKER_CLUSTER brokerCluster, String topicName, int numOfEvents, long runTime) throws Exception {
	    logger.info("Starting...");
        
		startTime = System.currentTimeMillis();
		
	    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
				getConsumerProperties(brokerCluster, false, false));
	    
	    consumer.subscribe(Arrays.asList(topicName));
	    
	    while(runTimeRemains(runTime) || moreEventsToReceive(numOfEvents)) {
    	    try {
    	        ConsumerRecords<String, String> records = consumer.poll(
    	        		Configuration.getPolltimeout());
    	        msgs.addAndGet(records.count());
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


