package net.neophyte.messaging.kafka;

import java.util.UUID;

import net.neophyte.messaging.kafka.utils.MessageUtil;
import net.neophyte.messaging.kafka.utils.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 
 * @author shuvro
 *
 */
public final class Kafka10_2ProducerApp extends AbstractKafkaClient {
	private final static Logger logger = LoggerFactory
			.getLogger(Kafka10_2ProducerApp.class);

	public static void main(String[] args) {
		Kafka10_2ProducerApp kafkaProducer = new Kafka10_2ProducerApp();
		try {
			/* produces configured # of events in sync mode */
			kafkaProducer.produceNumEventsSyncMode(BROKER_CLUSTER.CLUSTER1,
					Configuration.getTopicName(),
					Configuration.getMessageSize(),
					Configuration.getEventCount());
			
			/* produces configured # of events in async mode */
			kafkaProducer.produceNumEventsAsyncMode(BROKER_CLUSTER.CLUSTER1,
					Configuration.getTopicName(),
					Configuration.getMessageSize(),
					Configuration.getEventCount());
			
			/* produces events for configured run time minutes in sync mode */ 
			kafkaProducer.produceEventsSyncModeForDuration(BROKER_CLUSTER.CLUSTER1,
					Configuration.getTopicName(),
					Configuration.getMessageSize(), Configuration.getRuntime());
			
			/* produces events for configured run time minutes in async mode */ 
			kafkaProducer.produceEventsAsyncModeForDuration(BROKER_CLUSTER.CLUSTER1,
					Configuration.getTopicName(),
					Configuration.getMessageSize(), Configuration.getRuntime());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Produces the specified number of events in sync mode
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param msgSize The message size in bytes
	 * @param numOfEvents The number of events to produce
	 * @throws Exception An exception in case of failure
	 */
	private void produceNumEventsSyncMode(BROKER_CLUSTER brokerCluster, String topicName, int msgSize,
			long numOfEvents) throws Exception {

		produceEvents(brokerCluster, topicName, msgSize, numOfEvents, true,
				Configuration.IGNORE);
	}

	/**
	 * Produces the specified number of events in async mode
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param msgSize The message size in bytes
	 * @param numOfEvents The number of events to produce
	 * @throws Exception An exception in case of failure
	 */
	private void produceNumEventsAsyncMode(BROKER_CLUSTER brokerCluster, String topicName, int msgSize,
			long numOfEvents) throws Exception {
		produceEvents(brokerCluster, topicName, msgSize, numOfEvents, false,
				Configuration.IGNORE);
	}

	/**
	 * Produces for the specified run time in sync mode
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param msgSize The message size in bytes
	 * @param runTime The run time in mili seconds
	 * @throws Exception An exception in case of failure
	 */
	private void produceEventsSyncModeForDuration(BROKER_CLUSTER brokerCluster, String topicName, int msgSize,
			long runTime) throws Exception {
		produceEvents(brokerCluster, topicName, msgSize, Configuration.IGNORE, true, runTime);
	}

	/**
	 * Produces for the specified run time in async mode
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param msgSize The message size in bytes
	 * @param runTime The run time in mili seconds
	 * @throws Exception An exception in case of failure
	 */
	private void produceEventsAsyncModeForDuration(BROKER_CLUSTER brokerCluster, String topicName, int msgSize,
			long runTime) throws Exception {
		produceEvents(brokerCluster, topicName, msgSize, Configuration.IGNORE, false, runTime);
	}

	/**
	 * Returns if all events have been produced or not 
	 * 
	 * @param numOfEvents The number of events to produce
	 * @return <code>true</code> if not all events produced otherwise
	 * <code>false</code>
	 */
	private boolean moreEventsToProduced(long numOfEvents) {
		if (numOfEvents == Configuration.IGNORE) {
			return false;
		}
		return msgs.get() < numOfEvents;
	}

	/**
	 * Produce events
	 * 
	 * @param brokerCluster The broker Cluster
	 * @param topicName The topic name
	 * @param msgSize The message size in bytes
	 * @param numOfEvents The number of events to produce
	 * @param isSyncMode Whether sync/async mode 
	 * @param runTime The run time in mili seconds
	 * @throws Exception An exception in case of failure
	 */
	private void produceEvents(BROKER_CLUSTER brokerCluster, String topicName, int msgSize,
			long numOfEvents, boolean isSyncMode, long runTime)
			throws Exception {
		logger.info("Starting...");

		startTime = System.currentTimeMillis();

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				getProducerProperties(brokerCluster, false, false));

		String message = MessageUtil.generateMessage(msgSize);
		
		while (runTimeRemains(runTime) || moreEventsToProduced(numOfEvents)) {
			try {
				String uuid = UUID.randomUUID().toString();
				if (isSyncMode) {
					RecordMetadata metadata = producer.send(
							new ProducerRecord<String, String>(topicName, uuid,
									message)).get();
					msgs.incrementAndGet();
					System.out.println("Produced to Partition: "
							+ metadata.partition() + " Offset: "
							+ metadata.offset());
				} else {
					producer.send(new ProducerRecord<String, String>(topicName,
							uuid, message), new ProducerCallback());
				}
			} catch (Exception e) {
				errors.incrementAndGet();
				logger.error(e.getMessage());
			}

			if (Configuration.getAdddelaytime() > 0
					&& Configuration.getAdddelaytime() <= Configuration
							.getMaxdelaytime()) {
				try {
					Thread.sleep(Configuration.getAdddelaytime());
				} catch (InterruptedException ie) {
					logger.error(ie.getMessage());
				}
			}
		}

		long totalRunTime = System.currentTimeMillis() - startTime;
		logger.info("Total Run Time: "
				+ Util.getHh_Mm_Ss_ssss_Time(totalRunTime)
				+ ", Total Messages Sent: " + msgs.get());
		logger.info("Total errors: " + errors.get());

		if (Util.isNotNull(producer)) {
			producer.close();
		}

		logger.info("Exiting...");
	}

	/**
	 * 
	 * An inner class, implements the Callback interface to override 
	 * the OnCompletion method receive message producing status in 
	 * async mode 
	 */
	private class ProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata metadata, Exception e) {
			if (e == null) {
				System.out.println("Produced to Partition: "
						+ metadata.partition() + " Offset: "
						+ metadata.offset());
				msgs.incrementAndGet();
			} else {
				logger.error(e.getMessage());
				errors.incrementAndGet();
			}
		}
	}
}
