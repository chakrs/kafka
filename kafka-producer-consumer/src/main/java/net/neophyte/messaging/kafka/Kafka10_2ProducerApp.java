package net.neophyte.messaging.kafka;

import java.util.UUID;

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
	private final Logger logger = LoggerFactory.getLogger(Kafka10_2ProducerApp.class);
	private final static String topicName = "test_topic";
	private final static short addDelayTime = 0;// second
	private final static short MAX_DELAY_TIME = 10;// second
	private final static int MSG_SIZE_IN_BYTES = 1024;// 1KB

	private static long TEST_RUN_TIME = 1000 * 60 * 2;// two minutes

	public static void main(String[] args) {
		Kafka10_2ProducerApp kafkaProducer = new Kafka10_2ProducerApp();
		try {
			/* produces 1000 messages in sync mode */
			kafkaProducer.produceNumMessageSyncMode(topicName, MSG_SIZE_IN_BYTES, 1000);
			/* produces 1000 messages in async mode */
			kafkaProducer.produceNumMessageAsyncMode(topicName, MSG_SIZE_IN_BYTES, 1000);
			/* produces messages for TEST_RUN_TIME minutes in sync mode */
			kafkaProducer.produceMessageSyncModeFor(topicName, MSG_SIZE_IN_BYTES, TEST_RUN_TIME);
			/* produces messages for TEST_RUN_TIME minutes in async mode */
			kafkaProducer.produceMessageAsyncModeFor(topicName, MSG_SIZE_IN_BYTES, TEST_RUN_TIME);
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private void produceNumMessageSyncMode(String topicName, int msgSize,
			long numOfEvents) throws Exception {
		produceMessage(topicName, msgSize, numOfEvents, true, -1);
	}

	private void produceNumMessageAsyncMode(String topicName, int msgSize,
			long numOfEvents) throws Exception {
		produceMessage(topicName, msgSize, numOfEvents, false, -1);
	}

	private void produceMessageSyncModeFor(String topicName, int msgSize,
			long runTime) throws Exception {
		produceMessage(topicName, msgSize, -1, true, runTime);
	}

	private void produceMessageAsyncModeFor(String topicName, int msgSize,
			long runTime) throws Exception {
		produceMessage(topicName, msgSize, -1, false, runTime);
	}

	private boolean allMessagesProduced(long numOfEvents) {
		if (numOfEvents == -1) {
			return false;
		}
		return msgs.get() < numOfEvents;
	}

	@SuppressWarnings("unused")
	private void produceMessage(String topicName, int msgSize,
			long numOfEvents, boolean isSyncMode, long runTime) throws Exception {
		logger.info("Starting...");

		startTime = System.currentTimeMillis();

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(
				getProducerProperties(BROKER_CLUSTER.CLUSTER1, false, false));

		String message = MessageUtil.generateMessage(msgSize);
		while (runTimeRemains(runTime) && allMessagesProduced(numOfEvents)) {
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

			if (addDelayTime > 0 && addDelayTime <= MAX_DELAY_TIME) {
				try {
					Thread.sleep(addDelayTime * 1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
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
