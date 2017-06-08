package net.neophyte.messaging.kafka;

/**
 * 
 * @author shuvro
 *
 */
public abstract class Configuration {

	/* Plaintext port is 9092 */
	public static String BROKER_CLUSTER1 = "kafka-cluster1-node1.domain.com:1000,"
			+ "kafka-cluster1-node2.domain.com:9092";

	/* SSL port is 9093 */
	public static String BROKER_CLUSTER1_SSL = "kafka-cluster1-node1.domain.com:1001,"
		+ "kafka-cluster1-node2.domain.com:9093";
	
	public static final int IGNORE = -1;
	private static final String brokerUrl = "tcp://localhost:32780";
	private static final String topicName = "test_topic";
	private static final String truststoreLocation = "/Users/user1/cert/kafka.client.truststore.jks";
	private static final String keystoreLocation = "/Users/user1/cert/kafka.client.keystore.jks";
	/*
	 * Ideally the passwords should be read from somewhere such as a
	 * property file where the password is stored in an encrypted form and
	 * before assigning it here below the password is decrypted
	 */
	private static final String truststorePassword = "pass";
	private static final String keystorePassword = "pass";
	private static final String keyPassword = "pass";
	
	private final static int MSG_SIZE_IN_BYTES = 1024;// 1KB
	private final static int eventCount = 100;
	private final static long runTime = 20 * 1000; /* specify mili seconds in case of run for a specified time */
	private final static short addDelayTime = 0;// mili seconds
	private final static short maxDelayTime = 10 * 1000;// mili seconds
	private final static long pollTimeout = 10000;
	
	public static String getBrokerurl() {
		return brokerUrl;
	}

	public static String getTopicName() {
		return topicName;
	}

	public static int getMessageSize() {
		return MSG_SIZE_IN_BYTES;
	}

	public static int getEventCount() {
		return eventCount;
	}

	public static long getRuntime() {
		return runTime;
	}

	public static short getAdddelaytime() {
		return addDelayTime;
	}

	public static short getMaxdelaytime() {
		return maxDelayTime;
	}

	public static long getPolltimeout() {
		return pollTimeout;
	}

	public static String getTruststorelocation() {
		return truststoreLocation;
	}

	public static String getTruststorepassword() {
		return truststorePassword;
	}

	public static String getKeystorelocation() {
		return keystoreLocation;
	}

	public static String getKeystorepassword() {
		return keystorePassword;
	}

	public static String getKeypassword() {
		return keyPassword;
	}
}
