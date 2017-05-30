package net.neophyte.messaging.kafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author shuvro
 *
 */
public class AbstractKafkaClient {

	/* Plaintext port is 1000 */
	protected static String BROKER_CLUSTER1 = "kafka-cluster1-node1.domain.com:1000,"
			+ "kafka-cluster1-node2.domain.com:1000";

	/* SSL port is 1001 */
	protected static String BROKER_CLUSTER1_SSL = "kafka-cluster1-node1.domain.com:1001,"
			+ "kafka-cluster1-node2.domain.com:1001";

	protected AtomicInteger msgs = new AtomicInteger(0);
	protected AtomicInteger errors = new AtomicInteger(0);
	protected static long startTime = 0;

	enum BROKER_CLUSTER {
		CLUSTER1("cluster1"), CLUSTER2("cluster2"), CLUSTER3("cluster3");

		String brokerCluster;

		BROKER_CLUSTER(String brokerCluster) {
			this.brokerCluster = brokerCluster;
		}

		public String getBrokerCluster() {
			return brokerCluster;
		}
	};

	protected static boolean runTimeRemains(long runTime) {
		if (runTime == -1) {
			return true;
		}
		return (System.currentTimeMillis() - startTime) < runTime;
	}

	protected Properties getProducerProperties(BROKER_CLUSTER brokerCluster, boolean isSsl,
			boolean useClientAuth) throws Exception {
		Properties props = new Properties();
		setBootstrapServersProperties(props, brokerCluster, isSsl);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		if (isSsl) {
			setSSLProperties(props);
		}

		if (useClientAuth) {
			setClientAuthProperties(props);
		}

		return props;
	}

	protected Properties getConsumerProperties(BROKER_CLUSTER brokerCluster, boolean isSsl,
			boolean useClientAuth) throws Exception {
		Properties props = new Properties();
		setBootstrapServersProperties(props, brokerCluster, isSsl);
		props.put("group.id", "test-consumer-group");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "10000");
		props.put("max.poll.records", "1000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");

		if (isSsl) {
			setSSLProperties(props);
		}

		if (useClientAuth) {
			setClientAuthProperties(props);
		}

		return props;
	}

	private void setBootstrapServersProperties(Properties props, BROKER_CLUSTER brokerCluster,
			boolean isSsl) throws Exception {
		String brokers = null;
		switch (brokerCluster) {
			case CLUSTER1:
				if (isSsl) {
					brokers = BROKER_CLUSTER1_SSL;
				} else {
					brokers = BROKER_CLUSTER1;
				}
				break;
				
			default:
				throw new Exception("Undefined cluster was specified!");
		}
		props.put("bootstrap.servers", brokers);
	}

	private void setSSLProperties(Properties props) {
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location",
				"/Users/user1/cert/kafka.client.truststore.jks");
		/*
		 * ideally the passwords should be read from somewhere such as a
		 * property file where the password is stored in an encrypted form and
		 * before passing it here below the password is decrypted
		 */
		props.put("ssl.truststore.password", "pass");
	}

	private void setClientAuthProperties(Properties props) {
		props.put("ssl.keystore.location",
				"/Users/user1/cert/kafka.client.keystore.jks");
		/*
		 * ideally the passwords should be read from somewhere such as a
		 * property file where the password is stored in an encrypted form and
		 * before passing it here below the password is decrypted
		 */
		props.put("ssl.keystore.password", "pass");
		props.put("ssl.key.password", "pass");
	}
}
