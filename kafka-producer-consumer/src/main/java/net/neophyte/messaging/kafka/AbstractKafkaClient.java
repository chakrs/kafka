package net.neophyte.messaging.kafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author shuvro
 *
 */
public class AbstractKafkaClient {

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

	/**
	 * Returns if run time is over or not
	 * 
	 * @param runTime The time to run
	 * @return <code>true</code> if run time remains otherwise <code>false</code>
	 */
	protected static boolean runTimeRemains(long runTime) {
		if (runTime == Configuration.IGNORE) {
			return false;
		}
		return (System.currentTimeMillis() - startTime) < runTime;
	}

	/**
	 * Sets kafka producer properties
	 * 
	 * @param brokerCluster The broker cluster
	 * @param isSsl Whether to enable ssl
	 * @param useClientAuth Whether to enable client auth
	 * @throws Exception An exception in case of failure
	 * @return The producer {@link Properties}
	 * @throws Exception An exception in case of failure
	 */
	protected Properties getProducerProperties(BROKER_CLUSTER brokerCluster,
			boolean isSsl, boolean useClientAuth) throws Exception {
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

	/**
	 * Sets kafka consumer properties
	 * 
	 * @param brokerCluster The broker cluster
	 * @param isSsl Whether to enable ssl
	 * @param useClientAuth Whether to enable client auth
	 * @return The consumer {@link Properties}
	 * @throws Exception An exception in case of failure
	 */
	protected Properties getConsumerProperties(BROKER_CLUSTER brokerCluster,
			boolean isSsl, boolean useClientAuth) throws Exception {
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

	/**
	 * Sets kafka bootstrap server properties
	 * 
	 * @param props The {@link Properties} object to set the properties to
	 * @param brokerCluster The broker cluster
	 * @param isSsl Whether to enable ssl 
	 * @throws Exception An exception in case of failure
	 */
	private void setBootstrapServersProperties(Properties props,
			BROKER_CLUSTER brokerCluster, boolean isSsl) throws Exception {
		String brokers = null;
		switch (brokerCluster) {
		case CLUSTER1:
			if (isSsl) {
				brokers = Configuration.BROKER_CLUSTER1_SSL;
			} else {
				brokers = Configuration.BROKER_CLUSTER1;
			}
			break;

		default:
			throw new Exception("Undefined cluster was specified!");
		}
		props.put("bootstrap.servers", brokers);
	}

	/**
	 * Sets SSL properties
	 * 
	 * @param props The {@link Properties} object to set the properties to
	 */
	private void setSSLProperties(Properties props) {
		props.put("security.protocol", "SSL");
		props.put("ssl.truststore.location",
				Configuration.getTruststorelocation());
		props.put("ssl.truststore.password",
				Configuration.getTruststorepassword());
	}

	/**
	 * Sets SSL properties for client auth
	 * 
	 * @param props The {@link Properties} object to set the properties to
	 */
	private void setClientAuthProperties(Properties props) {
		props.put("ssl.keystore.location", Configuration.getKeystorelocation());
		props.put("ssl.keystore.password", Configuration.getKeystorepassword());
		props.put("ssl.key.password", Configuration.getKeypassword());
	}
}
