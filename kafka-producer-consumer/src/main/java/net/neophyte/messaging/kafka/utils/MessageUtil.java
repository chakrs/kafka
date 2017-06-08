package net.neophyte.messaging.kafka.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An utility class for message
 * 
 * @author shuvro
 */
public class MessageUtil {
	/* Cache for test messages; key is the length of the test message string */
	private static final Map<Integer, String> TEST_MESSAGES = new ConcurrentHashMap<Integer, String>();
	private static final int CHAR_NUM = 26;

	/**
	 * Generates a sample text string of specified length. The returned string
	 * may be the same by repeated calls to this method.
	 *
	 * @param length
	 *            number of chars
	 * @return a string has length chars
	 */
	public static String generateMessage(int length) {
		return TEST_MESSAGES.computeIfAbsent(
				length,
				len -> {
					StringBuilder str = new StringBuilder(len);
					for (int i = 0; i < len; i++) {
						str.append((char) ('a' + (i + Math.ceil(Math.random()
								* CHAR_NUM))
								% CHAR_NUM));
					}
					return str.toString();
				});
	}
}
