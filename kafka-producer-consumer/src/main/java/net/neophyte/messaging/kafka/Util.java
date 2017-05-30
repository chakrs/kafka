package net.neophyte.messaging.kafka;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * Util class
 * 
 * @author shuvro
 *
 */
public class Util {

	private static final String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss";
    private Util() {}

    public static String currentTimeStamp() {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String formattedDate = sdf.format(new Date());

        return formattedDate;
    }

    public static String getDateTime(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String formattedDate = sdf.format(new Date(time));

        return formattedDate;
    }

    public static String getHh_Mm_Ss_ssss_Time(long millis) {
        String hmsTimeStr = String.format(
                "%02d:%02d:%02d:%03d",
                TimeUnit.MILLISECONDS.toHours(millis),
                TimeUnit.MILLISECONDS.toMinutes(millis)
                        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS
                                .toHours(millis)),
                TimeUnit.MILLISECONDS.toSeconds(millis)
                        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS
                                .toMinutes(millis)),
                TimeUnit.MILLISECONDS.toMillis(millis)
                        - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS
                                .toSeconds(millis)));
        return hmsTimeStr;
    }

    public static boolean isNotNull(Object obj) {
        return (obj != null);
    }
    
    public static boolean isEmpty(String string) {

        return (string == null) || (string.trim().length() == 0);
    }

    public static boolean isNotEmpty(String string) {

        return (!isEmpty(string));
    }
}
