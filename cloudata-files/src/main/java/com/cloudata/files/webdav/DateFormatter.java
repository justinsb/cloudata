package com.cloudata.files.webdav;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateFormatter {

    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    // super("E, dd MMM yyyy HH:mm:ss z", Locale.ENGLISH);

    private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("GMT");

    final SimpleDateFormat formatter;

    public DateFormatter() {
        formatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        formatter.setTimeZone(TIME_ZONE);
    }

    public String format(Date date) {
        return formatter.format(date);
    }

    public String format(long date) {
        return format(new Date(date));
    }

    private static final ThreadLocal<DateFormatter> dateFormatThreadLocal = new ThreadLocal<DateFormatter>() {
        @Override
        protected DateFormatter initialValue() {
            return new DateFormatter();
        }
    };

    static DateFormatter get() {
        return dateFormatThreadLocal.get();
    }
}
