package com.thomsonreuters.ce.exportjobcontroller.task.file.iiroutage.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ConcurrentDateUtil {
	
	private static ThreadLocal<DateFormat> yyyyMMdd = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};
	
	private static ThreadLocal<DateFormat> yyyyMMddDash = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd");
		}
	};
	
	private static ThreadLocal<DateFormat> yyyyMMddDashHHmmss = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};
	
	public static Date parseToyMd(String dateStr) throws ParseException{
		return yyyyMMdd.get().parse(dateStr);
	}
	
	public static Date parseToyMdDash(String dateStr) throws ParseException{
		return yyyyMMddDash.get().parse(dateStr);
	}
	
	public static Date parseToyMdDashHms(String dateStr) throws ParseException{
		return yyyyMMddDashHHmmss.get().parse(dateStr);
	}
	
	public static String formatToyMd(Date date) {
		return yyyyMMdd.get().format(date);
	}
	
	public static String formatToyMdDash(Date date) {
		return yyyyMMddDash.get().format(date);
	}
	
	public static String formatToyMdDashHms(Date date) {
		return yyyyMMddDashHHmmss.get().format(date);
	}

}
