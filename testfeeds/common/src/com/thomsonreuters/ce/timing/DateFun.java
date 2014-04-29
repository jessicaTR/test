package com.thomsonreuters.ce.timing;

import java.text.*;
import java.util.*;

public class DateFun {
	
	public static String getStringDate(Date time) { 
		  
		  SimpleDateFormat formatter = new SimpleDateFormat(DateConstants.FULLTIMEFORMAT); 
		  String dateString = formatter.format(time); 
		  return dateString; 
		 } 

	public static Date strToDateLong(String strDate) { 
		  SimpleDateFormat formatter = new SimpleDateFormat(DateConstants.FULLDATEFORMAT); 
		  ParsePosition pos = new ParsePosition(0); 
		  Date strtodate = formatter.parse(strDate, pos); 
		  return strtodate; 
	} 
	
	public static Date GetTimeOfDay(Date BaseDate, String Time)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(DateConstants.DAILYDATEFORMAT);
		String Strdate = sdf.format(BaseDate);
				
		return strToDateLong(Strdate+Time);
		
	}
	
	public static Date GetTimeOfHour(Date BaseDate, String Time)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(DateConstants.HOURLYDATEFORMAT);
		String Strdate = sdf.format(BaseDate);
		
		return strToDateLong(Strdate+Time);
	}	

	public static Date GetTimeOfMinute(Date BaseDate,String Time)
	{
		SimpleDateFormat sdf = new SimpleDateFormat(DateConstants.MINUTEDATEFORMAT);
		String Strdate = sdf.format(BaseDate);
		
		return strToDateLong(Strdate+Time);
	}	
	
	public static Date GetTimeOfMonth(Date BaseDate,String Time)
	{
		SimpleDateFormat sdf;
		
		int StrDate= Integer.parseInt(Time.substring(0,2));
		
		Calendar cDay1 = Calendar.getInstance();
		cDay1.setTime(BaseDate);
		int lastDay = cDay1.getActualMaximum(Calendar.DAY_OF_MONTH);
		
		if (lastDay<StrDate)
		{
			sdf = new SimpleDateFormat("yyyy-MM-"+String.valueOf(lastDay));
			Time=Time.substring(2);
		}
		else
		{
			sdf = new SimpleDateFormat("yyyy-MM-");
		}
		
		String Strdate = sdf.format(BaseDate);
		
		return strToDateLong(Strdate+Time);
	}
	
	public static Date GetTimeOfWeek(Date BaseDate,String Time)
	{
		String strWeek=Time.substring(0,3);
		String strTime=Time.substring(4);
		
		Calendar c = Calendar.getInstance();
		c.setTime(BaseDate);
		
		if (strWeek.equals("MON"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY); 
		else if (strWeek.equals("TUE"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY); 
		else if (strWeek.equals("WED"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY); 
		else if (strWeek.equals("THU"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY); 
		else if (strWeek.equals("FRI"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY); 
		else if (strWeek.equals("SAT"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY); 
		else if (strWeek.equals("SUN"))
			c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); 
						
		SimpleDateFormat sdf = new SimpleDateFormat(DateConstants.DAILYDATEFORMAT);

		Date TargetDate = c.getTime();
		String Strdate = sdf.format(TargetDate);
		
		return strToDateLong(Strdate+strTime);
	}	
	
	public static Date getLastDayOfMonth(Date sDate1) {
		Calendar cDay1 = Calendar.getInstance();
		cDay1.setTime(sDate1);
		final int lastDay = cDay1.getActualMaximum(Calendar.DAY_OF_MONTH);
		Date lastDate = cDay1.getTime();
		lastDate.setDate(lastDay);
		return lastDate;
	}  
	
	public static void main(String arg[]) {

	}

}
