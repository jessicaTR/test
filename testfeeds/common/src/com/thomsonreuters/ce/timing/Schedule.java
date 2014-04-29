package com.thomsonreuters.ce.timing;


import java.util.Calendar;
import java.util.Date;

public class Schedule {
	
	public Schedule(Date basedate, ScheduleType type,String time)
	{
		this.TYPE=type;
		this.TIME=time;
		this.BaseDate=basedate;
	}
	
	private ScheduleType TYPE;
	private String TIME;
	private Date BaseDate;
	
	public Date getPreviousValidTime()
	{
		Date ExecuteTime=null;

		if (this.TYPE.equals(ScheduleType.MINUTELY))
		{
			ExecuteTime=DateFun.GetTimeOfMinute(this.BaseDate,this.TIME);
			
			if (ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.MINUTE, -1);
				ExecuteTime=cal.getTime();	
			}	

		}
		else if (this.TYPE.equals(ScheduleType.HOURLY))
		{
			ExecuteTime=DateFun.GetTimeOfHour(this.BaseDate,this.TIME);
			
			if (ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.HOUR, -1);
				ExecuteTime=cal.getTime();					
			}		
			
		}
		else if (this.TYPE.equals(ScheduleType.DAILY))
		{
			ExecuteTime=DateFun.GetTimeOfDay(this.BaseDate,this.TIME);
			
			if (ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.DATE, -1);
				ExecuteTime=cal.getTime();					
			}		
						
		}
		else if (this.TYPE.equals(ScheduleType.WEEKLY))
		{
			ExecuteTime=DateFun.GetTimeOfWeek(this.BaseDate,this.TIME);
			
			if (ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.WEEK_OF_YEAR, -1);
				ExecuteTime=cal.getTime();					
			}		
						
		}
		else if (this.TYPE.equals(ScheduleType.MONTHLY))
		{
			
			ExecuteTime=DateFun.GetTimeOfMonth(this.BaseDate,this.TIME);
			
			if (ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.MONTH, -1);
				ExecuteTime=cal.getTime();					
			}		
			
		}
		else if (this.TYPE.equals(ScheduleType.ONEOFF))
		{
			
			ExecuteTime=DateFun.strToDateLong(this.TIME);
				
		}		
		

		return ExecuteTime;		
	}
	
	public Date GetNextValidTime()
	{
		
		Date ExecuteTime=null;

		if (this.TYPE.equals(ScheduleType.MINUTELY))
		{
			ExecuteTime=DateFun.GetTimeOfMinute(this.BaseDate,this.TIME);
			
			if (!ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.MINUTE, 1);
				ExecuteTime=cal.getTime();	
			}	

		}
		else if (this.TYPE.equals(ScheduleType.HOURLY))
		{
			ExecuteTime=DateFun.GetTimeOfHour(this.BaseDate,this.TIME);
			
			if (!ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.HOUR, 1);
				ExecuteTime=cal.getTime();					
			}		
			
		}
		else if (this.TYPE.equals(ScheduleType.DAILY))
		{
			ExecuteTime=DateFun.GetTimeOfDay(this.BaseDate,this.TIME);
			
			if (!ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.DATE, 1);
				ExecuteTime=cal.getTime();					
			}		
						
		}
		else if (this.TYPE.equals(ScheduleType.WEEKLY))
		{
			ExecuteTime=DateFun.GetTimeOfWeek(this.BaseDate,this.TIME);
			
			if (!ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.WEEK_OF_YEAR, 1);
				ExecuteTime=cal.getTime();					
			}		
						
		}
		else if (this.TYPE.equals(ScheduleType.MONTHLY))
		{
			
			ExecuteTime=DateFun.GetTimeOfMonth(this.BaseDate,this.TIME);
			
			if (!ExecuteTime.after(this.BaseDate))
			{
				Calendar cal=Calendar.getInstance();
				cal.setTime(ExecuteTime);
				cal.add(Calendar.MONTH, 1);
				ExecuteTime=cal.getTime();					
			}		
			
		}
		else if (this.TYPE.equals(ScheduleType.ONEOFF))
		{
			
			ExecuteTime=DateFun.strToDateLong(this.TIME);
				
		}		
		

		return ExecuteTime;
	}
}
