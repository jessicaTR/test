package com.thomsonreuters.ce.timing;

import java.util.Date;

public class TimerPool {
	private Timer[] TimerList;

	private int TimerIndex = 0;

	public TimerPool(int Thread_Num) {
		TimerList = new Timer[Thread_Num];
		for (int i = 0; i < Thread_Num; i++) {
			TimerList[i] = new Timer();
		}
	}
	
	public void Start()
	{
		for (int i = 0; i < TimerList.length; i++) {
			TimerList[i].start();
		}		
	}
	
	public void Stop()
	{
		for (int i = 0; i < TimerList.length; i++) {
			TimerList[i].Stop();
		}		
	}	


	// add timer and return a TimerKey
	public TimerHandler createTimer(long milli_secs, long Interval,
			Runnable TimeOutAction) {
		
		synchronized (TimerList) {
		TimerHandler thisHandler = TimerList[TimerIndex].createTimer(milli_secs,Interval, TimeOutAction);
		IncreaseIndex();
		return thisHandler;
		}


	}

	// add timer using a future time
	public TimerHandler createTimer(Date futureTime, long Interval,
			Runnable TimeOutAction) {
		synchronized (TimerList) {
		TimerHandler thisHandler = TimerList[TimerIndex].createTimer(futureTime, Interval,TimeOutAction);
		IncreaseIndex();
		return thisHandler;
		}

	}

	private void IncreaseIndex() {
		TimerIndex = TimerIndex + 1;
		if (TimerIndex == TimerList.length) {
			TimerIndex = 0;
		}		
	}

}
