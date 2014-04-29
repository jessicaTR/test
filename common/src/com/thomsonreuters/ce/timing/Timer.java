package com.thomsonreuters.ce.timing;

import java.util.LinkedList;
import java.util.Iterator;
import java.util.Date;

//import org.apache.log4j.Logger;

public class Timer
extends Thread {

	private boolean Stop_Flag;
	private LinkedList<TimerHandler> TimerList;
	private boolean IsStopped;
	private Object stop_lock=new Object();
	
	//stop timer service
	public void Stop() {

		synchronized (this.TimerList) {
			TimerList.clear();
			this.Stop_Flag = true;			
			this.TimerList.notifyAll();
		}
		
		synchronized (stop_lock)
		{
			if (IsStopped==false)
			{
				try {
					stop_lock.wait();
					
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}			
		}
	}

	//create a timer server with target thread pool
	public Timer() {
		this.TimerList = new LinkedList<TimerHandler>();
		this.Stop_Flag = false;
		
	}


	public void run() {

		synchronized (stop_lock)
		{
			IsStopped=false;
		}

		try {
			while (true) {

				TimerHandler thisTimer;

				synchronized (this.TimerList) {
					//if no timer contain in server , then wait
					while (this.TimerList.isEmpty()) {
						try {
							this.TimerList.wait();
						}
						catch (InterruptedException e) {
						}
						
						if (this.Stop_Flag) {
							return;
						}
					}

					//compute the sleep time to next timer
					long SleepTime = TimerList.get(0).ExpectedTime - System.currentTimeMillis();

					//wait sleep time to next timer
					while (SleepTime > 0) {
						try {
							this.TimerList.wait(SleepTime);
						}
						catch (InterruptedException ex) {
							return;
						}

						if (this.Stop_Flag) {
//						System.out.println("Timer service is being stopped in run()");
							return;
						}

						if (this.TimerList.isEmpty()) {
							break;
						}

						//if some new timer added to server ,recompute the sleep time
						SleepTime = TimerList.get(0).ExpectedTime - System.currentTimeMillis();
					}

					if (this.TimerList.isEmpty())
					{
						//if some timers was canceled ,then service is empty , then return loop header
						continue;
					}

					//it's the timer to run next timer
					thisTimer = TimerList.remove(0);

				}

				new Thread(thisTimer).start();
			}
		} 
		finally
		{
			synchronized (stop_lock)
			{
				IsStopped=true;
				stop_lock.notify();
				
			}
		}

	}

	public String toString() {
		return "TimerAction";
	}

	//add timer and return a TimerKey
	public TimerHandler createTimer(long milli_secs, long inteval, Runnable TimeOutAction) {

		TimerHandler thisHandler=null;
		synchronized (this.TimerList) {

			long targetTime=System.currentTimeMillis() +  milli_secs;
			thisHandler=new TimerHandler(this, targetTime, inteval, TimeOutAction);
			this.TimerList.add(thisHandler);
			/////////////////////////////////////////////////////////////////////////
			//sort the timer according the time
			java.util.Collections.sort(this.TimerList);
			this.TimerList.notifyAll();

		}
		return thisHandler;
	}

	//add timer using a future time
	public TimerHandler createTimer(Date futureTime,long inteval, Runnable TimeOutAction) {
		return createTimer(futureTime.getTime()-System.currentTimeMillis(),inteval,TimeOutAction);
	}

	//add timer and return a TimerKey
	protected Runnable cancelTimer(TimerHandler Timer_H) {
		Runnable thisAction = null;

		synchronized (this.TimerList) {
			Iterator<TimerHandler> TimerIter = this.TimerList.iterator();
			while (TimerIter.hasNext()) {
				TimerHandler thisTimerElment = TimerIter.next();
				if (thisTimerElment == Timer_H) {
					this.TimerList.remove(thisTimerElment);


					thisAction = thisTimerElment.TimeOutAction;
					this.TimerList.notifyAll();
					break;
				}
			}
		}
		return thisAction;
	}

}
