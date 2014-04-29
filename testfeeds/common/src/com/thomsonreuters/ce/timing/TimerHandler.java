package com.thomsonreuters.ce.timing;



public class TimerHandler
    implements Runnable,Comparable {

  private boolean CancelFlag = false;

  private boolean isRunning = false;

  private boolean isFinished = false;

  private Timer thisService;

  public long ExpectedTime;

  public Runnable TimeOutAction;
  
  public long interval;

  protected TimerHandler(Timer timerService,
                         long Expected_Time,
                         long timeInterval,
                         Runnable TimeOut_Action) {
    this.thisService = timerService;
    this.ExpectedTime = Expected_Time;
    this.TimeOutAction = TimeOut_Action;
    this.interval = timeInterval;
  }

  public int compareTo(Object t) {
    if ( ( (TimerHandler) t).ExpectedTime > this.ExpectedTime) {
      return -1;
    }
    else if ( ( (TimerHandler) t).ExpectedTime < this.ExpectedTime) {
      return 1;
    }
    return 0;
  }

  public boolean cancel() {
    synchronized (this) {
      if ( this.isFinished || this.isRunning) {
        //timer is done or it's running
        return false;
      }
      else {
        if (this.thisService.cancelTimer(this) == null) {
          this.CancelFlag = true;
        }
        return true;
      }      
    }
  }

  public void run() {

    synchronized (this) {
      if (CancelFlag) {
        //cos action canceled, do nothing
        CancelFlag = false;
        return;
      }
      this.isFinished = false;
      this.isRunning = true;
    }

    try {
      this.TimeOutAction.run();
      
      if (this.interval!=0)
      {
    	  this.thisService.createTimer(this.interval, this.interval, TimeOutAction);
      }
    }
    catch(Throwable e) {
    	e.printStackTrace();
    }

    synchronized (this) {
      this.isRunning = false;
      this.isFinished = true;
    }    
  }

  public String toString()
  {
    StringBuffer strTH=new StringBuffer("TimerHandler for ");
    strTH.append(this.TimeOutAction.toString());
    return strTH.toString();
  }
}
