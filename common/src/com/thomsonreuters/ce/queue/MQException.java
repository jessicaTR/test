package com.thomsonreuters.ce.queue;

import com.thomsonreuters.ce.exception.EasyException;

public class MQException extends EasyException {


	  public MQException()
	  {
	  }

	  public MQException(String strErrMsg) {
	    super(strErrMsg);
	  }

	  public MQException(Throwable ex)
	  {
	      super(ex);
	  }

	  public MQException(String strErrMsg,Throwable ex)
	  {
	      super(strErrMsg,ex);
	  }	
	
}
