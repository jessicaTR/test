package com.thomsonreuters.ce.exception;



public class LogicException extends EasyException {
	  public LogicException()
	  {
	  }

	  public LogicException(String strErrMsg) {
	    super(strErrMsg);
	  }

	  public LogicException(Throwable ex)
	  {
	      super(ex);
	  }

	  public LogicException(String strErrMsg,Throwable ex)
	  {
	      super(strErrMsg,ex);
	  }
}