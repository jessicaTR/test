package com.thomsonreuters.ce.exception;

import java.util.UUID;

public class SystemException extends EasyException {
	
	  private String EventID = UUID.randomUUID().toString();
	  
	  public SystemException()
	  {
	  }
	  
	  public String getEventID()
	  {
		  return this.EventID;
	  }

	  public SystemException(String strErrMsg) {
		super(strErrMsg);
	    
	  }

	  public SystemException(Throwable ex)
	  {
	      super(ex);
	  }

	  public SystemException(String strErrMsg,Throwable ex)
	  {
	      super(strErrMsg,ex);
	  }
}
