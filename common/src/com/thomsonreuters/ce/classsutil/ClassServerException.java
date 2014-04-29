package com.thomsonreuters.ce.classsutil;

import com.thomsonreuters.ce.exception.EasyException;

public class ClassServerException extends EasyException {

	  public ClassServerException()
	  {
	  }

	  public ClassServerException(String strErrMsg) {
	    super(strErrMsg);
	  }

	  public ClassServerException(Throwable ex)
	  {
	      super(ex);
	  }

	  public ClassServerException(String strErrMsg,Throwable ex)
	  {
	      super(strErrMsg,ex);
	  }	
	
}
