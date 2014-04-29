package com.thomsonreuters.ce.exception;

public class RMIException
    extends EasyException {

  public RMIException()
  {
  }

  public RMIException(String strErrMsg) {
    super(strErrMsg);
  }

  public RMIException(Throwable ex)
  {
      super(ex);
  }

  public RMIException(String strErrMsg,Throwable ex)
  {
      super(strErrMsg,ex);
  }
}
