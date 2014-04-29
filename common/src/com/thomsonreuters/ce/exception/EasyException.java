package com.thomsonreuters.ce.exception;

import java.io.*;

public class EasyException extends RuntimeException {

  public EasyException()
  {
  }

  public EasyException(String strErrMsg) {
    super(strErrMsg);
  }

  public EasyException(Throwable ex)
  {
      super(ex);
  }

  public EasyException(String strErrMsg,Throwable ex)
  {
      super(strErrMsg,ex);
  }

}
