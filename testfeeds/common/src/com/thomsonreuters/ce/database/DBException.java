package com.thomsonreuters.ce.database;

import com.thomsonreuters.ce.exception.EasyException;

public class DBException extends EasyException {

  public DBException()
  {
  }

  public DBException(String strErrMsg) {
    super(strErrMsg);
  }

  public DBException(Throwable ex)
  {
      super(ex);
  }

  public DBException(String strErrMsg,Throwable ex)
  {
      super(strErrMsg,ex);
  }
}
