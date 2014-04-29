package com.thomsonreuters.ce.io;

/**
 * Title:         InputBuffer <br>
 * Description:   Utility class used to retrieve parameters from the input buffer <br>
 * Copyright:     Copyright (c) 2001 <br>
 * Company:       Shlumberger <br>
 * @author        Zohair Gherbi <br>
 * @version 1.0 <br>
 */

/**
 * Input buffer object
 */
public class InputBuffer {
  public int LENGTH_MAX;

  public byte btbuffer[];
  public int nCount;
  boolean bIsError;
  private int nReadOffset;

  /**
   * Creates the buffer object
   */
  public InputBuffer() {
    bIsError = false;
    LENGTH_MAX = 1024;
    btbuffer = new byte[LENGTH_MAX];
    nCount = 0;
    nReadOffset = 0;
  }

  public InputBuffer(int Size) {
    bIsError = false;
    btbuffer = new byte[Size];
    LENGTH_MAX = Size;
    nCount = 0;
    nReadOffset = 0;
  }

  /**
   *  Initializes the buffer for writing
   */
  public void resetWrite() {
    bIsError = false;
    nCount = 0;
  }

  /**
   *  Initializes the buffer for reading
   */
  public void resetRead() {
    bIsError = false;
    nReadOffset = 0;
  }

  /**
   * Increments the read offset
   * @param nIncrement the value of step
   */
  public void incrementReadOffset(int nIncrement) {
    bIsError = false;

    if (nIncrement >= 0) {
      nReadOffset += nIncrement;
    }
  }

  /**
   * Adds an array of bytes to the end of the buffer
   * Return false if error
   * @param bArray   the value should be added in buffer
   * @param nLength  the length of the array
   */
  public void addBytesArray(byte bArray[], int nLength) {
    bIsError = false;

    if (bArray == null) {
      bIsError = true;
      return;
    }

    try {
      System.arraycopy(bArray, 0, btbuffer, nCount, nLength);
      nCount += nLength;
    }
    catch (NullPointerException e1) {
      bIsError = true;
    }
    catch (ArrayStoreException e2) {
      bIsError = true;
    }
    catch (IndexOutOfBoundsException e3) {
      bIsError = true;
    }
  }

  /**
   * Retrieve a byte from the buffer using the current read index
   * @return  the data at the current read index
   */
  public final byte getByte() {
    bIsError = false;
    byte btByte = 0;

    if (nReadOffset < LENGTH_MAX) {
      btByte = btbuffer[nReadOffset];
      nReadOffset++;
    }
    else {
      bIsError = true;
    }

    return btByte;
  }

  /**
   * Retrieve an integer from the buffer using the current read index
   * @return  the integer data
   */
  public final int getInteger() {
    bIsError = false;
    int nInteger = 0;

    if ( (nReadOffset + 3) < LENGTH_MAX) {
      nInteger = ( ( (btbuffer[nReadOffset] & 0xFF) << 24) |
                  ( (btbuffer[nReadOffset + 1] & 0xFF) << 16) |
                  ( (btbuffer[nReadOffset + 2] & 0xFF) << 8) |
                  (btbuffer[nReadOffset + 3] & 0xFF));
      nReadOffset += 4;
    }
    else {
      bIsError = true;
    }

    return nInteger;
  }

  /**
       * Retrieve a null terminated string from the buffer using the current read index
   * @return  the string data
   */
  public final String getNullTerminatedString() {
    bIsError = false;
    int nIndex = 0;
    String strString = null;

    while (btbuffer[nReadOffset + nIndex] != 0) {
      nIndex++;

    }
    if (nIndex > 0) {
      try {
        strString = new String(btbuffer, nReadOffset, nIndex);
      }
      catch (IndexOutOfBoundsException e) {
        bIsError = true;
      }
    }
    else {
      strString = "";
    }

    nReadOffset += nIndex + 1;

    return strString;
  }

  /**
   * Retrieve an array of butes from the buffer using the current read index
   * @param bArray     the byte array which stores the retrieved data
   * @param nLength     the length of the byte array
   */
  public void getBytesArray(byte bArray[], int nLength) {
    bIsError = false;

    try {
      System.arraycopy(btbuffer, nReadOffset, bArray, 0, nLength);
      nReadOffset += nLength;
    }
    catch (NullPointerException e1) {
      bIsError = true;
    }
    catch (ArrayStoreException e2) {
      bIsError = true;
    }
    catch (IndexOutOfBoundsException e3) {
      bIsError = true;
    }
  }

  /**
   * indicate if there are unreaded data in buffer
   * @return   Returns true if there is bytes to read
   */
  public boolean isBytesToRead() {
    if (nReadOffset >= nCount) {
      return false;
    }
    else {
      return true;
    }
  }

  /**
   * Retrieve a short integer (integer coded on two bytes)
   * @return  the short data
   */
  public int getShortInteger() {
    bIsError = false;
    int nInteger = 0;

    if ( (nReadOffset + 1) < LENGTH_MAX) {
      nInteger = ( (btbuffer[nReadOffset] & 0xFF) << 8) |
          (btbuffer[nReadOffset + 1] & 0xFF);
      nReadOffset += 2;
    }
    else {
      bIsError = true;
    }

    return nInteger;
  }

  /**
   * Retrieve a fixed size String (not always nulled terminated)
   * @param nSize   the length of the string
   * @return        the string data
   */
  public final String getFixedSizeString(int nSize) {
    bIsError = false;
    String strString = null;
    if ( (nReadOffset + nSize) > LENGTH_MAX) {
      bIsError = true;
      return null;
    }

    int nEnd = nSize;
    try {
      while ( (nEnd > 0) && (btbuffer[nReadOffset + --nEnd] == 0)) {}
      strString = new String(btbuffer, nReadOffset, nEnd + 1);
    }
    catch (IndexOutOfBoundsException e) {
      bIsError = true;
    }

    nReadOffset += nSize;

    return (strString);

  }

  public final byte[] getRemaiderData() {
    bIsError = false;

    byte[] bArray = new byte[nCount - nReadOffset];
    try {
      System.arraycopy(btbuffer, nReadOffset, bArray, 0, bArray.length);
      nReadOffset = nCount;
    }
    catch (NullPointerException e1) {
      bIsError = true;
      bArray = null;
    }
    catch (ArrayStoreException e2) {
      bIsError = true;
      bArray = null;
    }
    catch (IndexOutOfBoundsException e3) {
      bIsError = true;
      bArray = null;
    }
    return bArray;
  }
}
