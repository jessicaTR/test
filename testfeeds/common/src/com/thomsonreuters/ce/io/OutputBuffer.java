package com.thomsonreuters.ce.io;

public class OutputBuffer {
  public int bufferSize;
  public byte btbuffer[];
  public int nCount;
  private boolean bError;

  /**
   * Creates the buffer object
   */
  public OutputBuffer() {
    this(1024);
  }

  /**
   * Creates the buffer object
   */
  public OutputBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
    btbuffer = null;
    bError = false;
    btbuffer = new byte[bufferSize];
    if (btbuffer == null) {
      bError = true;
    }
    nCount = 0;
  }

  /**
   *  Initializes the buffer
   */
  public void reset() {
    bError = false;
    if (btbuffer == null) {
      bError = true;
    }
    nCount = 0;
  }

  /**
   * Adds a byte to the end of the buffer
   * Returns false if error
   */
  public boolean addByte(byte bData) {
    boolean bResult = false;

    if (bError) {
      return false;
    }

    if (nCount < bufferSize) {
      btbuffer[nCount] = bData;
      nCount++;
      bResult = true;
    }

    return bResult;
  }

  public boolean addShort(short sData) {
    boolean bResult = false;

    if (bError) {
      return false;
    }

    if ( (nCount + 1) < bufferSize) {
      btbuffer[nCount] = (byte) (sData >> 8);
      btbuffer[nCount + 1] = (byte) (sData);
      nCount += 2;
      bResult = true;
    }

    return bResult;
  }

  /**
   * Adds an Integer to the end of the buffer
   * Returns false if error
   */
  public boolean addInteger(int nData) {
    boolean bResult = false;

    if (bError) {
      return false;
    }

    if ( (nCount + 3) < bufferSize) {
      btbuffer[nCount] = (byte) (nData >> 24);
      btbuffer[nCount + 1] = (byte) (nData >> 16);
      btbuffer[nCount + 2] = (byte) (nData >> 8);
      btbuffer[nCount + 3] = (byte) (nData);
      nCount += 4;
      bResult = true;
    }

    return bResult;
  }

  /**
   * Adds an array of bytes to the end of the buffer
   * Returns false if error
   */
  public boolean addBytesArray(byte bArray[], int nLength) {
    boolean bResult = false;

    if (bError || (bArray == null)) {
      return false;
    }

    try {
      System.arraycopy(bArray, 0, btbuffer, nCount, nLength);
      nCount += nLength;
      bResult = true;
    }
    catch (NullPointerException e1) {
    }
    catch (ArrayStoreException e2) {
    }
    catch (IndexOutOfBoundsException e3) {
    }

    return bResult;
  }

  /**
   * Adds a string to the end of the buffer
   * Returns false if error
   */
  public boolean addString(String strString) {
    boolean bResult = false;
    int nLength;

    if (bError || (strString == null)) {
      return false;
    }

    nLength = strString.length();
    if (nLength > 0) {
      try {
        System.arraycopy(strString.getBytes(), 0, btbuffer, nCount, nLength);
        nCount += nLength;
        bResult = true;
      }
      catch (NullPointerException e1) {
      }
      catch (ArrayStoreException e2) {
      }
      catch (IndexOutOfBoundsException e3) {
      }
    }
    else {
      bResult = true;
    }

    return bResult;
  }

  /**
   * Adds a string and a NULL byte to the end of the buffer
   * Returns false if error
   */
  public boolean addNullTerminatedString(String strString) {
    boolean bResult = false;
    int nLength;

    if (bError || (strString == null)) {
      return false;
    }

    nLength = strString.length();
    if (nLength > 0) {
      try {
        System.arraycopy(strString.getBytes(), 0, btbuffer, nCount, nLength);
        nCount += nLength;
        btbuffer[nCount] = (byte) (0);
        nCount++;
        bResult = true;
      }
      catch (NullPointerException e1) {
      }
      catch (ArrayStoreException e2) {
      }
      catch (IndexOutOfBoundsException e3) {
      }
    }
    else {
      btbuffer[nCount] = (byte) (0);
      nCount++;
      bResult = true;
    }

    return bResult;
  }

  /**
   * Adds an Integer at a defined position
   * Returns false if error
   */
  public boolean addIntegerAt(int nData, int nPosition) {
    boolean bResult = false;

    if (bError) {
      return false;
    }

    if ( (nPosition + 3) < bufferSize) {
      btbuffer[nPosition] = (byte) (nData >> 24);
      btbuffer[nPosition + 1] = (byte) (nData >> 16);
      btbuffer[nPosition + 2] = (byte) (nData >> 8);
      btbuffer[nPosition + 3] = (byte) (nData);
      bResult = true;
    }

    return bResult;
  }

  // AGA 06-NOV-2001
  /**
   * This method increments the offset of the number coding an integer on 4 bytes in SMPP protocol:
   * it skips an integer.
   */
  public boolean skipInteger() {
    if (!bError && ( (nCount + 3) < bufferSize)) {
      nCount += 4;
      return true;
    }
    else {
      return false;
    }
  }

  /**
   * This method adds a SMP optional parameter. These parameters are TLV.
   * The length and value of this TLV is given as a byte array.
   */
  public boolean addTLV(int tag, byte[] data, int dataLength) {
    if (!bError && ( (nCount + 4 + dataLength) <= bufferSize)) {

      // set the Tag
      btbuffer[nCount++] = (byte) (tag >> 8);
      btbuffer[nCount++] = (byte) (tag);

      // set the Length
      btbuffer[nCount++] = (byte) (dataLength >> 8);
      btbuffer[nCount++] = (byte) (dataLength);

      // set the value
      System.arraycopy(data, 0, btbuffer, nCount, dataLength);
      nCount += dataLength;

      return true;
    }
    else {
      return false;
    }
  }

  /**
   * This method adds a SMP optional parameter. These parameters are TLV.
   * The value of this TLV is given as an integer and its length is 4 bytes.
   */
  public boolean addTLV(int tag, int value) {
    if (!bError && ( (nCount + 8) <= bufferSize)) {

      // set the Tag
      btbuffer[nCount++] = (byte) (tag >> 8);
      btbuffer[nCount++] = (byte) (tag);

      // set the Length
      btbuffer[nCount++] = 0x00;
      btbuffer[nCount++] = 0x04;

      // set the value
      btbuffer[nCount++] = (byte) (value >> 24);
      btbuffer[nCount++] = (byte) (value >> 16);
      btbuffer[nCount++] = (byte) (value >> 8);
      btbuffer[nCount++] = (byte) (value);

      return true;
    }
    else {
      return false;
    }
  }

  // END AGA

  // AGA 05-DEC-2001 : for support of more message to send
  /**
   * This method adds a SMP optional parameter. These parameters are TLV.
   * The value of this TLV is given as a byte.
   */
  public boolean addByteTLV(int tag, byte value) {
    if (!bError && ( (nCount + 5) <= bufferSize)) {

      // set the Tag
      btbuffer[nCount++] = (byte) (tag >> 8);
      btbuffer[nCount++] = (byte) (tag);

      // set the Length
      btbuffer[nCount++] = 0x00;
      btbuffer[nCount++] = 0x01;

      // set the value
      btbuffer[nCount++] = value;

      return true;
    }
    else {
      return false;
    }
  }

  // END AGA

  /**
   * Write the string s in the buffer.
   * If length < s.length() then copy the whole string and return s.length()
   * If length = s.length() then copy the whole string and return length
   * If length > s.length() then copy the whole string with padding '\0' and return length
   * @param s is the original data
   * @param length is the expected data length to filled in destination array
   * @return is the copied data length
   */
  //Add by Lifeng Oct 10, 2002
  public boolean addString(String s, int length) {
    if (length <= s.length()) {
      return (addString(s));
    }
    else { // length > s.length()
      addString(s);
      for (int i = 0; i < length - s.length(); i++) {
        addByte( (byte) 0);
      }
      return true;
    }
  }

}
