package com.thomsonreuters.ce.dbor.pasdi.cursor;

import java.io.IOException;

import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;

public class FileDataMarker {
	
	private long Position;
	private int Size;
	private CursorAction CA;
	
	
	public FileDataMarker(CursorAction ca, long pos, int size)
	{
		this.CA=ca;
		this.Position=pos;
		this.Size=size;
	}

	public long getPosition() {
		return Position;
	}

	public int getSize() {
		return Size;
	}
	
	public SDICursorRow[] getResultSet() throws IOException,ClassNotFoundException
	{
		return CA.getResultSet(this);
	}
	

}
