package com.thomsonreuters.ce.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;

public class ObjectReadStream extends ObjectInputStream {

	public ObjectReadStream() throws IOException, SecurityException {
		// TODO Auto-generated constructor stub
	}

	public ObjectReadStream(InputStream in) throws IOException {
		super(in);
		// TODO Auto-generated constructor stub
	}
	
	protected void readStreamHeader()
	throws IOException, StreamCorruptedException
	{

	}	

}
