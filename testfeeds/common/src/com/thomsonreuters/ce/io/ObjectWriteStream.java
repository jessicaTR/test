package com.thomsonreuters.ce.io;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class ObjectWriteStream extends ObjectOutputStream {

	public ObjectWriteStream() throws IOException, SecurityException {
		// TODO Auto-generated constructor stub
	}

	public ObjectWriteStream(OutputStream out) throws IOException {
		super(out);
		// TODO Auto-generated constructor stub
	}

	protected void writeStreamHeader() throws IOException {
		super.reset();
	}
}
