package com.thomsonreuters.ce.io;

import java.io.BufferedInputStream;
import java.io.InputStream;

public class AdvancedBIS extends BufferedInputStream {
	public AdvancedBIS(InputStream in, int size)
	{
		super(in, size);
	}
	
	public AdvancedBIS(InputStream in)
	{
		super(in);
	}
	

	public int getPos()
	{
		return this.pos;
	}
	
	public int getCount()
	{
		return this.count;
	}
	
	public byte[] getBuffer()
	{
		return this.buf;
	}
}
