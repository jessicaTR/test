package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.io.Serializable;

public class SDICursorRow implements Serializable{ 
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long Perm_ID;
	
	public SDICursorRow(long Perm_ID)
	{
		this.Perm_ID=Perm_ID;
	}
	
	public long getPerm_ID()
	{
		return this.Perm_ID;
	}
	
	
	
}
