package com.thomsonreuters.ce.dbor.vessel.location;

import java.sql.Timestamp;

public class Event {
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "Zone CZO ID:"+this.Czo_id+" | "
		+"Entry Point Database ID:"+this.EntrypointID+ " | "
		+"Current Point Database ID:"+this.CurrentpointID+ " | "
		+"Is Close Event:"+this.IsCloseEvent;
	}
	
	public Event(long eventID, int czo_id, long entrypointID, long currentpointID, boolean isCloseEvent, Timestamp entryTime, Timestamp outTime) {
	    super();
	    EventID = eventID;
	    Czo_id = czo_id;
	    EntrypointID = entrypointID;
	    CurrentpointID = currentpointID;
	    IsCloseEvent = isCloseEvent;
	    EntryTime = entryTime;
	    OutTime = outTime;
	}

	public long EventID;
	public int Czo_id;
	public long EntrypointID;
	public long CurrentpointID;
	public boolean IsCloseEvent;
	
	public Timestamp EntryTime;
	public Timestamp OutTime;

}