package com.thomsonreuters.ce.dbor.vessel.location;

import java.sql.Timestamp;

public class DBLocation implements Comparable {

    @Override
    public boolean equals(Object obj) {
	// TODO Auto-generated method stub
	return super.equals(obj);
    }


    public DBLocation(long locationId, Timestamp recordTime, Double pSpeed,
	    Double pLat, Double pLong, boolean isSpeedChange, boolean isDud,
	    Timestamp eTA, Double heading, Integer dIT_Status_ID,
	    String destination, Double draught,Integer dit_data_source_id) {
	super();
	this.locationId = locationId;
	this.recordTime = recordTime;
	this.pSpeed = pSpeed;
	this.pLat = pLat;
	this.pLong = pLong;
	this.IsSpeedChange = isSpeedChange;
	this.IsDud = isDud;
	this.ETA = eTA;
	this.Heading = heading;
	this.DIT_Status_ID = dIT_Status_ID;
	this.Destination = destination;
	this.Draught = draught;
	this.DIT_data_source_id=dit_data_source_id;
    }

    public DBLocation(long locationId, Timestamp recordTime, Double pSpeed,
	    Double pLat, Double pLong, boolean isSpeedChange, boolean isDud,
	    Timestamp eTA, Double heading, Integer dIT_Status_ID,
	    String destination, Double draught) {
	super();
	this.locationId = locationId;
	this.recordTime = recordTime;
	this.pSpeed = pSpeed;
	this.pLat = pLat;
	this.pLong = pLong;
	this.IsSpeedChange = isSpeedChange;
	this.IsDud = isDud;
	this.ETA = eTA;
	this.Heading = heading;
	this.DIT_Status_ID = dIT_Status_ID;
	this.Destination = destination;
	this.Draught = draught;

    }


    public long locationId;
    public Timestamp recordTime;
    public Double pSpeed;
    public Double pLat;
    public Double pLong;
    public boolean	IsSpeedChange;
    public boolean IsDud;
    public Timestamp ETA;
    public Double Heading;
    public Integer DIT_Status_ID;
    public String Destination;
    public Double Draught;
    public Integer DIT_data_source_id;


    public int compareTo(Object o) {
	// TODO Auto-generated method stub
	if (this.recordTime.after(((DBLocation)o).recordTime))
	{
	    return 1;
	}
	else if (this.recordTime.before(((DBLocation)o).recordTime))
	{
	    return -1;
	}
	else
	{
	    return 0;
	}
    }

}
