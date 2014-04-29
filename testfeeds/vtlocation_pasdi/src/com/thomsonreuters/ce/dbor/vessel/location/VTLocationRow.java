package com.thomsonreuters.ce.dbor.vessel.location;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import au.com.bytecode.opencsv.CSVParser;

import com.thomsonreuters.ce.exception.LogicException;
import com.thomsonreuters.ce.exception.SystemException;

public class VTLocationRow implements Serializable, Comparable{


    public VTLocationRow(Timestamp recordTime, Double pSpeed,
	    Double pLat, Double pLong, 
	    Timestamp eTA, Double heading, Integer dIT_Status_ID,
	    String destination, Double draught,Integer datasource_id) {
	super();
	this.pTime = recordTime;
	this.pSpeed = pSpeed;
	this.pLat = pLat;
	this.pLong = pLong;
	this.vEta = eTA;
	this.pHdg = heading;
	this.dit_status_id = dIT_Status_ID;
	this.vDest = destination;
	this.vDraught = draught;
	this.dit_datasource_id=datasource_id;
    }
    

    
    @Override
    public String toString() {
	// TODO Auto-generated method stub
	return "(shipId="+shipId
		+")(sImo="+sImo
		+")(sMmsi="+sMmsi
		+")(sShiptype="+sShiptype
		+")(pTime="+pTime
		+")(vEta="+vEta
		+")(pHdg="+pHdg
		+")(pLat="+pLat
		+")(pLong="+pLong
		+")(pSpeed="+pSpeed
		+")(pStatus="+pStatus
		+")(pSource="+pSource
		+")(vDest="+vDest
		+")(sCallsign="+sCallsign
		+")(vDraught="+vDraught
		+")(sName="+sName
		+")(sLength="+sLength
		+")(sWidth="+sWidth
		+")(dit_datasource_id="+dit_datasource_id
		+")";
    }

    long shipId;
    int sImo;
    long sMmsi;
    Timestamp pTime ;
    Timestamp vTime ;
    Timestamp vEta ;
    Double pHdg ;
    double pLat;
    double pLong;
    Double pSpeed;
    String pStatus ;
    Integer dit_status_id;
    String pSource ;
    int dit_datasource_id;
    String vDest ;
    String vDestLocode ;
    String vDestCleaned ;
    String sCallsign ;
    int sShiptype;
    Double vDraught ;
    String sName ;
    Double sLength ;
    Double sWidth ;


    public int compareTo(Object o) {
	if (this.pTime.after(((VTLocationRow)o).pTime))
	{
	    return 1;
	}
	else if (this.pTime.before(((VTLocationRow)o).pTime))
	{
	    return -1;
	}
	else
	{
	    return 0;	        	
	}
    }
    
    private String[] Header;
    private String[] colData;
    
    public VTLocationRow(String[] header,String LocRow)
    {
	this.Header=header;
	
	CSVParser CSVP=new CSVParser(',','"','\\',true,false);

	try {
	    this.colData = CSVP.parseLine(LocRow);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    throw new SystemException("Parsing CSV failed!");
	}
	
	shipId=Long.parseLong(getValue("ship_id"));	

	String strIMOId = getValue("imo");
	if (strIMOId.equals(""))
	{					
	    sImo = 0;
	}
	else
	{
	    sImo = Integer.parseInt(strIMOId);
	}

	String strMmsi = getValue("mmsi");
	sMmsi=(!strMmsi.equals("")) ? Long.parseLong(strMmsi): 0;

	String strShiptype = ""; //no this field in historical data file
	sShiptype=(!strShiptype.equals("")) ? Integer.parseInt(strShiptype): 0;

	String strRecordTime = getValue("timestamp_position");
	if (strRecordTime.equals("")) 
	{				
	    throw new LogicException("Vessel Location:  RecordTime is not provided for ship ID: " + shipId + " . This row failed!");
	}
	pTime = strToDate(strRecordTime);

	String strVTime = getValue("timestamp_voyage");
	if (!strVTime.equals("")) 
	{				
	    vTime = strToDate(strVTime);
	}
	else
	{
	    vTime = null;
	}

	sName = getValue("name");

	vDest = getValue("destination");
	if (vDest.equals(""))
	{
	    vDest=null;
	}

	vDestLocode = getValue("destination_locode");

	vDestCleaned = getValue("destination_portname");

	String strLatitude = getValue("lat");
	pLat = Double.parseDouble(strLatitude);

	String strLongitude = getValue("lon");
	pLong = Double.parseDouble(strLongitude);

	String strHeading = getValue("heading");
	if (!strHeading.equals(""))
	{
	    pHdg = Double.parseDouble(strHeading);
	}
	else
	{
	    pHdg= 0.0D;
	}

	String strDraught = getValue("draught");
	if (!strDraught.equals(""))
	{
	    vDraught  = Double.parseDouble(strDraught);
	}
	else
	{
	    vDraught =null;
	}

	String strSpeed = getValue("speed");

	if (!strSpeed.equals(""))
	{
	    pSpeed = Double.parseDouble(strSpeed);
	}
	else
	{
	    pSpeed=null;
	}

	String strETA =getValue("eta");
	if (!strETA.equals("") && !strETA.startsWith("00"))
	{
	    vEta  = strToDate(strETA);
	}
	else 
	{
	    vEta  = null;
	}

	pSource = getValue("source_position");
	String strDatasource=null;
	if (pSource.equals("T-AIS"))
	{
	    strDatasource = "VesselTracker";
	}
	else if (pSource.equals("S-AIS"))
	{
	    strDatasource = "AIS";
	}
	else
	{
	    // for other datasource all set VesselTracker, because most datasource is
	    // empty
	    strDatasource = "VesselTracker";
	}

	dit_datasource_id=VesselDataSource.getInstance(strDatasource).getID();

	pStatus = getValue("nav_status");
	String strStatus = VTLocationLoader.statusProp.getProperty(pStatus);
	if (strStatus!=null)
	{
	    dit_status_id=AssetStatus.getInstance(strStatus).getID();
	}
	else
	{
	    dit_status_id=-1;
	}

	sCallsign  = getValue("callsign");

	String strLength = getValue("length");
	if (!strLength.equals(""))
	{
	    sLength =Double.parseDouble(strLength);
	}
	else
	{
	    sLength =null;
	}

	String strWidth = getValue("width");	
	if (!strWidth.equals(""))
	{
	    sWidth =Double.parseDouble(strWidth);
	}
	else
	{
	    sWidth =null;
	}
	
	
	
    }
    
    public String getValue(String columnName)
	{
		for(int i=0; i<Header.length;i++)
		{
			if (Header[i].equals(columnName))
			{
				return colData[i].trim();
			}
		}
		return null;
	}

    public VTLocationRow(String LocRow)
    {
	CSVParser CSVP=new CSVParser(',','"','\\',true,false);

	String[] colData=null;
	try {
	    colData = CSVP.parseLine(LocRow);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    throw new SystemException("Parsing CSV failed!");
	}

	shipId=Long.parseLong(colData[0].trim());	

	String strIMOId = colData[1].trim();
	if (strIMOId.equals(""))
	{					
	    sImo = 0;
	}
	else
	{
	    sImo = Integer.parseInt(strIMOId);
	}

	String strMmsi = colData[2].trim();
	sMmsi=(!strMmsi.equals("")) ? Long.parseLong(strMmsi): 0;

	String strShiptype = colData[4].trim();
	sShiptype=(!strShiptype.equals("")) ? Integer.parseInt(strShiptype): 0;

	String strRecordTime = colData[21].trim();
	if (strRecordTime.equals("")) 
	{				
	    throw new LogicException("Vessel Location:  RecordTime is not provided for ship ID: " + shipId + " . This row failed!");
	}
	pTime = strToDate(strRecordTime);

	String strVTime = colData[13].trim();
	if (!strVTime.equals("")) 
	{				
	    vTime = strToDate(strVTime);
	}
	else
	{
	    vTime = null;
	}

	sName = colData[7].trim();

	vDest = colData[9].trim();
	if (vDest.equals(""))
	{
	    vDest=null;
	}

	vDestLocode = colData[11].trim();

	vDestCleaned = colData[10].trim();

	String strLatitude = colData[16].trim();
	pLat = Double.parseDouble(strLatitude);

	String strLongitude = colData[15].trim();
	pLong = Double.parseDouble(strLongitude);

	String strHeading = colData[17].trim();
	if (!strHeading.equals(""))
	{
	    pHdg = Double.parseDouble(strHeading);
	}
	else
	{
	    pHdg= 0.0D;
	}

	String strDraught = colData[8].trim();
	if (!strDraught.equals(""))
	{
	    vDraught  = Double.parseDouble(strDraught);
	}
	else
	{
	    vDraught =null;
	}

	String strSpeed = colData[18].trim();

	if (!strSpeed.equals(""))
	{
	    pSpeed = Double.parseDouble(strSpeed);
	}
	else
	{
	    pSpeed=null;
	}

	String strETA = colData[12].trim();
	if (!strETA.equals(""))
	{
	    vEta  = strToDate(strETA);
	}
	else
	{
	    vEta  = null;
	}

	pSource = colData[23].trim();
	String strDatasource=null;
	if (pSource.equals("T-AIS"))
	{
	    strDatasource = "VesselTracker";
	}
	else if (pSource.equals("S-AIS"))
	{
	    strDatasource = "AIS";
	}
	else
	{
	    // for other datasource all set VesselTracker, because most datasource is
	    // empty
	    strDatasource = "VesselTracker";
	}

	dit_datasource_id=VesselDataSource.getInstance(strDatasource).getID();

	pStatus = colData[19].trim();
	String strStatus = VTLocationLoader.statusProp.getProperty(pStatus);
	if (strStatus!=null)
	{
	    dit_status_id=AssetStatus.getInstance(strStatus).getID();
	}
	else
	{
	    dit_status_id=-1;
	}

	sCallsign  = colData[3].trim();

	String strLength = colData[5].trim();
	if (!strLength.equals(""))
	{
	    sLength =Double.parseDouble(strLength);
	}
	else
	{
	    sLength =null;
	}

	String strWidth = colData[6].trim();	
	if (!strWidth.equals(""))
	{
	    sWidth =Double.parseDouble(strWidth);
	}
	else
	{
	    sWidth =null;
	}

    }


    private static java.sql.Timestamp strToDate(String strDate) {

	strDate=strDate.substring(0, 19);

	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	ParsePosition pos = new ParsePosition(0);
	java.sql.Timestamp strtodate = new java.sql.Timestamp(formatter.parse(strDate, pos).getTime());
	return strtodate;

    }




}	

