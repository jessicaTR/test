package com.thomsonreuters.ce.dbor.vessel.location;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.ArrayList;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;

import oracle.jdbc.OracleTypes;
import au.com.bytecode.opencsv.CSVParser;

import com.thomsonreuters.ce.dbor.server.DBConnNames;
import com.thomsonreuters.ce.exception.SystemException;
import com.thomsonreuters.ce.dbor.file.DateOrderedFileWrapper;
import com.thomsonreuters.ce.dbor.file.ExtensionFilter;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.cache.FileCategory;
import com.thomsonreuters.ce.dbor.cache.MsgCategory;
import com.thomsonreuters.ce.dbor.cache.ProcessingStatus;
import com.thomsonreuters.ce.dbor.pasdi.SearchSDIIncrementalLoad;
import com.thomsonreuters.ce.database.EasyConnection;
import com.thomsonreuters.ce.thread.ControlledThread;
import com.thomsonreuters.ce.thread.ThreadController;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.geotools.geometry.jts.JTS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.crs.DefaultGeographicCRS;



public class VTLocationLoader extends ControlledThread {

    private static final String InsertFileProcessHistory = "insert into file_process_history (id, file_name,dit_file_category_id,start_time,dit_processing_status) values (fph_seq.nextval,?,?,sysdate,?)";
    private static final String CompleteFileHistory = "update file_process_history set end_time=sysdate, dit_processing_status=? where id=?";
    private static final String InsertProcessingDetail = "insert into processing_detail (fph_id,log_time,dit_message_category_id,message) values (?,sysdate,?,?)";
    private static final String GetProcessingDetail = "select count(*) from processing_detail where fph_id = ? and dit_message_category_id in (select id from dimension_item where value='WARNING')";

    private static final String GetVesID="{ ? = call vessel_load_pkg.get_vt_ves_fn(?,?,?,?,?,?,?,?)}";
    private static final String GetEventsByOutPoint="SELECT id, czo_id,vlo_entry_id entry_point_id, entry_time, vlo_out_id out_point_id, out_time, is_closed  FROM vessel_event WHERE vlo_out_id= ?";

    private static final String GetEventsByEntryPoint="SELECT id, czo_id,vlo_entry_id entry_point_id, entry_time, vlo_out_id out_point_id, out_time, is_closed  FROM vessel_event WHERE vlo_entry_id= ?";

    private static final String GetEventsByTimeRange="SELECT id, czo_id, vlo_entry_id entry_point_id, vlo_out_id out_point_id,is_closed  FROM vessel_event WHERE ves_id = ? and entry_time < ? and out_time > ?";

    private static final String GetAfterPointByTime="select id "
	    +"from (select id, axsmarine_id,record_time,destination, row_number() over(partition by ves_id order by record_time) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time > ?) t "
	    +"where t.rn = 1";

    private static final String GetBeforePointByTime="select id "
	    +"from (select id,axsmarine_id,record_time,destination,"
	    +"row_number() over(partition by ves_id order by record_time  desc) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time <?) t "
	    +"where t.rn = 1";	


    private static final String GetVesselProductType="select pas.pat_id pat_id from physical_asset pas where pas.ves_id=?";

    private static final String Get2BeforeUntil2AfterLocations="select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time  desc) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time < ?) t "
	    +"where t.rn in (1, 2) "
	    +"union all "
	    +"select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time > ?) t "
	    +"where t.rn in (1,2) "
	    +"union all "
	    +"select id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time <= ? "
	    +"and record_time >= ? ";

    private static final String Get2BeforeUntilLastLocation="select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time  desc) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time < ?) t "
	    +"where t.rn in (1, 2) "
	    +"union "
	    +"select id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time >= ? ";

    private static final String Get2BeforeAnd2AfterLocations="select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time  desc) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time < ?) t "
	    +"where t.rn in (1, 2) "
	    +"union all "
	    +"select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time > ?) t "
	    +"where t.rn in (1,2) ";

    private static final String Get2BeforeLocations="select  id,speed,longitude,latitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught from (select id,speed,latitude,longitude,is_speed_change,is_dud,record_time,eta,heading,dit_status_id,destination,ais_draught, "
	    +"row_number() over(partition by ves_id order by record_time  desc) rn "
	    +"from vessel_location "
	    +"where ves_id = ? "
	    +"and record_time < ?) t "
	    +"where t.rn in (1, 2) ";


    private static final String GetAllOverlappingEvents="SELECT id, czo_id, vlo_entry_id entry_point_id, entry_time, vlo_out_id out_point_id,out_time, is_closed  FROM vessel_event WHERE ves_id = ? and entry_time <= ? and out_time >= ?";

    private static final String GetBeforeOverlappingEvents="SELECT id, czo_id, vlo_entry_id entry_point_id, entry_time, vlo_out_id out_point_id, out_time,is_closed  FROM vessel_event WHERE ves_id = ? and out_time >= ?";

    private static final String delete_his_event_proc="{ call vessel_load_pkg.delete_his_event_proc(?,?,?,?)}";

    private static final String delete_his_location_proc="{ call vessel_load_pkg.delete_his_location_proc(?,?,?)}";

    private static final String delete_his_event_proc_with_id="{call vessel_load_pkg.delete_his_event_proc(?)}";

	private final static String location_status_Config_File = "../cfg/vtlocation/vtlocationstatus.conf";
	
	private final static String Scanner_Config_File = "../cfg/vtlocation/vtlocation.conf";
	
    public static Properties statusProp = new Properties();

    private static String VTLocationStatus="";

    private static String SDIStatus="";

    private static long Interval;
    private static String FileExtension;
    private static String HistoryRebuildFilePattern;
    private static String ArrivalFolder;
    private static String WorkFolder;
    private static String ArchiveFolder;
    private static String UnprocFolder;
    private static int ThreadNum;
    public static String ZoneDumpFileName;
    public static int MaxLocFileCountInSDI;
    public static int VTFilesPerSDI;
    public static String ricfolder;
    public static String tempfolder;
    public static double speedthrottle;
	public static Logger VTLocationLogger;

    public static CoordinateReferenceSystem CRS = DefaultGeographicCRS.WGS84;

    protected long FPH_ID;


    static 
    {		
	try {
		
		VTLocationLogger=Logger.getLogger("vtlocation");
		
	   	Properties ScannerProp = new Properties();
	    ScannerProp.load(new FileInputStream(Scanner_Config_File));
	    Interval=Long.parseLong(ScannerProp.getProperty("checkinterval"));
	    FileExtension=ScannerProp.getProperty("fileextension");
	    HistoryRebuildFilePattern=ScannerProp.getProperty("historyrebuildfilepattern");
	    ArrivalFolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("arrival"));
	    WorkFolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("work"));
	    ArchiveFolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("archive"));
	    UnprocFolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("unproc"));
	    ThreadNum=Integer.parseInt(ScannerProp.getProperty("threadnum"));
	    ZoneDumpFileName=ScannerProp.getProperty("zonedumpfile");
	    MaxLocFileCountInSDI=Integer.parseInt(ScannerProp.getProperty("maxlocfilecountinsdi"));
	    VTFilesPerSDI=Integer.parseInt(ScannerProp.getProperty("vtfilespersdi"));
	    ricfolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("ricfolder"));
	    speedthrottle=Double.valueOf(ScannerProp.getProperty("speedthrottle"));
	    
		/////////////////////////////////////////////////////////////////////////////
		// Temp folder
		/////////////////////////////////////////////////////////////////////////////
	    tempfolder=FileUtilities.GetAbsolutePathFromEnv(ScannerProp.getProperty("tempfolder"));
		
	    cleanup(WorkFolder,ArrivalFolder,FileExtension);
	    cleanup(WorkFolder,ArrivalFolder,HistoryRebuildFilePattern);

	    VTLocationLogger.info("Loading location status config file: " + location_status_Config_File);
	    statusProp.load(new FileInputStream(location_status_Config_File));

	} catch (Exception e) {
	    VTLocationLogger.warn( "Load config file failed!", e);
	    throw new SystemException("Load config file failed!");
	}
    }

    private File InZipLocFile=null;



    Timestamp  FileTimeStamp =null;

    private int ThreadCounter=0;

    private int NumberOfFileConsumed=0;


    public FileCategory getFileCatory() {
	return FileCategory.getInstance("LOCATION EVENT");
    }

	public VTLocationLoader(ThreadController tc)
	{
		super(tc);
	}

    @Override
    public void ControlledProcess() {

	File Arrival= new File(ArrivalFolder);
	ExtensionFilter EF = new ExtensionFilter(FileExtension);
	File[] FileList = Arrival.listFiles(EF);

	VTLocationLogger.debug( FileList.length
		+ " file(s) patterned with "+FileExtension+" has been found in arrival folder: "
		+ ArrivalFolder);

	DateOrderedFileWrapper[] fileWrappers = new DateOrderedFileWrapper[FileList.length];
	for (int i = 0; i < FileList.length; i++) {
	    fileWrappers[i] = new DateOrderedFileWrapper(FileList[i]);
	}
	// Sorting
	Arrays.sort(fileWrappers);

	// processing

	if (fileWrappers.length>0)
	{
	    File[] FilesToProcess;
	    if (fileWrappers.length>MaxLocFileCountInSDI)
	    {
		FilesToProcess=new File[MaxLocFileCountInSDI];
	    }
	    else
	    {
		FilesToProcess=new File[fileWrappers.length];
	    }

	    for (int i=0;i<FilesToProcess.length;i++)
	    {
		FilesToProcess[i]=fileWrappers[i].getFile();
	    }

	    this.NumberOfFileConsumed=this.NumberOfFileConsumed+FilesToProcess.length;

	    if ((this.NumberOfFileConsumed>=VTFilesPerSDI))
	    {	
		ProcessFile(FilesToProcess,true);
		this.NumberOfFileConsumed=0;
	    }
	    else
	    {
		ProcessFile(FilesToProcess,false);				
	    }

	}

	//History rebuild
	EF = new ExtensionFilter(HistoryRebuildFilePattern);
	FileList = Arrival.listFiles(EF);

	if (FileList.length>0)
	{
	    ProcessHistoryFile(FileList);
	}

	if (IsShuttingDown())
	{
	    return;
	}

	VTLocationLoader VTLEL=new VTLocationLoader(this.TC);
	VTLEL.NumberOfFileConsumed=this.NumberOfFileConsumed;

	if (fileWrappers.length==0)
	{			
	    Starter.TimerService.createTimer(Interval,0, VTLEL);
	}
	else 
	{
	    Starter.TimerService.createTimer(0,0, VTLEL);
	}

    }


    private void ProcessHistoryFile(File[] fileList) {


	File HistoryFile=fileList[0];
	String FileName=HistoryFile.getName();

	//Move to work folder
	File WorkFile=new File(WorkFolder+ FileName);
	MoveFile(HistoryFile, WorkFile);

	VTLocationLogger.debug( "History rebuild files "
		+ FileName
		+ " have been moved to work folder: "
		+ WorkFolder);

	//Process history file in work folder
	VTLocationLogger.info("File processor starts processing files: "+ FileName);


	/////////////////////////////////////////////
	//Create file processing history			
	CreateFileProcessHistory(FileName);

	long starttime=new Date().getTime();

	try {
	    //Unzip location data file to temp folder	
	    InZipLocFile=new File(tempfolder,"Tempfileforhistoryrebuild");

	    if (InZipLocFile.exists())
	    {
		InZipLocFile.delete();
	    }
	    OutputStream OS = new FileOutputStream(InZipLocFile);

	    ZipFile ZippedFile = new ZipFile(WorkFile);
	    Enumeration ZippedFiles = ZippedFile.entries();

	    while(ZippedFiles.hasMoreElements())
	    {
		ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
		if (entry.getName().equals("export.csv"))
		{
		    InputStream IS = ZippedFile.getInputStream(entry);
		    int count;
		    byte[] buffer=new byte[1024];

		    while((count=IS.read(buffer))!=-1)
		    {
			OS.write(buffer,0,count);
		    }
		    IS.close();
		}					
	    }				
	    ZippedFile.close();				
	    OS.close();

	    ////////////////////////////////////////////////
	    //Starting file parsing
	    RandomAccessFile RAF = new RandomAccessFile(InZipLocFile,"r");	
	    RAF.seek(0);

	    String line = null;		
	    long TempShip=0;
	    long[] Positions=new long[0];					

	    HashMap<Long, long[]> SHIPID_LOC_MAP= new HashMap<Long, long[]>();
	    CSVParser CSVP=new CSVParser(',','"','\\',true,false);

	    String[] Headers = RAF.readLine().split(",");

	    long currentPos=RAF.getFilePointer();

	    while ((line = RAF.readLine()) != null) {

		String[] colData = null;
		String ShipID;

		try {
		    colData = CSVP.parseLine(line);
		    ShipID = colData[0].trim();
		} catch (Exception e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("Data parsing error, so skip line: "+line, e );
		    currentPos=RAF.getFilePointer();
		    continue;
		}

		if (!ShipID.equals(""))
		{
		    long ship_id=Integer.parseInt(ShipID);

		    if (TempShip!=0 && TempShip !=ship_id )
		    {
			long[] Existing_Positions=SHIPID_LOC_MAP.get(TempShip);

			if (Existing_Positions==null)
			{									
			    SHIPID_LOC_MAP.put(TempShip, Positions);
			}
			else
			{
			    long[] TempPositions=new long[Existing_Positions.length+Positions.length];
			    System.arraycopy(Existing_Positions, 0, TempPositions, 0, Existing_Positions.length);
			    System.arraycopy(Positions, 0, TempPositions, Existing_Positions.length, Positions.length);
			    SHIPID_LOC_MAP.put(TempShip, TempPositions);

			}
			Positions=new long[0];								
		    }

		    TempShip=ship_id;
		    long[] TempPositions=new long[Positions.length+1];
		    System.arraycopy(Positions, 0, TempPositions, 0, Positions.length);
		    TempPositions[Positions.length]=currentPos;
		    Positions=TempPositions;

		}
		else
		{
		    VTLocationLogger.warn("Skip line because of blank ship id: "+line );
		}

		currentPos=RAF.getFilePointer();
	    }

	    if (TempShip!=0)
	    {
		long[] Existing_Positions=SHIPID_LOC_MAP.get(TempShip);

		if (Existing_Positions==null)
		{									
		    SHIPID_LOC_MAP.put(TempShip, Positions);
		}
		else
		{
		    long[] TempPositions=new long[Existing_Positions.length+Positions.length];
		    System.arraycopy(Existing_Positions, 0, TempPositions, 0, Existing_Positions.length);
		    System.arraycopy(Positions, 0, TempPositions, Existing_Positions.length, Positions.length);
		    SHIPID_LOC_MAP.put(TempShip, TempPositions);									
		}
	    }

	    ArrayList<long[]> IMOPositions = new ArrayList<long[]>();

	    Iterator<long[]> it = SHIPID_LOC_MAP.values().iterator();
	    while (it.hasNext()) {
		long[] currentPositions = it.next();
		IMOPositions.add(currentPositions);
	    }
	    SHIPID_LOC_MAP.clear();

	    long FileParsingEndTime=new Date().getTime();			
	    long FileParsingTime=FileParsingEndTime-starttime;
	    VTLocationLogger.info("LocationLoader spends "+ FileParsingTime+" on parsing historical data files");

	    //Unload all polygons from database
	    /////////////////////////////////////////
	    Zone.LoadZonePolygons();

	    long PolygonsUnloadingTime=new Date().getTime()-FileParsingEndTime;
	    VTLocationLogger.info("Unload all polygons from database takes "+ PolygonsUnloadingTime);

	    //Determine what current historical rebuild is for by file name

	    if (FileName.startsWith("outagerefill"))
	    {
		//rebuild history for ISuite outage
		/////////////////////////////////////////

		for (int i=0; i<ThreadNum;i++)
		{
		    new Thread(new OutageRefill(IMOPositions,Headers)).start();
		}

	    }
	    else if (FileName.startsWith("polygonchange"))
	    {
		//rebuild history for polygonchange

		int start_pos=FileName.indexOf("_");
		int end_pos=FileName.indexOf("@");
		String polygon_ids=FileName.substring(start_pos+1,end_pos);
		String[] Polygons=polygon_ids.split("_");
		int[] CZO_IDs= new int[Polygons.length];

		String timespan=FileName.substring(end_pos+1);
		int separator_pos=timespan.indexOf("-");

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		ParsePosition pos = new ParsePosition(0);

		String strstart_time=timespan.substring(0,separator_pos);
		Timestamp start_time=new Timestamp(formatter.parse(strstart_time, pos).getTime());
		String strend_time=timespan.substring(separator_pos+1,separator_pos+15);
		pos = new ParsePosition(0);
		Timestamp end_time=new Timestamp(formatter.parse(strend_time, pos).getTime());

		for (int i=0;i<Polygons.length;i++)
		{
		    int Axsmarine_id=Integer.parseInt(Polygons[i]);
		    for (Map.Entry<Integer, Zone> thisEntry : Zone.ZoneMap.entrySet())
		    {
			Zone thisZone=thisEntry.getValue();
			if (thisZone.getAxsmarine_ID()==Axsmarine_id)
			{
			    CZO_IDs[i]=thisZone.getCzo_ID();
			}

		    }
		}

		for (int i=0; i<ThreadNum;i++)
		{
		    new Thread(new PolygonChange(IMOPositions,Headers,CZO_IDs,start_time,end_time)).start();
		}


	    }
	    else if (FileName.startsWith("vesselhistory"))
	    {
		//rebuild vessel history

		int start_pos=FileName.indexOf("_");
		int end_pos=FileName.indexOf("@");
		String imos=FileName.substring(start_pos+1,end_pos);
		String[] tempIMOs=imos.split("_");
		int[] IMOs=new int[tempIMOs.length];

		for (int i=0; i<tempIMOs.length ; i++ )
		{
		    IMOs[i]=Integer.parseInt(tempIMOs[i]);
		}

		String timespan=FileName.substring(end_pos+1);
		int separator_pos=timespan.indexOf("-");

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		ParsePosition pos = new ParsePosition(0);

		String strstart_time=timespan.substring(0,separator_pos);
		Timestamp start_time=new Timestamp(formatter.parse(strstart_time, pos).getTime());
		String strend_time=timespan.substring(separator_pos+1,separator_pos+15);
		pos = new ParsePosition(0);
		Timestamp end_time=new Timestamp(formatter.parse(strend_time, pos).getTime());

		for (int i=0; i<ThreadNum;i++)
		{
		    new Thread(new VesselHistory(IMOPositions,Headers,IMOs,start_time,end_time)).start();
		}			

	    }

	    synchronized (VTLocationLoader.this)
	    {
		if (VTLocationLoader.this.ThreadCounter>0)
		{
		    VTLocationLoader.this.wait();
		}
	    }

	    long DataLoadEndTime=new Date().getTime();
	    long DataLoadTime=DataLoadEndTime-FileParsingEndTime;
	    VTLocationLogger.info("LocationLoader spends "+ DataLoadTime+" on loading historical data");
	    InZipLocFile.delete();
	    CompleteFileHis();
	    VTLocationLogger.info("File processor has completed file: "+ FileName);

	} catch (Exception e) {

	    LogDetails(MsgCategory.WARN, e.getMessage());
	    VTLocationLogger.warn(	"Unknown exception is captured while processing file: "
		    + FileName, e);

	    UpdateFileHisToFailed();

	    ////////////////////////////////////////
	    // move to unproc
	    try {
		VTLocationLogger.debug(
			"Starts moving feed file: " + FileName
			+ " to upproc folder: "
			+ UnprocFolder);

		File UnprocFile=new File(UnprocFolder + WorkFile.getName());
		MoveFile(WorkFile, UnprocFile);

		VTLocationLogger.debug( "Files: "
			+ FileName
			+ " has been moved to unproc folder: "
			+ UnprocFolder);

	    } catch (Exception e1) {
		VTLocationLogger.warn(
			"Can not move file to unproc folder, please investigate.",
			e1);				
	    }	
	}

	/////////////////////////////////////////////
	// Move to archive
	try {
	    VTLocationLogger.debug(
		    "starts moving file to archive folder: "
			    + ArchiveFolder);

	    File ArchiveFile=new File(ArchiveFolder + FileName);
	    MoveFile(WorkFile, ArchiveFile);

	    VTLocationLogger.debug( "File: "
		    + FileName
		    + " have been moved to archive folder: "
		    + ArchiveFolder);
	} catch (Exception e) {
	    VTLocationLogger.warn( "Can not move file to archive folder, please investigate."
		    , e);

	}		


    }


    public void ProcessFile(File[] FeedFiles, boolean NeedsSDI) {


	/////////////////////////////////////////////
	// move to work
	String FileNames="";

	FileNames="("+FeedFiles.length+") files from:"+FeedFiles[0].getName()+" to:"+FeedFiles[FeedFiles.length-1].getName();

	File[] WorkingFiles=new File[FeedFiles.length];
	try {
	    VTLocationLogger.debug( "Starts moving feed files: "
		    + FileNames + " to work folder: "
		    + WorkFolder);


	    for (int i=0;i<FeedFiles.length;i++)
	    {
		WorkingFiles[i]=new File(WorkFolder	+ FeedFiles[i].getName());
		MoveFile(FeedFiles[i], WorkingFiles[i]);
	    }

	    VTLocationLogger.debug( "Feed files: "
		    + FileNames
		    + " have been moved to work folder: "
		    + WorkFolder);
	} catch (Exception e) {
	    VTLocationLogger.warn( "Can not move files: "+FileNames+" to work folder, please investigate."
		    , e);
	    return;
	}

	try {

	    VTLocationLogger.info(
		    "File processor starts processing files: "+ FileNames);	
	    /////////////////////////////////////////////
	    //Create file processing history			
	    CreateFileProcessHistory(FileNames);			

	    long starttime=new Date().getTime();


	    /////////////////////////////////////////
	    //Organize and Sort locations and events for each vessel
	    /////////////////////////////////////////			

	    //Unzip location data file to temp folder	
	    InZipLocFile=new File(tempfolder,"TempfileforVTLocation");

	    if (InZipLocFile.exists())
	    {
		InZipLocFile.delete();
	    }

	    OutputStream OS = new FileOutputStream(InZipLocFile);

	    for (File WorkingFile:WorkingFiles)
	    {
		ZipFile ZippedFile = new ZipFile(WorkingFile);
		Enumeration ZippedFiles = ZippedFile.entries();

		while(ZippedFiles.hasMoreElements())
		{
		    ZipEntry entry = (ZipEntry) ZippedFiles.nextElement();
		    String FileName = entry.getName();

		    if ((FileName.indexOf("CurrentLocation_") >= 0 ))
		    {
			InputStream IS = ZippedFile.getInputStream(entry);
			int count;
			byte[] buffer=new byte[1024];

			while((count=IS.read(buffer))!=-1)
			{
			    OS.write(buffer,0,count);
			}
			IS.close();
		    }					
		}				
		ZippedFile.close();				
	    }

	    OS.close();

	    ////////////////////////////////////////////////
	    //Starting file parsing
	    RandomAccessFile RAF = new RandomAccessFile(InZipLocFile,"r");	
	    RAF.seek(0);

	    String line = null;		
	    long TempShip=0;
	    long[] Positions=new long[0];					
	    long currentPos=0;

	    HashMap<Long, long[]> SHIPID_LOC_MAP= new HashMap<Long, long[]>();
	    CSVParser CSVP=new CSVParser(',','"','\\',true,false);

	    while ((line = RAF.readLine()) != null) {
		String[] colData = null;

		try {
		    colData = CSVP.parseLine(line);
		} catch (Exception e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

		String ShipID;
		try {
		    ShipID = colData[0].trim();
		} catch (Exception e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("Data parsing error, so skip line: "+line, e );
		    continue;
		}

		if (!ShipID.equals(""))
		{
		    long ship_id=Integer.parseInt(ShipID);

		    if (TempShip!=0 && TempShip !=ship_id )
		    {
			long[] Existing_Positions=SHIPID_LOC_MAP.get(TempShip);

			if (Existing_Positions==null)
			{									
			    SHIPID_LOC_MAP.put(TempShip, Positions);
			}
			else
			{
			    long[] TempPositions=new long[Existing_Positions.length+Positions.length];
			    System.arraycopy(Existing_Positions, 0, TempPositions, 0, Existing_Positions.length);
			    System.arraycopy(Positions, 0, TempPositions, Existing_Positions.length, Positions.length);
			    SHIPID_LOC_MAP.put(TempShip, TempPositions);

			}
			Positions=new long[0];								
		    }

		    TempShip=ship_id;
		    long[] TempPositions=new long[Positions.length+1];
		    System.arraycopy(Positions, 0, TempPositions, 0, Positions.length);
		    TempPositions[Positions.length]=currentPos;
		    Positions=TempPositions;

		}
		else
		{
		    VTLocationLogger.warn("Skip line because of blank ship id: "+line );
		}

		currentPos=RAF.getFilePointer();
	    }

	    if (TempShip!=0)
	    {
		long[] Existing_Positions=SHIPID_LOC_MAP.get(TempShip);

		if (Existing_Positions==null)
		{									
		    SHIPID_LOC_MAP.put(TempShip, Positions);
		}
		else
		{
		    long[] TempPositions=new long[Existing_Positions.length+Positions.length];
		    System.arraycopy(Existing_Positions, 0, TempPositions, 0, Existing_Positions.length);
		    System.arraycopy(Positions, 0, TempPositions, Existing_Positions.length, Positions.length);
		    SHIPID_LOC_MAP.put(TempShip, TempPositions);									
		}
	    }

	    ArrayList<long[]> IMOPositions = new ArrayList<long[]>();

	    Iterator<long[]> it = SHIPID_LOC_MAP.values().iterator();
	    while (it.hasNext()) {
		long[] currentPositions = it.next();
		IMOPositions.add(currentPositions);
	    }
	    SHIPID_LOC_MAP.clear();

	    long FileParsingEndTime=new Date().getTime();			
	    long FileParsingTime=FileParsingEndTime-starttime;
	    VTLocationLogger.info("LocationLoader spends "+ FileParsingTime+" on parsing files");

	    /////////////////////////////////////////
	    //Unload all polygons from database
	    /////////////////////////////////////////
	    Zone.LoadZonePolygons();

	    long PolygonsUnloadingTime=new Date().getTime()-FileParsingEndTime;
	    VTLocationLogger.info("Unload all polygons from database takes "+ PolygonsUnloadingTime);


	    /////////////////////////////////////////

	    for (int i=0; i<ThreadNum;i++)
	    {
		new Thread(new CurrentLoad(IMOPositions)).start();
	    }


	    synchronized (VTLocationLoader.this)
	    {
		if (VTLocationLoader.this.ThreadCounter>0)
		{
		    VTLocationLoader.this.wait();
		}
	    }

	    long DataLoadEndTime=new Date().getTime();
	    long DataLoadTime=DataLoadEndTime-FileParsingEndTime;
	    VTLocationLogger.info("LocationLoader spends "+ DataLoadTime+" on loading data");

	    InZipLocFile.delete();

	    ////////////////////////////////////////////////
	    //update last update in case part of file was inserted into DB
	    UpdateLastUpdateDate();

	    File Arrival= new File(ArrivalFolder);
	    ExtensionFilter EF = new ExtensionFilter(FileExtension);
	    String LastFileName=FeedFiles[FeedFiles.length-1].getName();
	    String DBTimepoint=getDBDate();			
	    String TheNumberOfFilesInBacklog=String.valueOf(Arrival.listFiles(EF).length);
	    VTLocationStatus=DBTimepoint+","+LastFileName+","+TheNumberOfFilesInBacklog;

	    GenerateRicFile();

	    CompleteFileHis();
	    VTLocationLogger.info("File processor has completed file: "+ FileNames);

	    long TimeUsed = new Date().getTime() - starttime;

	    if (NeedsSDI && (TimeUsed > 60000)) {
		String ManiFest_Name = SearchSDIIncrementalLoad
			.GenerateIncrementalSDI();
		DBTimepoint = getDBDate();
		SDIStatus = DBTimepoint + "," + ManiFest_Name;
		GenerateRicFile();
	    }

	} catch (Exception e) {

	    LogDetails(MsgCategory.WARN, e.getMessage());
	    VTLocationLogger.warn(	"Unknown exception is captured while processing files: "
		    + FileNames, e);

	    UpdateFileHisToFailed();

	    ////////////////////////////////////////
	    // move to unproc
	    try {
		VTLocationLogger.debug(
			"Starts moving feed files: " + FileNames
			+ " to upproc folder: "
			+ UnprocFolder);

		File[] UnprocFiles=new File[WorkingFiles.length];

		for (int i=0;i<WorkingFiles.length;i++)
		{
		    UnprocFiles[i]=new File(UnprocFolder + WorkingFiles[i].getName());
		    MoveFile(WorkingFiles[i], UnprocFiles[i]);
		}

		VTLocationLogger.debug( "Files: "
			+ FileNames
			+ " has been moved to unproc folder: "
			+ UnprocFolder);

	    } catch (Exception e1) {
		VTLocationLogger.warn(
			"Can not move file to unproc folder, please investigate.",
			e1);				
	    }	
	    return;

	}

	/////////////////////////////////////////////
	// Move to archive
	try {
	    VTLocationLogger.debug(
		    "starts moving files to archive folder: "
			    + ArchiveFolder);

	    File[] ArchiveFiles=new File[WorkingFiles.length];

	    for (int i=0;i<WorkingFiles.length;i++)
	    {
		ArchiveFiles[i]=new File(ArchiveFolder + WorkingFiles[i].getName());
		MoveFile(WorkingFiles[i], ArchiveFiles[i]);
	    }

	    VTLocationLogger.debug( "Files: "
		    + FileNames
		    + " have been moved to archive folder: "
		    + ArchiveFolder);
	} catch (Exception e) {
	    VTLocationLogger.warn( "Can not move file to archive folder, please investigate."
		    , e);
	    return;
	}

    }	


    public static void cleanup(String WorkFolder,String ArrivalFolder,String FileExtension)
    {
	VTLocationLogger.debug("Cleanup started.");

	//get all files from working folder
	File f = new File(WorkFolder);
	File[] files = f.listFiles(new ExtensionFilter(FileExtension));

	for(int i = 0; i < files.length; i++)
	{
	    String fileName = files[i].getName();
	    File arrivalFile = new File(ArrivalFolder	+ fileName);
	    File workFile = new File(WorkFolder	+  fileName);
	    MoveFile(workFile,arrivalFile);
	    VTLocationLogger.info("Cleanup: Working File moved back to arrival: " + fileName);
	}
	VTLocationLogger.debug("Cleanup for location event files finished.");
    }

    public void CompleteFileHis()
    {
	Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);


	try {
	    // if processint_detail table has records, then it's
	    // COMPLETEDWITHWARNING
	    int pdeCount=-1;
	    PreparedStatement getPdetPreStatement = DBConn
		    .prepareStatement(GetProcessingDetail);
	    getPdetPreStatement.setLong(1, this.FPH_ID);
	    ResultSet objResultSet = getPdetPreStatement.executeQuery();
	    if (objResultSet.next()) {
		pdeCount = objResultSet.getInt(1);
	    }

	    objResultSet.close();
	    getPdetPreStatement.close();

	    PreparedStatement objPreStatement = null;
	    objPreStatement = DBConn.prepareStatement(CompleteFileHistory);

	    if (pdeCount <= 0) {
		objPreStatement.setInt(1, ProcessingStatus.COMPLETED.getID());
	    } else {
		objPreStatement.setInt(1, ProcessingStatus.COMPLETEDWITHWARNING
			.getID());
	    }			

	    objPreStatement.setLong(2,this.FPH_ID);
	    objPreStatement.executeUpdate();
	    DBConn.commit();
	    objPreStatement.close();

	} catch (SQLException e) {
	    // TODO Auto-generated catch block
	    VTLocationLogger.warn("SQL exception",e);
	}finally
	{
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		VTLocationLogger.warn("SQL exception",e);
	    }												
	}			
    }

    public  void LogDetails(MsgCategory mc, String Msg)
    {
	Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
	try {		
	    PreparedStatement objPreStatement = DBConn.prepareStatement(InsertProcessingDetail);
	    objPreStatement.setLong(1, this.FPH_ID);
	    objPreStatement.setInt(2, mc.getID());
	    objPreStatement.setString(3, Msg);

	    objPreStatement.executeUpdate();

	    DBConn.commit();
	    objPreStatement.close();
	}
	catch (SQLException e) {
	    // TODO Auto-generated catch block
	    VTLocationLogger.warn("SQL exception",e);


	} finally {
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		VTLocationLogger.warn("SQL exception",e);
	    }
	}				

    }	

    public static void MoveFile(File f1, File f2) {
	try {
	    int length = 1048576;
	    FileInputStream in = new FileInputStream(f1);
	    FileOutputStream out = new FileOutputStream(f2);
	    byte[] buffer = new byte[length];

	    while (true) {
		int ins = in.read(buffer);
		if (ins == -1) {
		    in.close();
		    out.flush();
		    out.close();
		    f1.delete();
		    return;
		} else
		{
		    out.write(buffer, 0, ins);
		}
	    }

	} catch (Exception e) {

	    throw new SystemException("File: " + f1.getName()
		    + " can not be moved to "
		    + f2.getAbsolutePath(),e);			
	} 
    }	

    public void UpdateLastUpdateDate(){
	/////////////////////////////////////////
	//Update vessel location and event timestamp on cnr 
	//which will be replicated to apds and 
	//trigger the vessel track mview refresh as soon as possible
	/////////////////////////////////////////
	Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);
	try{	
	    CallableStatement csUpdateLast = DBConn.prepareCall("begin ce_refresh_maintain_pkg.update_last_upd_date_proc('VESSEL LOCATION EVENT');  end;");
	    csUpdateLast.execute();
	    csUpdateLast.close();
	    DBConn.commit();
	    
	}catch(SQLException e){
	    VTLocationLogger.warn(
		    "Failed to update Last Update(to trigger vessel track mview on APDS)" , e );					
	}finally {
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		VTLocationLogger.warn( "SQL exception", e);
	    }
	}
    }

    public void CreateFileProcessHistory(String FeedFileName)
    {
	Connection DBConn= new EasyConnection(DBConnNames.CEF_CNR);


	try {
	    DatabaseMetaData dmd = DBConn.getMetaData();
	    PreparedStatement objPreStatement = DBConn.prepareStatement(InsertFileProcessHistory, new String[]{"ID"});
	    objPreStatement.setString(1, FeedFileName);
	    objPreStatement.setInt(2, getFileCatory().getID());
	    objPreStatement.setInt(3, ProcessingStatus.PROCESSING.getID());

	    objPreStatement.executeUpdate();

	    //get ID			
	    if(dmd.supportsGetGeneratedKeys()) {   
		ResultSet rs = objPreStatement.getGeneratedKeys();   
		while(rs.next()) {
		    this.FPH_ID=rs.getLong(1);
		}
	    }

	    DBConn.commit();
	    objPreStatement.close();
	}
	catch (SQLException e) {
	    // TODO Auto-generated catch block
	    VTLocationLogger.warn("SQL exception",e);


	} finally {
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		VTLocationLogger.warn("SQL exception",e);
	    }
	}		
    }

    public void UpdateFileHisToFailed()
    {
	Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

	try {
	    PreparedStatement objPreStatement = null;
	    objPreStatement = DBConn.prepareStatement(CompleteFileHistory);
	    objPreStatement.setInt(1, ProcessingStatus.FAILED.getID());
	    objPreStatement.setLong(2, this.FPH_ID);
	    objPreStatement.executeUpdate();
	    DBConn.commit();
	    objPreStatement.close();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block
		VTLocationLogger.warn( "SQL exception", e);
	} finally {
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		VTLocationLogger.warn( "SQL exception", e);
	    }
	}
    }

    private class VesselHistory implements Runnable
    {
	private ArrayList<long[]> IMOPositions;
	private String[] Headers;
	private ArrayList<Integer> IMOs =new ArrayList<Integer>();
	private Timestamp Start_time;
	private Timestamp End_time;
	
	public VesselHistory(ArrayList<long[]> IPs, String[] headers, int[] imos,Timestamp start, Timestamp end)
	{
	    IMOPositions=IPs;
	    Headers=headers;
	    Start_time=start;
	    End_time=end;

	    for(int id: imos)
	    {
		IMOs.add(id);
	    }

	    synchronized (VTLocationLoader.this)
	    {
		VTLocationLoader.this.ThreadCounter++;
	    }
	}

	public void run()
	{

	    Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

	    try
	    {
		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}					

		while(true)
		{
		    long[] Positions;

		    synchronized(IMOPositions)
		    {
			if (!IMOPositions.isEmpty())
			{
			    Positions=IMOPositions.remove(0);
			}
			else
			{
			    break;
			}						
		    }

		    try {

			ArrayList<Event> DerivedEventList=new ArrayList<Event>();

			VTLocationRow[] LR=VTLocationLoader.this.ReadHistoryRow(Positions,Headers);
			//Sort locations by record time
			Arrays.sort(LR);

			///////////////////////////
			//Get Vessel Database ID
			///////////////////////////	

			long Ves_id;

			CallableStatement objGetVesselID=null;

			if (!IMOs.contains(LR[0].sImo))
			{
			    continue;
			}

			objGetVesselID = DBConn.prepareCall(GetVesID);
			objGetVesselID.registerOutParameter(1, OracleTypes.DECIMAL);
			objGetVesselID.setLong(2,LR[0].shipId );
			objGetVesselID.setInt(3,LR[0].sImo );
			objGetVesselID.setString(4, LR[0].sName);
			objGetVesselID.setLong(5, LR[0].sMmsi);
			objGetVesselID.setString(6, LR[0].sCallsign);
			objGetVesselID.setInt(7, LR[0].sShiptype );
			if (LR[0].sLength!=null)
			{
			    objGetVesselID.setDouble(8, LR[0].sLength);
			}
			else
			{
			    objGetVesselID.setNull(8, OracleTypes.DECIMAL);
			}

			if (LR[0].sWidth!=null)
			{
			    objGetVesselID.setDouble(9, LR[0].sWidth );
			}
			else
			{
			    objGetVesselID.setNull(9, OracleTypes.DECIMAL);
			}


			objGetVesselID.execute();						
			Ves_id = objGetVesselID.getLong(1);

			objGetVesselID.close();

			if (Ves_id==0)
			{
			    VTLocationLogger.debug("Vessel:  Skip unknown vessel IMO: "+ LR[0].sImo + " ship ID:"+LR[0].shipId);
			    continue;
			}

			
			try{
			    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call vessel_location_purge_pkg.reset_vlp_proc(?)}");
			    callableClearSessionSM.setLong(1, Ves_id);
			    callableClearSessionSM.execute();
			    callableClearSessionSM.close();					

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn("SQLException", e);					
			}

			///////////////////////////
			//Get Vessel Product Type
			///////////////////////////					

			PreparedStatement objGetVesselType = DBConn.prepareStatement(GetVesselProductType);
			objGetVesselType.setLong(1, Ves_id);
			ResultSet objVesselTypeResult = objGetVesselType.executeQuery();

			String VesselProductType=null;

			if (objVesselTypeResult.next())
			{
			    VesselProductType=PatRootType.getInstance(objVesselTypeResult.getInt("pat_id")).getRootType();
			    objVesselTypeResult.close();
			    objGetVesselType.close();

			}
			else
			{
			    VTLocationLogger.debug(
				    "Vessel:  Skip vessel: "+ LR[0].sImo+" ship ID:"+LR[0].shipId + ", because of unknown vessel type!");
			    objVesselTypeResult.close();
			    objGetVesselType.close();
			    continue;
			}



			VTLocationLogger.debug(
				"Vessel location event: start processing historical locations data for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);


			/////////////////////////////////////////
			//Get the last location record time 每 must be open events
			/////////////////////////////////////////	
			VTLocationLogger.debug(
				"Vessel:  start retrieving latest events for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			CallableStatement objStatement = DBConn.prepareCall("{call vessel_load_pkg.get_lat_location_proc(?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			objStatement.setLong(1, Ves_id);
			objStatement.registerOutParameter(2, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(3, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(4, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(5, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(6, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(7, OracleTypes.VARCHAR);						
			objStatement.registerOutParameter(8, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(9, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(10, OracleTypes.FLOAT);
			objStatement.registerOutParameter(11, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(12, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(13, OracleTypes.FLOAT);
			objStatement.execute();


			long last_point_id=objStatement.getLong(2);
			Double pSpeed=objStatement.getDouble(3);
			if (objStatement.wasNull())
			{
			    pSpeed=null;
			}

			Double pLong=objStatement.getDouble(4);
			Double pLat=objStatement.getDouble(5);

			boolean IsSpeedChange=false;
			String Temp_SpeedChange=objStatement.getString(6);
			if (Temp_SpeedChange!=null && (Temp_SpeedChange.equals("Y")))
			{
			    IsSpeedChange=true;
			}

			boolean IsDud=false;
			String Temp_Dud=objStatement.getString(7);
			if (Temp_Dud!=null && (Temp_Dud.equals("Y")))
			{
			    IsDud=true;
			}

			Timestamp Previous_record_time=objStatement.getTimestamp(8);
			Timestamp ETA=objStatement.getTimestamp(9);
			if (objStatement.wasNull())
			{
			    ETA=null;
			}

			Double Heading=objStatement.getDouble(10);
			if (objStatement.wasNull())
			{
			    Heading=null;
			}

			Integer Dit_status_id=objStatement.getInt(11);
			if (objStatement.wasNull())
			{
			    Dit_status_id=null;
			}

			String Destination=objStatement.getString(12);
			Double Draught=objStatement.getDouble(13);
			if (objStatement.wasNull())
			{
			    Draught=null;
			}						


			DBLocation LastDBLocation=new DBLocation(last_point_id, Previous_record_time, pSpeed,
				pLat, pLong, IsSpeedChange, IsDud,
				ETA, Heading, Dit_status_id,
				Destination, Draught);

			objStatement.close();

			//////////////////////////////
			//Cache points and events

			//Database location cache
			ArrayList<DBLocation> DBLs=new ArrayList<DBLocation>();
			ArrayList<Event> DBEs=new ArrayList<Event>();
			int LogicType=0; 
			//1 for all shore-based locations
			//2 for all satellite locations
			//3 for half satellite half shore-based locations
			//4 for no location in db before

			if (LastDBLocation.locationId!=-1 )
			{
			    if (Start_time.after(LastDBLocation.recordTime))
			    {
				LogicType=1;
				//(1)events with last db point as exit

				PreparedStatement objGetEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				objGetEventsByLastPoints.setLong(1, LastDBLocation.locationId);
				ResultSet ObjEvents=objGetEventsByLastPoints.executeQuery();
				ObjEvents.setFetchSize(100);
				while(ObjEvents.next())
				{
				    long event_id=ObjEvents.getInt("id");
				    int czo_id=ObjEvents.getInt("czo_id");
				    long entry_point_id=ObjEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjEvents.getTimestamp("entry_time");
				    long out_point_id=ObjEvents.getLong("out_point_id");
				    Timestamp out_time=ObjEvents.getTimestamp("entry_time");
				    boolean is_close=(ObjEvents.getString("is_closed")=="N") ? false : true;

				    Event thisEvent=new Event(event_id,czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);

				}	
				ObjEvents.close();
				objGetEventsByLastPoints.close();

			    }
			    else if (!End_time.after(LastDBLocation.recordTime))
			    {
				LogicType=2;

				///////////////////////////////
				//Remove all inside events for that vessel
				///////////////////////////////

				CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc);
				objDelHisEvents.setLong(1,Ves_id );
				objDelHisEvents.setNull(2, OracleTypes.DECIMAL);
				objDelHisEvents.setTimestamp(3, Start_time);
				objDelHisEvents.setTimestamp(4, End_time);
				objDelHisEvents.execute();						
				objDelHisEvents.close();





				//////////////////////////////////////////////////////////////////////////
				//All the locations in file are satellite points, need to cache
				//(1) from 2 db before points until 2 db after points

				PreparedStatement objGet2BeforeAnd2AfterLocations=DBConn.prepareStatement(Get2BeforeAnd2AfterLocations);
				objGet2BeforeAnd2AfterLocations.setLong(1, Ves_id);
				objGet2BeforeAnd2AfterLocations.setTimestamp(2, Start_time );
				objGet2BeforeAnd2AfterLocations.setLong(3, Ves_id);
				objGet2BeforeAnd2AfterLocations.setTimestamp(4, End_time );

				ResultSet PointsOf2BeforeAnd2After=objGet2BeforeAnd2AfterLocations.executeQuery();
				PointsOf2BeforeAnd2After.setFetchSize(1000);
				while(PointsOf2BeforeAnd2After.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeAnd2After.getLong(1);
				    Double point_Speed=PointsOf2BeforeAnd2After.getDouble(2);
				    if (PointsOf2BeforeAnd2After.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeAnd2After.getDouble(3);
				    Double point_Lat=PointsOf2BeforeAnd2After.getDouble(4);

				    String temp_SC=PointsOf2BeforeAnd2After.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeAnd2After.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeAnd2After.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeAnd2After.getTimestamp(8);
				    if (PointsOf2BeforeAnd2After.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeAnd2After.getDouble(9);
				    if (PointsOf2BeforeAnd2After.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeAnd2After.getInt(10);
				    if (PointsOf2BeforeAnd2After.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeAnd2After.getString(11);
				    Double point_ais_draught=PointsOf2BeforeAnd2After.getDouble(12);
				    if (PointsOf2BeforeAnd2After.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught,VesselDataSource.getInstance("VesselTracker").getID());
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeAnd2After.close();
				PointsOf2BeforeAnd2After.close();
				java.util.Collections.sort(DBLs);

				////////////////////////////////////////////////////////////////////////////
				//(2) all events with exit point later than the first point and entry point earlier than the last file point

				PreparedStatement objGetAllOverlappingEvents=DBConn.prepareStatement(GetAllOverlappingEvents);
				objGetAllOverlappingEvents.setLong(1, Ves_id);
				objGetAllOverlappingEvents.setTimestamp(2, End_time );
				objGetAllOverlappingEvents.setTimestamp(3, Start_time );

				ResultSet OverlappingEvents=objGetAllOverlappingEvents.executeQuery();
				OverlappingEvents.setFetchSize(100);
				while(OverlappingEvents.next())
				{
				    long event_id=OverlappingEvents.getLong("id");
				    int zone_czo_id=OverlappingEvents.getInt("czo_id");
				    long entry_point_id=OverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=OverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=OverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=OverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(OverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);
				}
				OverlappingEvents.close();
				objGetAllOverlappingEvents.close();

				////////////////////////////////////////////////////////////////////////////
				//(3) events with before point as exit and events with after point as entry

				////////////////////////////////
				//Get all events which before point is related as exit point
				/////////////////////////////////							

				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(Start_time))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				//Finding after point
				DBLocation AfterPoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.after(End_time))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				}

				//Adjust events
				ArrayList<Event> tempDBEs=new ArrayList<Event>();
				for (Event thisevent : DBEs)
				{
				    if (thisevent.EntryTime.before(Start_time) && thisevent.OutTime.after(End_time))
				    {
					Event AfterEvent= new Event(0, thisevent.Czo_id, AfterPoint.locationId, thisevent.CurrentpointID, thisevent.IsCloseEvent, AfterPoint.recordTime, thisevent.OutTime);
					tempDBEs.add(AfterEvent);
					DerivedEventList.add(AfterEvent);

					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					thisevent.IsCloseEvent=true;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				    else if (thisevent.EntryTime.before(Start_time) && !thisevent.OutTime.before(Start_time))
				    {
					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				    else  if (thisevent.OutTime.after(End_time) && !thisevent.EntryTime.after(End_time) && !thisevent.EntryTime.before(Start_time))
				    {
					thisevent.EntrypointID=AfterPoint.locationId;
					thisevent.EntryTime=AfterPoint.recordTime;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				}

				/////////////////////////////////
				//Populate Derived Events
				/////////////////////////////////
				for (Event aEvent:DerivedEventList)
				{
				    PopulateEvent(DBConn,Ves_id,aEvent);
				}

				DerivedEventList.clear();

				///////////////////////////////
				//Remove all inside points for that vessel
				///////////////////////////////

				CallableStatement objDelHisPoints = DBConn.prepareCall(delete_his_location_proc);
				objDelHisPoints.setLong(1,Ves_id );
				objDelHisPoints.setTimestamp(2, Start_time);
				objDelHisPoints.setTimestamp(3, End_time);
				objDelHisPoints.execute();						
				objDelHisPoints.close();

				DBEs.clear();

				if (BeforePoint!=null)
				{


				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}

				/////////////////////////////////
				//Get all events which after point is related as entry point
				/////////////////////////////////


				if (AfterPoint!=null)
				{


				    PreparedStatement objGetAfterPointEventsByLastPoints=DBConn.prepareStatement(GetEventsByEntryPoint);
				    objGetAfterPointEventsByLastPoints.setLong(1, AfterPoint.locationId);
				    ResultSet ObjAfterPointEvents=objGetAfterPointEventsByLastPoints.executeQuery();
				    ObjAfterPointEvents.setFetchSize(100);
				    while(ObjAfterPointEvents.next())
				    {
					long event_id=ObjAfterPointEvents.getLong("id");
					int zone_czo_id=ObjAfterPointEvents.getInt("czo_id");
					long entry_point_id=ObjAfterPointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjAfterPointEvents.getTimestamp("entry_time");
					long out_point_id=ObjAfterPointEvents.getLong("out_point_id");
					Timestamp out_time=ObjAfterPointEvents.getTimestamp("out_time");
					boolean is_close=(ObjAfterPointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjAfterPointEvents.close();
				    objGetAfterPointEventsByLastPoints.close();
				}




			    }
			    else if (!Start_time.after(LastDBLocation.recordTime) && End_time.after(LastDBLocation.recordTime))
			    {
				LogicType=3;

				///////////////////////////////
				//Remove all inside events for that vessel
				///////////////////////////////

				CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc);
				objDelHisEvents.setLong(1,Ves_id );
				objDelHisEvents.setNull(2, OracleTypes.DECIMAL);
				objDelHisEvents.setTimestamp(3, Start_time);
				objDelHisEvents.setTimestamp(4, LastDBLocation.recordTime);
				objDelHisEvents.execute();						
				objDelHisEvents.close();


				//there are some satellite points in files, need to cache
				//(1) from 2 db before points until the last db point.
				PreparedStatement objGet2BeforeLocations=DBConn.prepareStatement(Get2BeforeLocations);
				objGet2BeforeLocations.setLong(1, Ves_id);
				objGet2BeforeLocations.setTimestamp(2, Start_time );

				ResultSet PointsOf2Before=objGet2BeforeLocations.executeQuery();
				PointsOf2Before.setFetchSize(1000);
				while(PointsOf2Before.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2Before.getLong(1);
				    Double point_Speed=PointsOf2Before.getDouble(2);
				    if (PointsOf2Before.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2Before.getDouble(3);
				    Double point_Lat=PointsOf2Before.getDouble(4);

				    String temp_SC=PointsOf2Before.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2Before.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2Before.getTimestamp(7);

				    Timestamp point_eta=PointsOf2Before.getTimestamp(8);
				    if (PointsOf2Before.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2Before.getDouble(9);
				    if (PointsOf2Before.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2Before.getInt(10);
				    if (PointsOf2Before.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2Before.getString(11);
				    Double point_ais_draught=PointsOf2Before.getDouble(12);
				    if (PointsOf2Before.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught,VesselDataSource.getInstance("VesselTracker").getID());
				    DBLs.add(DBLocation);
				}
				PointsOf2Before.close();
				PointsOf2Before.close();

				java.util.Collections.sort(DBLs);							


				//(2) all events with exit point later than the first point in file

				PreparedStatement objGetBeforeOverlappingEvents=DBConn.prepareStatement(GetBeforeOverlappingEvents);
				objGetBeforeOverlappingEvents.setLong(1, Ves_id);
				objGetBeforeOverlappingEvents.setTimestamp(2, Start_time );

				ResultSet BeforeOverlappingEvents=objGetBeforeOverlappingEvents.executeQuery();
				BeforeOverlappingEvents.setFetchSize(100);
				while(BeforeOverlappingEvents.next())
				{
				    long event_id=BeforeOverlappingEvents.getLong("id");
				    int zone_czo_id=BeforeOverlappingEvents.getInt("czo_id");
				    long entry_point_id=BeforeOverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=BeforeOverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=BeforeOverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=BeforeOverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(BeforeOverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}

				BeforeOverlappingEvents.close();
				objGetBeforeOverlappingEvents.close();							

				//(3) events with before point as exit
				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(Start_time))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				if (BeforePoint!=null)
				{
				    //move current points of events to before points
				    for (Event thisevent : DBEs)
				    {
					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					thisevent.IsCloseEvent=false;
					DerivedEventList.add(thisevent);
				    }

				    /////////////////////////////////
				    //Populate Derived Events
				    /////////////////////////////////
				    for (Event aEvent:DerivedEventList)
				    {
					PopulateEvent(DBConn,Ves_id,aEvent);
				    }

				    DerivedEventList.clear();
				    DBEs.clear();

				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");

					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}

				///////////////////////////////
				//Remove all inside points for that vessel
				///////////////////////////////

				CallableStatement objDelHisPoints = DBConn.prepareCall(delete_his_location_proc);
				objDelHisPoints.setLong(1,Ves_id );
				objDelHisPoints.setTimestamp(2, Start_time);
				objDelHisPoints.setTimestamp(3, LastDBLocation.recordTime);
				objDelHisPoints.execute();						
				objDelHisPoints.close();
			    }

			}
			else
			{
			    LogicType=4;
			}

			//////////////////////////////
			long last_database_location_id=-1;

			HashMap<Integer, Event> PreviousZoneEvents =new HashMap<Integer, Event>();
			HashMap<Integer, Event> PostZoneEvents =new HashMap<Integer, Event>();
			DBLocation BeforePoint = null;
			DBLocation AfterPoint = null;

			int commitcounter=0;
			
			switch (LogicType)
			{
			case 1: //1 for all shore-based locations

			{
			    for (Event thisEvent : DBEs)
			    {
				PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
			    }

			    //Check if the last db location needs to be set to speed point
			    if ((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1) )
			    {
				if (LR[0].pSpeed!=null && LR[0].pSpeed>=1)
				{
				    //update last DB location as speed change point.
				    LastDBLocation.IsSpeedChange=true;
				    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
				}
			    }

			    
			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;							

				if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if (i==0)
				    {
					if (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    IsDud=IsDudPoint(LastDBLocation, thisLOCRow);
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], thisLOCRow);
				}


				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}

			break;
			case 2:	//2 for all satellite locations

			{

			    //The first before point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.before(Start_time))
				{
				    BeforePoint=thisLocation;
				}
				else
				{
				    break;
				}
			    }

			    //collect PreviousZoneEvents    
			    if (BeforePoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.CurrentpointID==BeforePoint.locationId)
				    {
					PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }

			    //Finding after point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.after(End_time))
				{
				    AfterPoint=thisLocation;
				    break;
				}
			    }

			    //collect PostZoneEvents
			    if (AfterPoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.EntrypointID==AfterPoint.locationId)
				    {
					PostZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }

			    for(int i=0;i<LR.length;i++)
			    {
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				long database_location_id=0;


				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);


				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////

				//Determine speedchange flag
				IsSpeedChange=false;
				IsDud=false;

				if (LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if (i==0)
				    {
					if (BeforePoint!=null && BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
					else if (AfterPoint!=null && AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
					else if (AfterPoint!=null && AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    if (BeforePoint!=null)
				    {
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], LR[i]);
				}


				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because of database exception");
				    continue;
				}

				DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				DBLs.add(newLocation);
				java.util.Collections.sort(DBLs);

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,LR[i].pTime,LR[i].pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }

			    //Merger with PostZoneEvents
			    Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
			    while (it.hasNext())
			    {
				Map.Entry<Integer, Event> thisEntry=it.next();
				int Zone_czo_id = thisEntry.getKey();
				Event PreviousZoneEvent=thisEntry.getValue();

				if (PostZoneEvents.containsKey(Zone_czo_id))
				{


				    Event PostZoneEvent=PostZoneEvents.get(Zone_czo_id);

				    PreviousZoneEvent.CurrentpointID=PostZoneEvent.CurrentpointID;
				    PreviousZoneEvent.OutTime=PostZoneEvent.OutTime;
				    PreviousZoneEvent.IsCloseEvent=PostZoneEvent.IsCloseEvent;

				    if (!DerivedEventList.contains(PreviousZoneEvent))
				    {
					DerivedEventList.add(PreviousZoneEvent);
				    }

				    //delete this postzoneevent
				    CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc_with_id);
				    objDelHisEvents.setLong(1,PostZoneEvent.EventID );
				    objDelHisEvents.execute();						
				    objDelHisEvents.close();


				    DBEs.remove(PostZoneEvent);
				    PostZoneEvents.remove(Zone_czo_id);


				    if (DerivedEventList.contains(PostZoneEvent))
				    {
					DerivedEventList.remove(PostZoneEvent);
				    }
				}
				else
				{
				    PreviousZoneEvent.IsCloseEvent=true;

				    if (!DerivedEventList.contains(PreviousZoneEvent))
				    {
					DerivedEventList.add(PreviousZoneEvent);
				    }
				}
			    }
			}

			break;
			case 3:	//3 for half satellite half shore-based locations

			{
			    //The first before point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.before(Start_time))
				{
				    BeforePoint=thisLocation;
				}
				else
				{
				    break;
				}
			    }

			    //collect PreviousZoneEvents    
			    if (BeforePoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.CurrentpointID==BeforePoint.locationId)
				    {
					PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }


			    for(int i=0;i<LR.length;i++)
			    {
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				long database_location_id=0;


				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////

				//Determine speedchange flag
				IsSpeedChange=false;
				IsDud=false;

				if (LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if (i==0)
				    {
					if (BeforePoint!=null && BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  

				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}

				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    if (BeforePoint!=null)
				    {
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], LR[i]);
				}


				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because of database exception");
				    continue;
				}

				DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				DBLs.add(newLocation);
				java.util.Collections.sort(DBLs);

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,LR[i].pTime,LR[i].pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}


			break;
			case 4:	//4 for no location in db before

			{
			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);


				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;

				if ((i==0) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[1].pSpeed!=null && LR[1].pSpeed>=1) )
				    {
					IsSpeedChange=true;
				    }
				}
				else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}
				else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i>0)
				{
				    IsDud=IsDudPoint(LR[i-1],LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}
			break;

			}

			/////////////////////////////////
			//Populate Derived Events
			/////////////////////////////////
			for (Event aEvent:DerivedEventList)
			{
			    PopulateEvent(DBConn,Ves_id,aEvent);
			}

			/////////////////////////////////
			//Update flag of last location
			/////////////////////////////////
			if (last_database_location_id!=-1)
			{
			    UpdateLastLocationFlag(DBConn, Ves_id,last_database_location_id);
			}


			/////////////////////////////////////////
			//Update modify timestamp
			/////////////////////////////////////////
			CallableStatement csSetTime = DBConn.prepareCall("begin vessel_load_pkg.set_change_timestamp_proc(?);  end;");

			try {
			    csSetTime.setLong(1, Ves_id);
			    csSetTime.execute();	

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn(
				    "Failed to change Physical Asset modify_date for I"+ LR[0].sImo+" ship ID:"+LR[0].shipId , e);					
			}		


			DBConn.commit();
			VTLocationLogger.debug(
				"Vessel:  loaded current location event data successfully for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

		    }
		    catch (Exception e) {

			VTLocationLogger.warn(
				"Vessel:  loading current location event data failed" , e);
			e.printStackTrace();
		    }
		}


		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}

	    }
	    finally
	    {
		try {
		    DBConn.close();
		} catch (SQLException e) {
		    VTLocationLogger.warn("SQLException", e);	
		}					

		synchronized (VTLocationLoader.this)
		{
		    VTLocationLoader.this.ThreadCounter--;

		    if (VTLocationLoader.this.ThreadCounter==0)
		    {			
			VTLocationLoader.this.notify();
		    }
		}
	    }
	}
    }

    private class PolygonChange implements Runnable
    {
	private ArrayList<long[]> IMOPositions;
	private String[] Headers;
	private ArrayList<Integer> CZO_IDs =new ArrayList<Integer>();
	private Timestamp Start_time;
	private Timestamp End_time;

	public PolygonChange(ArrayList<long[]> IPs, String[] headers, int[] czo_ids,Timestamp start, Timestamp end)
	{
	    IMOPositions=IPs;
	    Headers=headers;
	    Start_time=start;
	    End_time=end;

	    for(int id: czo_ids)
	    {
		CZO_IDs.add(id);
	    }

	    synchronized (VTLocationLoader.this)
	    {
		VTLocationLoader.this.ThreadCounter++;
	    }
	}

	public void run()
	{


	    Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

	    try
	    {
		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}					

		while(true)
		{
		    long[] Positions;

		    synchronized(IMOPositions)
		    {
			if (!IMOPositions.isEmpty())
			{
			    Positions=IMOPositions.remove(0);
			}
			else
			{
			    break;
			}						
		    }

		    try {

			ArrayList<Event> DerivedEventList=new ArrayList<Event>();

			VTLocationRow[] LR=VTLocationLoader.this.ReadHistoryRow(Positions,Headers);
			//Sort locations by record time
			Arrays.sort(LR);

			///////////////////////////
			//Get Vessel Database ID
			///////////////////////////	

			long Ves_id;

			CallableStatement objGetVesselID=null;			    		    

			objGetVesselID = DBConn.prepareCall(GetVesID);
			objGetVesselID.registerOutParameter(1, OracleTypes.DECIMAL);
			objGetVesselID.setLong(2,LR[0].shipId );
			objGetVesselID.setInt(3,LR[0].sImo );
			objGetVesselID.setString(4, LR[0].sName);
			objGetVesselID.setLong(5, LR[0].sMmsi);
			objGetVesselID.setString(6, LR[0].sCallsign);
			objGetVesselID.setInt(7, LR[0].sShiptype );
			if (LR[0].sLength!=null)
			{
			    objGetVesselID.setDouble(8, LR[0].sLength);
			}
			else
			{
			    objGetVesselID.setNull(8, OracleTypes.DECIMAL);
			}

			if (LR[0].sWidth!=null)
			{
			    objGetVesselID.setDouble(9, LR[0].sWidth );
			}
			else
			{
			    objGetVesselID.setNull(9, OracleTypes.DECIMAL);
			}


			objGetVesselID.execute();						
			Ves_id = objGetVesselID.getLong(1);

			objGetVesselID.close();

			if (Ves_id==0)
			{
			    VTLocationLogger.debug("Vessel:  Skip unknown vessel IMO: "+ LR[0].sImo + " ship ID:"+LR[0].shipId);
			    continue;
			}

			try{

			    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call vessel_location_purge_pkg.reset_vlp_proc(?)}");
			    callableClearSessionSM.setLong(1, Ves_id);
			    callableClearSessionSM.execute();
			    callableClearSessionSM.close();					

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn("SQLException", e);					
			}
			
			///////////////////////////
			//Get Vessel Product Type
			///////////////////////////					

			PreparedStatement objGetVesselType = DBConn.prepareStatement(GetVesselProductType);
			objGetVesselType.setLong(1, Ves_id);
			ResultSet objVesselTypeResult = objGetVesselType.executeQuery();

			String VesselProductType=null;

			if (objVesselTypeResult.next())
			{
			    VesselProductType=PatRootType.getInstance(objVesselTypeResult.getInt("pat_id")).getRootType();
			    objVesselTypeResult.close();
			    objGetVesselType.close();

			}
			else
			{
			    VTLocationLogger.debug(
				    "Vessel:  Skip vessel: "+ LR[0].sImo+" ship ID:"+LR[0].shipId + ", because of unknown vessel type!");
			    objVesselTypeResult.close();
			    objGetVesselType.close();
			    continue;
			}



			VTLocationLogger.debug(
				"Vessel location event: start processing historical locations data for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);


			/////////////////////////////////////////
			//Get the last location record time 每 must be open events
			/////////////////////////////////////////	
			VTLocationLogger.debug(
				"Vessel:  start retrieving latest events for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			CallableStatement objStatement = DBConn.prepareCall("{call vessel_load_pkg.get_lat_location_proc(?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			objStatement.setLong(1, Ves_id);
			objStatement.registerOutParameter(2, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(3, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(4, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(5, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(6, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(7, OracleTypes.VARCHAR);						
			objStatement.registerOutParameter(8, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(9, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(10, OracleTypes.FLOAT);
			objStatement.registerOutParameter(11, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(12, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(13, OracleTypes.FLOAT);
			objStatement.execute();


			long last_point_id=objStatement.getLong(2);
			Double pSpeed=objStatement.getDouble(3);
			if (objStatement.wasNull())
			{
			    pSpeed=null;
			}

			Double pLong=objStatement.getDouble(4);
			Double pLat=objStatement.getDouble(5);

			boolean IsSpeedChange=false;
			String Temp_SpeedChange=objStatement.getString(6);
			if (Temp_SpeedChange!=null && (Temp_SpeedChange.equals("Y")))
			{
			    IsSpeedChange=true;
			}

			boolean IsDud=false;
			String Temp_Dud=objStatement.getString(7);
			if (Temp_Dud!=null && (Temp_Dud.equals("Y")))
			{
			    IsDud=true;
			}

			Timestamp Previous_record_time=objStatement.getTimestamp(8);
			Timestamp ETA=objStatement.getTimestamp(9);
			if (objStatement.wasNull())
			{
			    ETA=null;
			}

			Double Heading=objStatement.getDouble(10);
			if (objStatement.wasNull())
			{
			    Heading=null;
			}

			Integer Dit_status_id=objStatement.getInt(11);
			if (objStatement.wasNull())
			{
			    Dit_status_id=null;
			}

			String Destination=objStatement.getString(12);
			Double Draught=objStatement.getDouble(13);
			if (objStatement.wasNull())
			{
			    Draught=null;
			}						


			DBLocation LastDBLocation=new DBLocation(last_point_id, Previous_record_time, pSpeed,
				pLat, pLong, IsSpeedChange, IsDud,
				ETA, Heading, Dit_status_id,
				Destination, Draught);

			objStatement.close();

			//////////////////////////////
			//Cache points and events

			//Database location cache
			ArrayList<DBLocation> DBLs=new ArrayList<DBLocation>();
			ArrayList<Event> DBEs=new ArrayList<Event>();
			int LogicType=0; 
			//1 for all shore-based locations
			//2 for all satellite locations
			//3 for half satellite half shore-based locations
			//4 for no location in db before

			if (LastDBLocation.locationId!=-1 )
			{
			    if (Start_time.after(LastDBLocation.recordTime))
			    {
				LogicType=1;
				//(1)events with last db point as exit

				PreparedStatement objGetEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				objGetEventsByLastPoints.setLong(1, LastDBLocation.locationId);
				ResultSet ObjEvents=objGetEventsByLastPoints.executeQuery();
				ObjEvents.setFetchSize(100);
				while(ObjEvents.next())
				{
				    long event_id=ObjEvents.getInt("id");
				    int czo_id=ObjEvents.getInt("czo_id");
				    if (!CZO_IDs.contains(czo_id))
				    {
					continue;
				    }
				    long entry_point_id=ObjEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjEvents.getTimestamp("entry_time");
				    long out_point_id=ObjEvents.getLong("out_point_id");
				    Timestamp out_time=ObjEvents.getTimestamp("entry_time");
				    boolean is_close=(ObjEvents.getString("is_closed")=="N") ? false : true;

				    Event thisEvent=new Event(event_id,czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);

				}	
				ObjEvents.close();
				objGetEventsByLastPoints.close();

			    }
			    else if (!End_time.after(LastDBLocation.recordTime))
			    {
				LogicType=2;

				///////////////////////////////
				//Remove all inside events for changed polygons
				///////////////////////////////

				for (int czo_id:CZO_IDs)
				{
				    CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc);
				    objDelHisEvents.setLong(1,Ves_id );
				    objDelHisEvents.setInt(2,czo_id );
				    objDelHisEvents.setTimestamp(3, Start_time);
				    objDelHisEvents.setTimestamp(4, End_time);
				    objDelHisEvents.execute();						
				    objDelHisEvents.close();
				}

				//////////////////////////////////////////////////////////////////////////
				//All the locations in file are satellite points, need to cache
				//(1) from 2 db before points until 2 db after points

				PreparedStatement objGet2BeforeUntil2AfterLocations=DBConn.prepareStatement(Get2BeforeUntil2AfterLocations);
				objGet2BeforeUntil2AfterLocations.setLong(1, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(2, Start_time );
				objGet2BeforeUntil2AfterLocations.setLong(3, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(4, End_time );
				objGet2BeforeUntil2AfterLocations.setLong(5, Ves_id );
				objGet2BeforeUntil2AfterLocations.setTimestamp(6, End_time );
				objGet2BeforeUntil2AfterLocations.setTimestamp(7, Start_time );

				ResultSet PointsOf2BeforeUntil2After=objGet2BeforeUntil2AfterLocations.executeQuery();
				PointsOf2BeforeUntil2After.setFetchSize(1000);
				while(PointsOf2BeforeUntil2After.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntil2After.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntil2After.getDouble(2);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntil2After.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntil2After.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntil2After.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntil2After.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntil2After.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntil2After.getTimestamp(8);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntil2After.getDouble(9);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntil2After.getInt(10);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntil2After.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntil2After.getDouble(12);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught,VesselDataSource.getInstance("VesselTracker").getID());
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntil2After.close();
				objGet2BeforeUntil2AfterLocations.close();
				java.util.Collections.sort(DBLs);

				////////////////////////////////////////////////////////////////////////////
				//(2) all events with exit point later than the first point and entry point earlier than the last file point

				PreparedStatement objGetAllOverlappingEvents=DBConn.prepareStatement(GetAllOverlappingEvents);
				objGetAllOverlappingEvents.setLong(1, Ves_id);
				objGetAllOverlappingEvents.setTimestamp(2, End_time );
				objGetAllOverlappingEvents.setTimestamp(3, Start_time );

				ResultSet OverlappingEvents=objGetAllOverlappingEvents.executeQuery();
				OverlappingEvents.setFetchSize(100);
				while(OverlappingEvents.next())
				{
				    long event_id=OverlappingEvents.getLong("id");
				    int zone_czo_id=OverlappingEvents.getInt("czo_id");
				    if (!CZO_IDs.contains(zone_czo_id))
				    {
					continue;
				    }
				    long entry_point_id=OverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=OverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=OverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=OverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(OverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);
				}
				OverlappingEvents.close();
				objGetAllOverlappingEvents.close();

				////////////////////////////////////////////////////////////////////////////
				//(3) events with before point as exit and events with after point as entry

				////////////////////////////////
				//Get all events which before point is related as exit point
				/////////////////////////////////							

				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(Start_time))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				//Finding after point
				DBLocation AfterPoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.after(End_time))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				}

				//Adjust events
				ArrayList<Event> tempDBEs=new ArrayList<Event>();
				for (Event thisevent : DBEs)
				{
				    if (thisevent.EntryTime.before(Start_time) && thisevent.OutTime.after(End_time))
				    {
					Event AfterEvent= new Event(0, thisevent.Czo_id, AfterPoint.locationId, thisevent.CurrentpointID, thisevent.IsCloseEvent, AfterPoint.recordTime, thisevent.OutTime);
					tempDBEs.add(AfterEvent);
					DerivedEventList.add(AfterEvent);

					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					thisevent.IsCloseEvent=true;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				    else if (thisevent.EntryTime.before(Start_time) && !thisevent.OutTime.before(Start_time))
				    {
					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				    else  if (thisevent.OutTime.after(End_time) && !thisevent.EntryTime.after(End_time) && !thisevent.EntryTime.before(Start_time))
				    {
					thisevent.EntrypointID=AfterPoint.locationId;
					thisevent.EntryTime=AfterPoint.recordTime;
					if (!DerivedEventList.contains(thisevent))
					{
					    DerivedEventList.add(thisevent);
					}
				    }
				}

				DBEs.addAll(tempDBEs);

				if (BeforePoint!=null)
				{


				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					if (!CZO_IDs.contains(zone_czo_id))
					{
					    continue;
					}
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}

				/////////////////////////////////
				//Get all events which after point is related as entry point
				/////////////////////////////////


				if (AfterPoint!=null)
				{


				    PreparedStatement objGetAfterPointEventsByLastPoints=DBConn.prepareStatement(GetEventsByEntryPoint);
				    objGetAfterPointEventsByLastPoints.setLong(1, AfterPoint.locationId);
				    ResultSet ObjAfterPointEvents=objGetAfterPointEventsByLastPoints.executeQuery();
				    ObjAfterPointEvents.setFetchSize(100);
				    while(ObjAfterPointEvents.next())
				    {
					long event_id=ObjAfterPointEvents.getLong("id");
					int zone_czo_id=ObjAfterPointEvents.getInt("czo_id");
					if (!CZO_IDs.contains(zone_czo_id))
					{
					    continue;
					}
					long entry_point_id=ObjAfterPointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjAfterPointEvents.getTimestamp("entry_time");
					long out_point_id=ObjAfterPointEvents.getLong("out_point_id");
					Timestamp out_time=ObjAfterPointEvents.getTimestamp("out_time");
					boolean is_close=(ObjAfterPointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjAfterPointEvents.close();
				    objGetAfterPointEventsByLastPoints.close();
				}




			    }
			    else if (!Start_time.after(LastDBLocation.recordTime) && End_time.after(LastDBLocation.recordTime))
			    {
				LogicType=3;

				///////////////////////////////
				//Remove all events for changed polygons
				///////////////////////////////

				for (int czo_id:CZO_IDs)
				{
				    CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc);
				    objDelHisEvents.setLong(1,Ves_id );
				    objDelHisEvents.setInt(2,czo_id );
				    objDelHisEvents.setTimestamp(3, Start_time);
				    objDelHisEvents.setTimestamp(4, LastDBLocation.recordTime);
				    objDelHisEvents.execute();						
				    objDelHisEvents.close();
				}

				//there are some satellite points in files, need to cache
				//(1) from 2 db before points until the last db point.
				PreparedStatement objGet2BeforeUntilLastLocation=DBConn.prepareStatement(Get2BeforeUntilLastLocation);
				objGet2BeforeUntilLastLocation.setLong(1, Ves_id);
				objGet2BeforeUntilLastLocation.setTimestamp(2, Start_time );
				objGet2BeforeUntilLastLocation.setLong(3, Ves_id );
				objGet2BeforeUntilLastLocation.setTimestamp(4, Start_time );

				ResultSet PointsOf2BeforeUntilLast=objGet2BeforeUntilLastLocation.executeQuery();
				PointsOf2BeforeUntilLast.setFetchSize(1000);
				while(PointsOf2BeforeUntilLast.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntilLast.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntilLast.getDouble(2);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntilLast.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntilLast.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntilLast.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntilLast.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntilLast.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntilLast.getTimestamp(8);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntilLast.getDouble(9);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntilLast.getInt(10);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntilLast.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntilLast.getDouble(12);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught,VesselDataSource.getInstance("VesselTracker").getID());
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntilLast.close();
				objGet2BeforeUntilLastLocation.close();

				java.util.Collections.sort(DBLs);							


				//(2) all events with exit point later than the first point in file

				PreparedStatement objGetBeforeOverlappingEvents=DBConn.prepareStatement(GetBeforeOverlappingEvents);
				objGetBeforeOverlappingEvents.setLong(1, Ves_id);
				objGetBeforeOverlappingEvents.setTimestamp(2, Start_time );

				ResultSet BeforeOverlappingEvents=objGetBeforeOverlappingEvents.executeQuery();
				BeforeOverlappingEvents.setFetchSize(100);
				while(BeforeOverlappingEvents.next())
				{
				    long event_id=BeforeOverlappingEvents.getLong("id");
				    int zone_czo_id=BeforeOverlappingEvents.getInt("czo_id");
				    if (!CZO_IDs.contains(zone_czo_id))
				    {
					continue;
				    }
				    long entry_point_id=BeforeOverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=BeforeOverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=BeforeOverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=BeforeOverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(BeforeOverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}

				BeforeOverlappingEvents.close();
				objGetBeforeOverlappingEvents.close();							

				//(3) events with before point as exit
				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(Start_time))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				if (BeforePoint!=null)
				{
				    //move current points of events to before points
				    for (Event thisevent : DBEs)
				    {
					thisevent.CurrentpointID=BeforePoint.locationId;
					thisevent.OutTime=BeforePoint.recordTime;
					thisevent.IsCloseEvent=false;
					DerivedEventList.add(thisevent);
				    }

				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					if (!CZO_IDs.contains(zone_czo_id))
					{
					    continue;
					}
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}


			    }

			}
			else
			{
			    LogicType=4;
			}

			//////////////////////////////
			long last_database_location_id=-1;

			HashMap<Integer, Event> PreviousZoneEvents =new HashMap<Integer, Event>();
			HashMap<Integer, Event> PostZoneEvents =new HashMap<Integer, Event>();
			DBLocation BeforePoint = null;
			DBLocation AfterPoint = null;

			int commitcounter=0;
			
			switch (LogicType)
			{
			case 1: //1 for all shore-based locations

			{
			    for (Event thisEvent : DBEs)
			    {
				PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
			    }

			    //Check if the last db location needs to be set to speed point
			    if ((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1) )
			    {
				if (LR[0].pSpeed!=null && LR[0].pSpeed>=1)
				{
				    //update last DB location as speed change point.
				    LastDBLocation.IsSpeedChange=true;
				    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
				}
			    }


			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType,CZO_IDs);

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;							

				if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if (i==0)
				    {
					if (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    IsDud=IsDudPoint(LastDBLocation, thisLOCRow);
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], thisLOCRow);
				}


				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}

			break;
			case 2:	//2 for all satellite locations

			{
			    ArrayList<DBLocation> TempDBRows=new ArrayList<DBLocation>();

			    for (DBLocation thisDBLocation : DBLs)
			    {
				if (thisDBLocation.recordTime.after(Start_time) && thisDBLocation.recordTime.before(End_time) )
				{
				    boolean existedpoint=false;

				    for (VTLocationRow thisVTLR :LR )
				    {
					if (thisDBLocation.recordTime.equals(thisVTLR))
					{
					    existedpoint=true;
					    break;
					}
				    }

				    if (!existedpoint)
				    {
					//need to recalculate events
					TempDBRows.add(thisDBLocation);
				    }
				}
			    }

			    //merge db and file locations
			    int point_number = LR.length+TempDBRows.size();
			    VTLocationRow[] tempLR=LR;
			    LR = new VTLocationRow[point_number];
			    System.arraycopy(tempLR, 0, LR, 0, tempLR.length);

			    int x=tempLR.length;

			    for(DBLocation thisLocation : TempDBRows)
			    {
				LR[x]=new VTLocationRow(thisLocation.recordTime,thisLocation.pSpeed,
					thisLocation.pLat, thisLocation.pLong, 
					thisLocation.ETA, thisLocation.Heading, thisLocation.DIT_Status_ID,
					thisLocation.Destination, thisLocation.Draught,thisLocation.DIT_data_source_id);
				x++;
			    }

			    //sorting
			    Arrays.sort(LR);

			    //The first before point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.before(Start_time))
				{
				    BeforePoint=thisLocation;
				}
				else
				{
				    break;
				}
			    }

			    //collect PreviousZoneEvents    
			    if (BeforePoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.CurrentpointID==BeforePoint.locationId)
				    {
					PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }

			    //Finding after point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.after(End_time))
				{
				    AfterPoint=thisLocation;
				    break;
				}
			    }

			    //collect PostZoneEvents
			    if (AfterPoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.EntrypointID==AfterPoint.locationId)
				    {
					PostZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }

			    for(int i=0;i<LR.length;i++)
			    {
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				long database_location_id=0;


				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType,CZO_IDs);

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////

				//Determine speedchange flag
				IsSpeedChange=false;
				IsDud=false;

				if (LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if (i==0)
				    {
					if (BeforePoint!=null && BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
					else if (AfterPoint!=null && AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
					else if (AfterPoint!=null && AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    if (BeforePoint!=null)
				    {
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], LR[i]);
				}

				boolean HasExisted=false;

				//Update existing point if possible
				for (int a=0;a<DBLs.size();a++)
				{
				    DBLocation thisLocation=DBLs.get(a);
				    if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
				    {
					if ((LR[i].vEta!=null && !LR[i].vEta.equals(thisLocation.ETA))|| (LR[i].vEta==null && thisLocation.ETA!=null)                                                                      
						|| (LR[i].pHdg!=null && !LR[i].pHdg.equals(thisLocation.Heading)) || (LR[i].pHdg==null && thisLocation.Heading!=null)                                             
						|| (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(thisLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && thisLocation.DIT_Status_ID!=null)      
						|| (LR[i].vDest != null && !LR[i].vDest.equals(thisLocation.Destination)) || (LR[i].vDest == null && thisLocation.Destination!=null)                                                                                                           
						|| (LR[i].vDraught!=null && !LR[i].vDraught.equals(thisLocation.Draught)) || (LR[i].vDraught==null && thisLocation.Draught!=null )
						|| (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(thisLocation.pSpeed)) ||(LR[i].pSpeed==null && thisLocation.pSpeed!=null)
						|| (thisLocation.IsSpeedChange!=IsSpeedChange)
						|| (thisLocation.IsDud!=IsDud)
						)                                                                                                                                                                   
					{                                                                                                                                                                       
					    PopulateLocation(DBConn,Ves_id,LR[i],thisLocation.IsSpeedChange,thisLocation.IsDud);
					    commitcounter++;
					    if (commitcounter>3000)
					    {
						DBConn.commit();
						commitcounter=0;
					    }
					    
					    thisLocation.Destination=LR[i].vDest;
					    thisLocation.ETA=LR[i].vEta;
					    thisLocation.Heading=LR[i].pHdg;
					    thisLocation.DIT_Status_ID=LR[i].dit_status_id;
					    thisLocation.Draught=LR[i].vDraught;
					    thisLocation.pSpeed=LR[i].pSpeed;
					    VTLocationLogger.debug("Update location to:"+ LR[i].toString());
					}
					database_location_id=thisLocation.locationId;
					HasExisted=true;
					break;
				    }
				}

				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				if (!HasExisted)				    
				{
				    database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				    commitcounter++;
				    if (commitcounter>3000)
				    {
					DBConn.commit();
					commitcounter=0;
				    }
				    
				    if (database_location_id==-1)
				    {
					VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because of database exception");
					continue;
				    }

				    DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				    DBLs.add(newLocation);
				    java.util.Collections.sort(DBLs);
				}

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,LR[i].pTime,LR[i].pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }

			    //Merger with PostZoneEvents
			    Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
			    while (it.hasNext())
			    {
				Map.Entry<Integer, Event> thisEntry=it.next();
				int Zone_czo_id = thisEntry.getKey();
				Event PreviousZoneEvent=thisEntry.getValue();

				if (PostZoneEvents.containsKey(Zone_czo_id))
				{


				    Event PostZoneEvent=PostZoneEvents.get(Zone_czo_id);

				    PreviousZoneEvent.CurrentpointID=PostZoneEvent.CurrentpointID;
				    PreviousZoneEvent.OutTime=PostZoneEvent.OutTime;
				    PreviousZoneEvent.IsCloseEvent=PostZoneEvent.IsCloseEvent;

				    if (!DerivedEventList.contains(PreviousZoneEvent))
				    {
					DerivedEventList.add(PreviousZoneEvent);
				    }

				    //delete this postzoneevent
				    CallableStatement objDelHisEvents = DBConn.prepareCall(delete_his_event_proc_with_id);
				    objDelHisEvents.setLong(1,PostZoneEvent.EventID );
				    objDelHisEvents.execute();						
				    objDelHisEvents.close();


				    DBEs.remove(PostZoneEvent);
				    PostZoneEvents.remove(Zone_czo_id);


				    if (DerivedEventList.contains(PostZoneEvent))
				    {
					DerivedEventList.remove(PostZoneEvent);
				    }
				}
				else
				{
				    PreviousZoneEvent.IsCloseEvent=true;

				    if (!DerivedEventList.contains(PreviousZoneEvent))
				    {
					DerivedEventList.add(PreviousZoneEvent);
				    }
				}
			    }
			}

			break;
			case 3:	//3 for half satellite half shore-based locations

			{
			    ArrayList<DBLocation> TempDBRows=new ArrayList<DBLocation>();

			    for (DBLocation thisDBLocation : DBLs)
			    {
				if (thisDBLocation.recordTime.after(Start_time))
				{
				    boolean existedpoint=false;

				    for (VTLocationRow thisVTLR :LR )
				    {
					if (thisDBLocation.recordTime.equals(thisVTLR))
					{
					    existedpoint=true;
					    break;
					}
				    }

				    if (!existedpoint)
				    {
					//need to recalculate events
					TempDBRows.add(thisDBLocation);
				    }
				}
			    }

			    //merge db and file locations
			    int point_number = LR.length+TempDBRows.size();
			    VTLocationRow[] tempLR=LR;
			    LR = new VTLocationRow[point_number];
			    System.arraycopy(tempLR, 0, LR, 0, tempLR.length);

			    int x=tempLR.length;

			    for(DBLocation thisLocation : TempDBRows)
			    {
				LR[x]=new VTLocationRow(thisLocation.recordTime,thisLocation.pSpeed,
					thisLocation.pLat, thisLocation.pLong, 
					thisLocation.ETA, thisLocation.Heading, thisLocation.DIT_Status_ID,
					thisLocation.Destination, thisLocation.Draught,thisLocation.DIT_data_source_id);
				x++;
			    }

			    //sorting
			    Arrays.sort(LR);

			    //The first before point				
			    for (DBLocation thisLocation: DBLs)
			    {
				if (thisLocation.recordTime.before(Start_time))
				{
				    BeforePoint=thisLocation;
				}
				else
				{
				    break;
				}
			    }

			    //collect PreviousZoneEvents    
			    if (BeforePoint!=null)
			    {
				for (Event thisEvent : DBEs)
				{
				    if (thisEvent.CurrentpointID==BeforePoint.locationId)
				    {
					PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}
			    }


			    for(int i=0;i<LR.length;i++)
			    {
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				long database_location_id=0;


				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType,CZO_IDs);

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////

				//Determine speedchange flag
				IsSpeedChange=false;
				IsDud=false;

				if (LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if (i==0)
				    {
					if (BeforePoint!=null && BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  

				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}

				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    if (BeforePoint!=null)
				    {
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], LR[i]);
				}

				boolean HasExisted=false;

				//Update existing point if possible
				for (int a=0;a<DBLs.size();a++)
				{
				    DBLocation thisLocation=DBLs.get(a);
				    if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
				    {
					if ((LR[i].vEta!=null && !LR[i].vEta.equals(thisLocation.ETA))|| (LR[i].vEta==null && thisLocation.ETA!=null)                                                                      
						|| (LR[i].pHdg!=null && !LR[i].pHdg.equals(thisLocation.Heading)) || (LR[i].pHdg==null && thisLocation.Heading!=null)                                             
						|| (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(thisLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && thisLocation.DIT_Status_ID!=null)      
						|| (LR[i].vDest != null && !LR[i].vDest.equals(thisLocation.Destination)) || (LR[i].vDest == null && thisLocation.Destination!=null)                                                                                                           
						|| (LR[i].vDraught!=null && !LR[i].vDraught.equals(thisLocation.Draught)) || (LR[i].vDraught==null && thisLocation.Draught!=null )
						|| (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(thisLocation.pSpeed)) ||(LR[i].pSpeed==null && thisLocation.pSpeed!=null)
						|| (thisLocation.IsSpeedChange!=IsSpeedChange)
						|| (thisLocation.IsDud!=IsDud)
						)                                                                                                                                                                   
					{                                                                                                                                                                       
					    PopulateLocation(DBConn,Ves_id,LR[i],thisLocation.IsSpeedChange,thisLocation.IsDud);
					    commitcounter++;
					    if (commitcounter>3000)
					    {
						DBConn.commit();
						commitcounter=0;
					    }
					    
					    thisLocation.Destination=LR[i].vDest;
					    thisLocation.ETA=LR[i].vEta;
					    thisLocation.Heading=LR[i].pHdg;
					    thisLocation.DIT_Status_ID=LR[i].dit_status_id;
					    thisLocation.Draught=LR[i].vDraught;
					    thisLocation.pSpeed=LR[i].pSpeed;
					    VTLocationLogger.debug("Update location to:"+ LR[i].toString());
					}
					database_location_id=thisLocation.locationId;
					HasExisted=true;
					break;
				    }
				}

				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				if (!HasExisted)				    
				{
				    database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				    commitcounter++;
				    if (commitcounter>3000)
				    {
					DBConn.commit();
					commitcounter=0;
				    }

				    if (database_location_id==-1)
				    {
					VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because of database exception");
					continue;
				    }

				    DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				    DBLs.add(newLocation);
				    java.util.Collections.sort(DBLs);
				}

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,LR[i].pTime,LR[i].pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}


			break;
			case 4:	//4 for no location in db before

			{
			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType,CZO_IDs);


				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;

				if ((i==0) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[1].pSpeed!=null && LR[1].pSpeed>=1) )
				    {
					IsSpeedChange=true;
				    }
				}
				else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}
				else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i>0)
				{
				    IsDud=IsDudPoint(LR[i-1],LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);
				commitcounter++;
				if (commitcounter>3000)
				{
				    DBConn.commit();
				    commitcounter=0;
				}
				
				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			}
			break;

			}

			/////////////////////////////////
			//Populate Derived Events
			/////////////////////////////////
			for (Event aEvent:DerivedEventList)
			{
			    PopulateEvent(DBConn,Ves_id,aEvent);
			}

			/////////////////////////////////
			//Update flag of last location
			/////////////////////////////////
			if (last_database_location_id!=-1)
			{
			    UpdateLastLocationFlag(DBConn, Ves_id,last_database_location_id);
			}


			/////////////////////////////////////////
			//Update modify timestamp
			/////////////////////////////////////////
			CallableStatement csSetTime = DBConn.prepareCall("begin vessel_load_pkg.set_change_timestamp_proc(?);  end;");

			try {
			    csSetTime.setLong(1, Ves_id);
			    csSetTime.execute();	

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn(
				    "Failed to change Physical Asset modify_date for I"+ LR[0].sImo+" ship ID:"+LR[0].shipId , e);					
			}		


			DBConn.commit();
			VTLocationLogger.debug(
				"Vessel:  loaded current location event data successfully for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

		    }
		    catch (Exception e) {

			VTLocationLogger.warn(
				"Vessel:  loading current location event data failed" , e);
			e.printStackTrace();
		    }
		}


		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}

	    }
	    finally
	    {
		try {
		    DBConn.close();
		} catch (SQLException e) {
		    VTLocationLogger.warn("SQLException", e);	
		}					

		synchronized (VTLocationLoader.this)
		{
		    VTLocationLoader.this.ThreadCounter--;

		    if (VTLocationLoader.this.ThreadCounter==0)
		    {			
			VTLocationLoader.this.notify();
		    }
		}
	    }
	}
    }

    private class OutageRefill implements Runnable
    {
	private ArrayList<long[]> IMOPositions;
	private String[] Headers;

	public OutageRefill(ArrayList<long[]> IPs, String[] headers)
	{
	    IMOPositions=IPs;
	    Headers=headers;

	    synchronized (VTLocationLoader.this)
	    {
		VTLocationLoader.this.ThreadCounter++;
	    }
	}

	public void run()
	{

	    Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

	    try
	    {
		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}					

		while(true)
		{
		    long[] Positions;

		    synchronized(IMOPositions)
		    {
			if (!IMOPositions.isEmpty())
			{
			    Positions=IMOPositions.remove(0);
			}
			else
			{
			    break;
			}						
		    }

		    try {						

			VTLocationRow[] LR=VTLocationLoader.this.ReadHistoryRow(Positions,Headers);
			//Sort locations by record time
			Arrays.sort(LR);

			///////////////////////////
			//Get Vessel Database ID
			///////////////////////////	

			long Ves_id;

			CallableStatement objGetVesselID=null;

			objGetVesselID = DBConn.prepareCall(GetVesID);
			objGetVesselID.registerOutParameter(1, OracleTypes.DECIMAL);
			objGetVesselID.setLong(2,LR[0].shipId );
			objGetVesselID.setInt(3,LR[0].sImo );
			objGetVesselID.setString(4, LR[0].sName);
			objGetVesselID.setLong(5, LR[0].sMmsi);
			objGetVesselID.setString(6, LR[0].sCallsign);
			objGetVesselID.setInt(7, LR[0].sShiptype );
			if (LR[0].sLength!=null)
			{
			    objGetVesselID.setDouble(8, LR[0].sLength);
			}
			else
			{
			    objGetVesselID.setNull(8, OracleTypes.DECIMAL);
			}

			if (LR[0].sWidth!=null)
			{
			    objGetVesselID.setDouble(9, LR[0].sWidth );
			}
			else
			{
			    objGetVesselID.setNull(9, OracleTypes.DECIMAL);
			}


			objGetVesselID.execute();						
			Ves_id = objGetVesselID.getLong(1);

			objGetVesselID.close();

			if (Ves_id==0)
			{
			    VTLocationLogger.debug("Vessel:  Skip unknown vessel IMO: "+ LR[0].sImo + " ship ID:"+LR[0].shipId);
			    continue;
			}
			
			try{

			    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call vessel_location_purge_pkg.reset_vlp_proc(?)}");
			    callableClearSessionSM.setLong(1, Ves_id);
			    callableClearSessionSM.execute();
			    callableClearSessionSM.close();					

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn("SQLException", e);					
			}


			///////////////////////////
			//Get Vessel Product Type
			///////////////////////////					

			PreparedStatement objGetVesselType = DBConn.prepareStatement(GetVesselProductType);
			objGetVesselType.setLong(1, Ves_id);
			ResultSet objVesselTypeResult = objGetVesselType.executeQuery();

			String VesselProductType=null;

			if (objVesselTypeResult.next())
			{
			    VesselProductType=PatRootType.getInstance(objVesselTypeResult.getInt("pat_id")).getRootType();
			    objVesselTypeResult.close();
			    objGetVesselType.close();

			}
			else
			{
			    VTLocationLogger.debug(
				    "Vessel:  Skip vessel: "+ LR[0].sImo+" ship ID:"+LR[0].shipId + ", because of unknown vessel type!");
			    objVesselTypeResult.close();
			    objGetVesselType.close();
			    continue;
			}



			VTLocationLogger.debug(
				"Vessel location event: start processing historical locations data for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			/////////////////////////////////////////
			//Get the last location record time 每 must be open events
			/////////////////////////////////////////	
			VTLocationLogger.debug(
				"Vessel:  start retrieving latest events for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			CallableStatement objStatement = DBConn.prepareCall("{call vessel_load_pkg.get_lat_location_proc(?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			objStatement.setLong(1, Ves_id);
			objStatement.registerOutParameter(2, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(3, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(4, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(5, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(6, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(7, OracleTypes.VARCHAR);						
			objStatement.registerOutParameter(8, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(9, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(10, OracleTypes.FLOAT);
			objStatement.registerOutParameter(11, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(12, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(13, OracleTypes.FLOAT);
			objStatement.execute();


			long last_point_id=objStatement.getLong(2);
			Double pSpeed=objStatement.getDouble(3);
			if (objStatement.wasNull())
			{
			    pSpeed=null;
			}

			Double pLong=objStatement.getDouble(4);
			Double pLat=objStatement.getDouble(5);

			boolean IsSpeedChange=false;
			String Temp_SpeedChange=objStatement.getString(6);
			if (Temp_SpeedChange!=null && (Temp_SpeedChange.equals("Y")))
			{
			    IsSpeedChange=true;
			}

			boolean IsDud=false;
			String Temp_Dud=objStatement.getString(7);
			if (Temp_Dud!=null && (Temp_Dud.equals("Y")))
			{
			    IsDud=true;
			}

			Timestamp Previous_record_time=objStatement.getTimestamp(8);
			Timestamp ETA=objStatement.getTimestamp(9);
			if (objStatement.wasNull())
			{
			    ETA=null;
			}

			Double Heading=objStatement.getDouble(10);
			if (objStatement.wasNull())
			{
			    Heading=null;
			}

			Integer Dit_status_id=objStatement.getInt(11);
			if (objStatement.wasNull())
			{
			    Dit_status_id=null;
			}

			String Destination=objStatement.getString(12);
			Double Draught=objStatement.getDouble(13);
			if (objStatement.wasNull())
			{
			    Draught=null;
			}						


			DBLocation LastDBLocation=new DBLocation(last_point_id, Previous_record_time, pSpeed,
				pLat, pLong, IsSpeedChange, IsDud,
				ETA, Heading, Dit_status_id,
				Destination, Draught);


			objStatement.close();

			//////////////////////////////
			//Cache points and events

			//Database location cache
			ArrayList<DBLocation> DBLs=new ArrayList<DBLocation>();
			ArrayList<Event> DBEs=new ArrayList<Event>();
			int LogicType=0; 
			//1 for all shore-based locations
			//2 for all satellite locations
			//3 for half satellite half shore-based locations
			//4 for no location in db before



			if (LastDBLocation.locationId!=-1 )
			{
			    //Existing vessel
			    if (LR.length==1 && LR[0].pTime.equals(LastDBLocation.recordTime))
			    {
				if ((LR[0].vEta!=null && !LR[0].vEta.equals(LastDBLocation.ETA))|| (LR[0].vEta==null && LastDBLocation.ETA!=null)
					|| (LR[0].pHdg!=null && !LR[0].pHdg.equals(LastDBLocation.Heading)) || (LR[0].pHdg==null && LastDBLocation.Heading!=null)
					|| (LR[0].dit_status_id!=null && !LR[0].dit_status_id.equals(LastDBLocation.DIT_Status_ID)) || (LR[0].dit_status_id==null && LastDBLocation.DIT_Status_ID!=null)
					|| (LR[0].vDest != null && !LR[0].vDest.equals(LastDBLocation.Destination)) || (LR[0].vDest == null && LastDBLocation.Destination!=null)
					|| (LR[0].vDraught!=null && !LR[0].vDraught.equals(LastDBLocation.Draught)) || (LR[0].vDraught==null && LastDBLocation.Draught!=null )
					|| (LR[0].pSpeed!=null && !LR[0].pSpeed.equals(LastDBLocation.pSpeed)) ||(LR[0].pSpeed==null && LastDBLocation.pSpeed!=null)
					)
				{
				    PopulateLocation(DBConn,Ves_id,LR[0],LastDBLocation.IsSpeedChange,LastDBLocation.IsDud);
				    VTLocationLogger.debug("Update location to:"+ LR[0].toString());
				}
				else
				{
				    VTLocationLogger.warn("Ignore location: "+ LR[0].toString()+" because location with same record_time has existed in database");							    
				}
				continue;
			    }
			    else if (!LR[0].pTime.before(LastDBLocation.recordTime))
			    {	
				LogicType=1;
				//All the locations in file are shore-based, just need to cache
				//(1)events with last db point as exit


				PreparedStatement objGetEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				objGetEventsByLastPoints.setLong(1, LastDBLocation.locationId);
				ResultSet ObjEvents=objGetEventsByLastPoints.executeQuery();

				ObjEvents.setFetchSize(100);

				while(ObjEvents.next())
				{
				    long event_id=ObjEvents.getInt("id");
				    int czo_id=ObjEvents.getInt("czo_id");
				    long entry_point_id=ObjEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjEvents.getTimestamp("entry_time");
				    long out_point_id=ObjEvents.getLong("out_point_id");
				    Timestamp out_time=ObjEvents.getTimestamp("entry_time");
				    boolean is_close=(ObjEvents.getString("is_closed")=="N") ? false : true;

				    Event thisEvent=new Event(event_id,czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);

				}	
				ObjEvents.close();
				objGetEventsByLastPoints.close();



			    }
			    else if (LR[LR.length-1].pTime.before(LastDBLocation.recordTime) )
			    {
				LogicType=2;
				//						//////////////////////////////////////////////////////////////////////////
				//All the locations in file are satellite points, need to cache
				//(1) from 2 db before points until 2 db after points
				Timestamp TheTimeStampOfFirstPoint=LR[0].pTime;
				Timestamp TheTimeStampOfLastPoint=LR[LR.length-1].pTime;


				PreparedStatement objGet2BeforeUntil2AfterLocations=DBConn.prepareStatement(Get2BeforeUntil2AfterLocations);
				objGet2BeforeUntil2AfterLocations.setLong(1, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(2, TheTimeStampOfFirstPoint );
				objGet2BeforeUntil2AfterLocations.setLong(3, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(4, TheTimeStampOfLastPoint );
				objGet2BeforeUntil2AfterLocations.setLong(5, Ves_id );
				objGet2BeforeUntil2AfterLocations.setTimestamp(6, TheTimeStampOfLastPoint );
				objGet2BeforeUntil2AfterLocations.setTimestamp(7, TheTimeStampOfFirstPoint );

				ResultSet PointsOf2BeforeUntil2After=objGet2BeforeUntil2AfterLocations.executeQuery();
				PointsOf2BeforeUntil2After.setFetchSize(1000);
				while(PointsOf2BeforeUntil2After.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntil2After.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntil2After.getDouble(2);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntil2After.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntil2After.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntil2After.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntil2After.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntil2After.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntil2After.getTimestamp(8);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntil2After.getDouble(9);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntil2After.getInt(10);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntil2After.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntil2After.getDouble(12);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught);
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntil2After.close();
				objGet2BeforeUntil2AfterLocations.close();


				java.util.Collections.sort(DBLs);

				//						//////////////////////////////////////////////////////////////////////////
				//(2) all events with exit point later than the first point and entry point earlier than the last file point

				PreparedStatement objGetAllOverlappingEvents=DBConn.prepareStatement(GetAllOverlappingEvents);
				objGetAllOverlappingEvents.setLong(1, Ves_id);
				objGetAllOverlappingEvents.setTimestamp(2, TheTimeStampOfLastPoint );
				objGetAllOverlappingEvents.setTimestamp(3, TheTimeStampOfFirstPoint );

				ResultSet OverlappingEvents=objGetAllOverlappingEvents.executeQuery();
				OverlappingEvents.setFetchSize(100);
				while(OverlappingEvents.next())
				{
				    long event_id=OverlappingEvents.getLong("id");
				    int zone_czo_id=OverlappingEvents.getInt("czo_id");
				    long entry_point_id=OverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=OverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=OverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=OverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(OverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);
				}
				OverlappingEvents.close();
				objGetAllOverlappingEvents.close();

				////////////////////////////////////////////////////////////////////////////
				//(3) events with before point as exit and events with after point as entry

				////////////////////////////////
				//Get all events which before point is related as exit point
				/////////////////////////////////							

				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(TheTimeStampOfFirstPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}


				if (BeforePoint!=null)
				{
				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}

				/////////////////////////////////
				//Get all events which after point is related as entry point
				/////////////////////////////////
				//Finding after point
				DBLocation AfterPoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.after(TheTimeStampOfLastPoint))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				}

				PreparedStatement objGetAfterPointEventsByLastPoints=DBConn.prepareStatement(GetEventsByEntryPoint);
				objGetAfterPointEventsByLastPoints.setLong(1, AfterPoint.locationId);
				ResultSet ObjAfterPointEvents=objGetAfterPointEventsByLastPoints.executeQuery();

				while(ObjAfterPointEvents.next())
				{
				    long event_id=ObjAfterPointEvents.getLong("id");
				    int zone_czo_id=ObjAfterPointEvents.getInt("czo_id");
				    long entry_point_id=ObjAfterPointEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjAfterPointEvents.getTimestamp("entry_time");
				    long out_point_id=ObjAfterPointEvents.getLong("out_point_id");
				    Timestamp out_time=ObjAfterPointEvents.getTimestamp("out_time");
				    boolean is_close=(ObjAfterPointEvents.getString("is_closed").equals("N") ? false : true);

				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}	
				ObjAfterPointEvents.close();
				objGetAfterPointEventsByLastPoints.close();

			    }
			    else
			    {
				LogicType=3;
				//there are some satellite points in files, need to cache
				//(1) from 2 db before points until the last db point.


				Timestamp TheTimeStampOfFirstPoint=LR[0].pTime;
				PreparedStatement objGet2BeforeUntilLastLocation=DBConn.prepareStatement(Get2BeforeUntilLastLocation);
				objGet2BeforeUntilLastLocation.setLong(1, Ves_id);
				objGet2BeforeUntilLastLocation.setTimestamp(2, TheTimeStampOfFirstPoint );
				objGet2BeforeUntilLastLocation.setLong(3, Ves_id );
				objGet2BeforeUntilLastLocation.setTimestamp(4, TheTimeStampOfFirstPoint );

				ResultSet PointsOf2BeforeUntilLast=objGet2BeforeUntilLastLocation.executeQuery();
				PointsOf2BeforeUntilLast.setFetchSize(1000);
				while(PointsOf2BeforeUntilLast.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntilLast.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntilLast.getDouble(2);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntilLast.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntilLast.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntilLast.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntilLast.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntilLast.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntilLast.getTimestamp(8);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntilLast.getDouble(9);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntilLast.getInt(10);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntilLast.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntilLast.getDouble(12);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught);
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntilLast.close();
				objGet2BeforeUntilLastLocation.close();

				java.util.Collections.sort(DBLs);							


				//(2) all events with exit point later than the first point in file


				PreparedStatement objGetBeforeOverlappingEvents=DBConn.prepareStatement(GetBeforeOverlappingEvents);
				objGetBeforeOverlappingEvents.setLong(1, Ves_id);
				objGetBeforeOverlappingEvents.setTimestamp(2, TheTimeStampOfFirstPoint );

				ResultSet BeforeOverlappingEvents=objGetBeforeOverlappingEvents.executeQuery();
				BeforeOverlappingEvents.setFetchSize(100);
				while(BeforeOverlappingEvents.next())
				{
				    long event_id=BeforeOverlappingEvents.getLong("id");
				    int zone_czo_id=BeforeOverlappingEvents.getInt("czo_id");
				    long entry_point_id=BeforeOverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=BeforeOverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=BeforeOverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=BeforeOverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(BeforeOverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}
				BeforeOverlappingEvents.close();
				objGetBeforeOverlappingEvents.close();							

				//(3) events with before point as exit
				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(TheTimeStampOfFirstPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				if (BeforePoint!=null)
				{

				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();


				}
			    }

			}
			else
			{
			    LogicType=4;
			}

			//////////////////////////////
			long last_database_location_id=-1;

			HashMap<Integer, Event> PreviousZoneEvents =new HashMap<Integer, Event>();
			ArrayList<Event> DerivedEventList=new ArrayList<Event>();

			switch (LogicType)
			{
			case 1: //1 for all shore-based locations



			    for (Event thisEvent : DBEs)
			    {
				PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
			    }

			    //Check if the last db location needs to be set to speed point
			    if ((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1) )
			    {
				if (LR[0].pSpeed!=null && LR[0].pSpeed>=1)
				{
				    //update last DB location as speed change point.
				    LastDBLocation.IsSpeedChange=true;
				    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
				}
			    }


			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
				    continue;
				}

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;							

				if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if (i==0)
				    {
					if (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    IsDud=IsDudPoint(LastDBLocation, thisLOCRow);
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], thisLOCRow);
				}


				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }



			    break;
			case 2:	//2 for all satellite locations

			    for(int i=0;i<LR.length;i++)
			    {



				///////////////////////////////////
				//find out before and after points
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				DBLocation BeforePoint = null;
				DBLocation AfterPoint = null;
				boolean HasExisted=false;
				DBLocation ExistingLocation=null;

				for (int a=0;a<DBLs.size();a++)
				{
				    DBLocation thisLocation=DBLs.get(a);
				    if (thisLocation.recordTime.before(TheTimeStampOfCurrentPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else if(thisLocation.recordTime.after(TheTimeStampOfCurrentPoint))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				    else if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
				    {
					ExistingLocation=thisLocation;
					HasExisted=true;
					break;
				    }

				}

				/////////////////////////////
				//If point with same record time exists, then ignore
				if (HasExisted)
				{

				    if ((LR[i].vEta!=null && !LR[i].vEta.equals(ExistingLocation.ETA))|| (LR[i].vEta==null && ExistingLocation.ETA!=null)                                                                      
					    || (LR[i].pHdg!=null && !LR[i].pHdg.equals(ExistingLocation.Heading)) || (LR[i].pHdg==null && ExistingLocation.Heading!=null)                                             
					    || (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(ExistingLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && ExistingLocation.DIT_Status_ID!=null)      
					    || (LR[i].vDest != null && !LR[i].vDest.equals(ExistingLocation.Destination)) || (LR[i].vDest == null && ExistingLocation.Destination!=null)                                                                                                           
					    || (LR[i].vDraught!=null && !LR[i].vDraught.equals(ExistingLocation.Draught)) || (LR[i].vDraught==null && ExistingLocation.Draught!=null )
					    || (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(ExistingLocation.pSpeed)) ||(LR[i].pSpeed==null && ExistingLocation.pSpeed!=null)
					    )                                                                                                                                                                   
				    {                                                                                                                                                                       
					PopulateLocation(DBConn,Ves_id,LR[i],ExistingLocation.IsSpeedChange,ExistingLocation.IsDud);
					ExistingLocation.Destination=LR[i].vDest;
					ExistingLocation.ETA=LR[i].vEta;
					ExistingLocation.Heading=LR[i].pHdg;
					ExistingLocation.DIT_Status_ID=LR[i].dit_status_id;
					ExistingLocation.Draught=LR[i].vDraught;
					ExistingLocation.pSpeed=LR[i].pSpeed;
					VTLocationLogger.debug("Update location to:"+ LR[i].toString());
				    }                                                                                                                                                                       
				    else                                                                                                                                                                    
				    {    
					VTLocationLogger.warn("Ignore location: "+ LR[i].toString()+" because location with same record_time has existed in database");
				    }
				    continue;
				}

				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);							

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because no polygon matchs");
				    continue;
				}

				////////////////////////////
				//Collecting same range events
				HashMap<Integer, Event> SameRangeZoneEvents=new HashMap<Integer, Event>();

				for (Event thisEvent:DBEs)
				{
				    if (thisEvent.EntryTime.before(TheTimeStampOfCurrentPoint) && thisEvent.OutTime.after(TheTimeStampOfCurrentPoint))
				    {
					SameRangeZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}

				////////////////////////////
				//Collecting before events
				HashMap<Integer, Event> BeforePointZoneEvents=new HashMap<Integer, Event>();

				if (BeforePoint!=null)
				{
				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.CurrentpointID==BeforePoint.locationId)
					{
					    BeforePointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }
				}

				////////////////////////////
				//Collecting after events
				HashMap<Integer, Event> AfterPointZoneEvents=new HashMap<Integer, Event>();
				if (AfterPoint!=null)
				{
				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.EntrypointID==AfterPoint.locationId)
					{
					    AfterPointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }
				}

				////////////////////////////
				//Calculate SpeedChange

				IsSpeedChange=false;
				if( LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1))
				    {
					IsSpeedChange=true;

					//////////////////////////////////////////////////
					//Populate location to database
					//////////////////////////////////////////////////
					int IndexOfNextLocation=DBLs.indexOf(BeforePoint)+1;

					DBLocation nextLocation=null;
					DBLocation nextnextLocation=null;
					if ((IndexOfNextLocation<(DBLs.size())))
					{
					    nextLocation=DBLs.get(IndexOfNextLocation);
					}

					if ((IndexOfNextLocation<(DBLs.size()-1)))
					{
					    nextnextLocation=DBLs.get(IndexOfNextLocation+1);
					}   

					//update IsSpeedChange for next DB location 
					if((nextLocation!=null) && 
						((nextLocation.pSpeed != null && nextLocation.pSpeed<1) && nextLocation.IsSpeedChange) &&
						(((nextnextLocation!=null) && (nextnextLocation.pSpeed != null && nextnextLocation.pSpeed<1)) || (nextnextLocation==null)))
					{
					    nextLocation.IsSpeedChange=false;
					    UpdateLocationMark(DBConn,nextLocation.locationId,"S","N");
					}

				    }
				    else if (((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)))
				    {
					IsSpeedChange=true;
					//////////////////////////////////////////////////
					//Populate location to database
					//////////////////////////////////////////////////
					int IndexOfBeforeLocation=DBLs.indexOf(AfterPoint)-1;

					DBLocation beforeLocation=null;
					DBLocation beforebeforeLocation=null;
					if (IndexOfBeforeLocation>=0)
					{
					    beforeLocation=DBLs.get(IndexOfBeforeLocation);
					}

					if (IndexOfBeforeLocation>=1)
					{
					    beforebeforeLocation=DBLs.get(IndexOfBeforeLocation-1);
					}


					//update IsSpeedChange for previous DB location
					if ((beforeLocation!=null) &&
						((beforeLocation.pSpeed!=null && beforeLocation.pSpeed<1) && beforeLocation.IsSpeedChange) &&
						(((beforebeforeLocation!=null) && (beforebeforeLocation.pSpeed!=null && beforebeforeLocation.pSpeed<1)) || (beforebeforeLocation==null)))
					{
					    beforeLocation.IsSpeedChange=false;
					    UpdateLocationMark(DBConn,beforeLocation.locationId,"S","N");
					}
				    }

				}
				else if (LR[i].pSpeed!= null && LR[i].pSpeed>=1)
				{
				    if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed<1) && (!BeforePoint.IsSpeedChange))
				    {
					BeforePoint.IsSpeedChange=true;
					UpdateLocationMark(DBConn,BeforePoint.locationId,"S","Y");

				    }

				    if ((AfterPoint!=null) && (AfterPoint.pSpeed!=null &&  AfterPoint.pSpeed<1) && (!AfterPoint.IsSpeedChange))
				    {
					AfterPoint.IsSpeedChange=true;
					UpdateLocationMark(DBConn,AfterPoint.locationId,"S","Y");
				    }
				}


				if (BeforePoint!=null)
				{							
				    IsDud=IsDudPoint(BeforePoint, LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				if (AfterPoint!=null)
				{
				    boolean IsAfterPointDud=IsDudPoint(LR[i], AfterPoint);
				    if (AfterPoint.IsDud!=IsAfterPointDud)
				    {
					UpdateLocationMark(DBConn,AfterPoint.locationId,"D",(IsAfterPointDud ? "Y" : "N"));
					AfterPoint.IsDud=IsAfterPointDud;
				    }
				}

				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				long database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				DBLs.add(newLocation);
				java.util.Collections.sort(DBLs);


				/////////////////////////////////////////////////
				//Split events
				//--update close events for old zones with before location as exit points
				//--create new close events for old zones with after location as entry point

				Iterator<Map.Entry<Integer, Event>> it = SameRangeZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int thisZone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(thisZone_czo_id))
				    {									
					////////////////////
					Event PreviousEvent=thisEntry.getValue();
					/////////////////////
					Event AfterEvent=new Event(0,thisZone_czo_id,AfterPoint.locationId,PreviousEvent.CurrentpointID,PreviousEvent.IsCloseEvent,AfterPoint.recordTime,PreviousEvent.OutTime); 
					DerivedEventList.add(AfterEvent);
					/////////////////////
					//Change value in event cache
					DBEs.add(AfterEvent);


					//update PreviousEvent
					PreviousEvent.CurrentpointID=BeforePoint.locationId;
					PreviousEvent.OutTime=BeforePoint.recordTime;
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}

				    }
				    else
				    {
					CurrentZones.remove((Object)thisZone_czo_id);
				    }
				}

				///////////////////////////
				//extend before events and after events
				for (Integer thisZone_czo_id:CurrentZones)
				{

				    if (BeforePointZoneEvents.containsKey(thisZone_czo_id))
				    {
					////////////////////////////////////////////////
					//  For before closed events which both before and current locations belong to, update exit point of before events with current locations. 每 must a close event
					////////////////////////////////////////////////

					Event BeforeOutEvent=BeforePointZoneEvents.get(thisZone_czo_id);
					BeforeOutEvent.CurrentpointID=database_location_id;
					BeforeOutEvent.OutTime=LR[i].pTime;									
					if (!DerivedEventList.contains(BeforeOutEvent))
					{
					    DerivedEventList.add(BeforeOutEvent);
					}


				    }
				    else if (AfterPointZoneEvents.containsKey(thisZone_czo_id))
				    {
					////////////////////////////////////////////////
					//  For after events which only current and after locations belong to, update entry point of after events with current locations.
					////////////////////////////////////////////////									

					Event AfterEntryEvent=AfterPointZoneEvents.get(thisZone_czo_id);
					AfterEntryEvent.EntrypointID=database_location_id;
					AfterEntryEvent.EntryTime=LR[i].pTime;
					if (!DerivedEventList.contains(AfterEntryEvent))
					{
					    DerivedEventList.add(AfterEntryEvent);
					}

				    }
				    else
				    {
					////////////////////////////////////////////////
					//  For zones which only current location belongs to, fire new close events for zones with current locations as both entry and exit point. 每 must a close events
					////////////////////////////////////////////////									
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id,database_location_id,true,LR[i].pTime,LR[i].pTime);
					DerivedEventList.add(NewEvent);
					DBEs.add(NewEvent);
				    }									
				}								

			    }


			    break;
			case 3:	//3 for half satellite half shore-based locations

			    PreviousZoneEvents =new HashMap<Integer, Event>();

			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];
				Timestamp TheTimeStampOfCurrentPoint=thisLOCRow.pTime;

				if (!TheTimeStampOfCurrentPoint.after(LastDBLocation.recordTime))
				{
				    ///////////////////////
				    //Satellite point

				    ///////////////////////////////////
				    //find out before and after points
				    DBLocation BeforePoint = null;
				    DBLocation AfterPoint = null;
				    boolean HasExisted=false;
				    DBLocation ExistingLocation=null;
				    for (int a=0;a<DBLs.size();a++)
				    {
					DBLocation thisLocation=DBLs.get(a);
					if (thisLocation.recordTime.before(TheTimeStampOfCurrentPoint))
					{
					    BeforePoint=thisLocation;
					}
					else if(thisLocation.recordTime.after(TheTimeStampOfCurrentPoint))
					{
					    AfterPoint=thisLocation;
					    break;
					}
					else if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
					{
					    ExistingLocation=thisLocation;
					    HasExisted=true;
					    break;
					}

				    }

				    /////////////////////////////
				    //If point with same record time exists, then ignore
				    if (HasExisted)
				    {
					if ((LR[i].vEta!=null && !LR[i].vEta.equals(ExistingLocation.ETA))|| (LR[i].vEta==null && ExistingLocation.ETA!=null)                                                                      
						|| (LR[i].pHdg!=null && !LR[i].pHdg.equals(ExistingLocation.Heading)) || (LR[i].pHdg==null && ExistingLocation.Heading!=null)                                             
						|| (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(ExistingLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && ExistingLocation.DIT_Status_ID!=null)      
						|| (LR[i].vDest != null && !LR[i].vDest.equals(ExistingLocation.Destination)) || (LR[i].vDest == null && ExistingLocation.Destination!=null)                                                                                        
						|| (LR[i].vDraught!=null && !LR[i].vDraught.equals(ExistingLocation.Draught)) || (LR[i].vDraught==null && ExistingLocation.Draught!=null )
						|| (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(ExistingLocation.pSpeed)) ||(LR[i].pSpeed==null && ExistingLocation.pSpeed!=null)
						)                                                                                                                                                                   
					{                                                                                                                                                                       
					    PopulateLocation(DBConn,Ves_id,LR[i],ExistingLocation.IsSpeedChange,ExistingLocation.IsDud);
					    ExistingLocation.Destination=LR[i].vDest;
					    ExistingLocation.ETA=LR[i].vEta;
					    ExistingLocation.Heading=LR[i].pHdg;
					    ExistingLocation.DIT_Status_ID=LR[i].dit_status_id;
					    ExistingLocation.Draught=LR[i].vDraught;
					    ExistingLocation.pSpeed=LR[i].pSpeed;
					    VTLocationLogger.debug("Update location to:"+ LR[i].toString());
					}      
					else
					{
					    VTLocationLogger.warn("Ignore location: "+ LR[i].toString()+" because location with same record_time has existed in database");
					}
					continue;
				    }


				    //////////////////////////////////////////////////
				    //Calculate zones where current locations belong to
				    //////////////////////////////////////////////////							
				    ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);							

				    if (CurrentZones.isEmpty())
				    {
					VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because no polygon matchs");
					continue;
				    }

				    ////////////////////////////
				    //Collecting same range events
				    HashMap<Integer, Event> SameRangeZoneEvents=new HashMap<Integer, Event>();

				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.EntryTime.before(TheTimeStampOfCurrentPoint) && thisEvent.OutTime.after(TheTimeStampOfCurrentPoint))
					{
					    SameRangeZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }

				    ////////////////////////////
				    //Collecting before events
				    HashMap<Integer, Event> BeforePointZoneEvents=new HashMap<Integer, Event>();

				    if (BeforePoint!=null)
				    {
					for (Event thisEvent:DBEs)
					{
					    if (thisEvent.CurrentpointID==BeforePoint.locationId)
					    {
						BeforePointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}
				    }

				    ////////////////////////////
				    //Collecting after events
				    HashMap<Integer, Event> AfterPointZoneEvents=new HashMap<Integer, Event>();
				    if (AfterPoint!=null)
				    {
					for (Event thisEvent:DBEs)
					{
					    if (thisEvent.EntrypointID==AfterPoint.locationId)
					    {
						AfterPointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}
				    }

				    ////////////////////////////
				    //Calculate SpeedChange

				    IsSpeedChange=false;
				    if(LR[i].pSpeed!=null && LR[i].pSpeed<1)
				    {
					if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1))
					{
					    IsSpeedChange=true;

					    //////////////////////////////////////////////////
					    //Populate location to database
					    //////////////////////////////////////////////////
					    int IndexOfNextLocation=DBLs.indexOf(BeforePoint)+1;

					    DBLocation nextLocation=null;
					    DBLocation nextnextLocation=null;
					    if ((IndexOfNextLocation<(DBLs.size())))
					    {
						nextLocation=DBLs.get(IndexOfNextLocation);
					    }

					    if ((IndexOfNextLocation<(DBLs.size()-1)))
					    {
						nextnextLocation=DBLs.get(IndexOfNextLocation+1);
					    }   

					    //update IsSpeedChange for next DB location 
					    if((nextLocation!=null) && 
						    ((nextLocation.pSpeed != null && nextLocation.pSpeed<1) && nextLocation.IsSpeedChange) &&
						    (((nextnextLocation!=null) && (nextnextLocation.pSpeed != null && nextnextLocation.pSpeed<1)) || (nextnextLocation==null)))
					    {
						nextLocation.IsSpeedChange=false;
						UpdateLocationMark(DBConn,nextLocation.locationId,"S","N");
					    }

					}
					else if (((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)))
					{
					    IsSpeedChange=true;
					    //////////////////////////////////////////////////
					    //Populate location to database
					    //////////////////////////////////////////////////
					    int IndexOfBeforeLocation=DBLs.indexOf(AfterPoint)-1;

					    DBLocation beforeLocation=null;
					    DBLocation beforebeforeLocation=null;
					    if (IndexOfBeforeLocation>=0)
					    {
						beforeLocation=DBLs.get(IndexOfBeforeLocation);
					    }

					    if (IndexOfBeforeLocation>=1)
					    {
						beforebeforeLocation=DBLs.get(IndexOfBeforeLocation-1);
					    }


					    //update IsSpeedChange for previous DB location
					    if ((beforeLocation!=null) &&
						    ((beforeLocation.pSpeed!=null && beforeLocation.pSpeed<1) && beforeLocation.IsSpeedChange) &&
						    (((beforebeforeLocation!=null) && (beforebeforeLocation.pSpeed!=null && beforebeforeLocation.pSpeed<1)) || (beforebeforeLocation==null)))
					    {
						beforeLocation.IsSpeedChange=false;
						UpdateLocationMark(DBConn,beforeLocation.locationId,"S","N");
					    }
					}

				    }
				    else if (LR[i].pSpeed!=null && LR[i].pSpeed>=1)
				    {
					if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed<1) && (!BeforePoint.IsSpeedChange))
					{
					    BeforePoint.IsSpeedChange=true;
					    UpdateLocationMark(DBConn,BeforePoint.locationId,"S","Y");

					}

					if ((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed<1) && (!AfterPoint.IsSpeedChange))
					{
					    AfterPoint.IsSpeedChange=true;
					    UpdateLocationMark(DBConn,AfterPoint.locationId,"S","Y");
					    if (AfterPoint.locationId==LastDBLocation.locationId)
					    {
						LastDBLocation.IsSpeedChange=true;
					    }

					}
				    }

				    if (BeforePoint!=null)
				    {							
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				    else
				    {
					IsDud=false;
				    }

				    if (AfterPoint!=null)
				    {
					boolean IsAfterPointDud=IsDudPoint(LR[i], AfterPoint);
					if (AfterPoint.IsDud!=IsAfterPointDud)
					{
					    UpdateLocationMark(DBConn,AfterPoint.locationId,"D",(IsAfterPointDud ? "Y" : "N"));
					    AfterPoint.IsDud=IsAfterPointDud;
					}
				    }


				    //////////////////////////////////////////////////
				    //Populate location to database
				    //////////////////////////////////////////////////
				    long database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange,IsDud);

				    DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				    DBLs.add(newLocation);
				    java.util.Collections.sort(DBLs);


				    /////////////////////////////////////////////////
				    //Split events
				    //--update close events for old zones with before location as exit points
				    //--create new close events for old zones with after location as entry point

				    Iterator<Map.Entry<Integer, Event>> it = SameRangeZoneEvents.entrySet().iterator();  
				    while (it.hasNext())
				    {
					Map.Entry<Integer, Event> thisEntry=it.next();
					int thisZone_czo_id = thisEntry.getKey();
					if (!CurrentZones.contains(thisZone_czo_id))
					{									
					    ////////////////////
					    Event PreviousEvent=thisEntry.getValue();
					    /////////////////////
					    Event AfterEvent=new Event(0,thisZone_czo_id,AfterPoint.locationId,PreviousEvent.CurrentpointID,PreviousEvent.IsCloseEvent,AfterPoint.recordTime,PreviousEvent.OutTime); 
					    DerivedEventList.add(AfterEvent);
					    DBEs.add(AfterEvent);//Change value in event cache

					    PreviousEvent.CurrentpointID=BeforePoint.locationId;
					    PreviousEvent.OutTime=BeforePoint.recordTime;
					    PreviousEvent.IsCloseEvent=true;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }



					}
					else
					{
					    CurrentZones.remove((Object)thisZone_czo_id);
					}
				    }

				    ///////////////////////////
				    //extend before events and after events
				    for (Integer thisZone_czo_id:CurrentZones)
				    {

					if (BeforePointZoneEvents.containsKey(thisZone_czo_id))
					{
					    ////////////////////////////////////////////////
					    //  For before closed events which both before and current locations belong to, update exit point of before events with current locations. 每 must a close event
					    ////////////////////////////////////////////////

					    Event BeforeOutEvent=BeforePointZoneEvents.get(thisZone_czo_id);
					    BeforeOutEvent.CurrentpointID=database_location_id;
					    BeforeOutEvent.OutTime=LR[i].pTime;									
					    if (!DerivedEventList.contains(BeforeOutEvent))
					    {
						DerivedEventList.add(BeforeOutEvent);
					    }


					}
					else if (AfterPointZoneEvents.containsKey(thisZone_czo_id))
					{
					    ////////////////////////////////////////////////
					    //  For after events which only current and after locations belong to, update entry point of after events with current locations.
					    ////////////////////////////////////////////////									

					    Event AfterEntryEvent=AfterPointZoneEvents.get(thisZone_czo_id);
					    AfterEntryEvent.EntrypointID=database_location_id;
					    AfterEntryEvent.EntryTime=LR[i].pTime;
					    if (!DerivedEventList.contains(AfterEntryEvent))
					    {
						DerivedEventList.add(AfterEntryEvent);
					    }

					}
					else
					{
					    ////////////////////////////////////////////////
					    //  For zones which only current location belongs to, fire new close events for zones with current locations as both entry and exit point. 每 must a close events
					    ////////////////////////////////////////////////									
					    Event NewEvent=new Event(0,thisZone_czo_id,database_location_id,database_location_id,true,LR[i].pTime,LR[i].pTime);
					    DerivedEventList.add(NewEvent);
					    DBEs.add(NewEvent);
					}									
				    }							
				}
				else
				{
				    //shore based point
				    ////////////////////////////////////////////////		//
				    //Calculate zones where current locations belong to
				    //////////////////////////////////////////////////							
				    ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				    if (CurrentZones.isEmpty())
				    {
					VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
					continue;
				    }

				    //////////////////////////////////////////////////
				    //Insert shore-based location to database
				    //////////////////////////////////////////////////
				    long database_location_id;

				    //Determine speedchange flag
				    IsSpeedChange=false;


				    if (!LR[i-1].pTime.after(LastDBLocation.recordTime))
				    {
					for (Event thisEvent : DBEs)
					{
					    if (thisEvent.CurrentpointID==LastDBLocation.locationId)
					    {
						PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}


					if ((thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1) && ((LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1) || (i<(LR.length-1)&&(LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))))
					{
					    IsSpeedChange=true;
					}
					else if (((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed>1) )
					{
					    //Check if the last db location needs to be set to speed point
					    //update last DB location as speed change point.
					    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
					}
				    }
				    else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				    {
					//last shore based location in current file
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				    {
					if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}
				    }

				    if (LR[i-1].pTime.before(LastDBLocation.recordTime))
				    {
					IsDud=IsDudPoint(LastDBLocation,thisLOCRow);
				    }
				    else
				    {
					IsDud=IsDudPoint(LR[i-1],thisLOCRow);
				    }

				    database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				    if (database_location_id==-1)
				    {
					VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
					continue;
				    }

				    if (database_location_id==0)
				    {
					VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
					continue;
				    }

				    //set last valid location id
				    last_database_location_id=database_location_id;								

				    //////////////////////////////////////////////////
				    //For previous zones which none of current locations belong to, fire close events with last location as exit point
				    //////////////////////////////////////////////////

				    Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				    while (it.hasNext())
				    {
					Map.Entry<Integer, Event> thisEntry=it.next();
					int Zone_czo_id = thisEntry.getKey();
					if (!CurrentZones.contains(Zone_czo_id))
					{
					    Event PreviousEvent=thisEntry.getValue();
					    PreviousEvent.IsCloseEvent=true;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }
					    //remove close event from PreviousZoneEvents;
					    it.remove();
					}
				    }


				    for (Integer thisZone_czo_id : CurrentZones) {

					if (PreviousZoneEvents.containsKey(thisZone_czo_id))
					{
					    //////////////////////////////////////////////////
					    //For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					    //////////////////////////////////////////////////
					    Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					    PreviousEvent.CurrentpointID=database_location_id ;
					    PreviousEvent.IsCloseEvent=false;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }
					}
					else
					{
					    //////////////////////////////////////////////////
					    //For current zones which only current locations belong to, fire new open events
					    //////////////////////////////////////////////////
					    Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					    PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					    DerivedEventList.add(NewEvent);		

					}
				    }


				}
			    }

			    break;
			case 4:	//4 for no location in db before

			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
				    continue;
				}

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;

				if ((i==0) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[1].pSpeed!=null && LR[1].pSpeed>=1) )
				    {
					IsSpeedChange=true;
				    }
				}
				else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}
				else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i>0)
				{
				    IsDud=IsDudPoint(LR[i-1],LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			    break;

			}

			/////////////////////////////////
			//Populate Derived Events
			/////////////////////////////////
			for (Event aEvent:DerivedEventList)
			{
			    PopulateEvent(DBConn,Ves_id,aEvent);
			}

			/////////////////////////////////
			//Update flag of last location
			/////////////////////////////////
			if (last_database_location_id!=-1)
			{
			    UpdateLastLocationFlag(DBConn, Ves_id,last_database_location_id);
			}


			/////////////////////////////////////////
			//Update modify timestamp
			/////////////////////////////////////////
			CallableStatement csSetTime = DBConn.prepareCall("begin vessel_load_pkg.set_change_timestamp_proc(?);  end;");

			try {
			    csSetTime.setLong(1, Ves_id);
			    csSetTime.execute();	

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn(
				    "Failed to change Physical Asset modify_date for I"+ LR[0].sImo+" ship ID:"+LR[0].shipId , e);					
			}		


			DBConn.commit();
			VTLocationLogger.debug(
				"Vessel:  loaded current location event data successfully for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

		    }
		    catch (Exception e) {

			VTLocationLogger.warn(
				"Vessel:  loading current location event data failed" , e);
			e.printStackTrace();
		    }
		}


		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}

	    }
	    finally
	    {
		try {
		    DBConn.close();
		} catch (SQLException e) {
		    VTLocationLogger.warn("SQLException", e);	
		}					

		synchronized (VTLocationLoader.this)
		{
		    VTLocationLoader.this.ThreadCounter--;

		    if (VTLocationLoader.this.ThreadCounter==0)
		    {			
			VTLocationLoader.this.notify();
		    }
		}
	    }
	}
    }


    private class CurrentLoad implements Runnable
    {

	private ArrayList<long[]> IMOPositions;

	public CurrentLoad(ArrayList<long[]> IPs)
	{
	    IMOPositions=IPs;
	    synchronized (VTLocationLoader.this)
	    {
		VTLocationLoader.this.ThreadCounter++;
	    }
	}

	public void run() {

	    Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);

	    try
	    {
		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{

		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.initialize_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();					

		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}					

		while(true)
		{
		    long[] Positions;

		    synchronized(IMOPositions)
		    {
			if (!IMOPositions.isEmpty())
			{
			    Positions=IMOPositions.remove(0);
			}
			else
			{
			    break;
			}						
		    }

		    try {						

			VTLocationRow[] LR=VTLocationLoader.this.ReadLocationRow(Positions);
			//Sort locations by record time
			Arrays.sort(LR);

			///////////////////////////
			//Get Vessel Database ID
			///////////////////////////	

			long Ves_id;

			CallableStatement objGetVesselID=null;

			objGetVesselID = DBConn.prepareCall(GetVesID);
			objGetVesselID.registerOutParameter(1, OracleTypes.DECIMAL);
			objGetVesselID.setLong(2,LR[0].shipId );
			objGetVesselID.setInt(3,LR[0].sImo );
			objGetVesselID.setString(4, LR[0].sName);
			objGetVesselID.setLong(5, LR[0].sMmsi);
			objGetVesselID.setString(6, LR[0].sCallsign);
			objGetVesselID.setInt(7, LR[0].sShiptype );
			if (LR[0].sLength!=null)
			{
			    objGetVesselID.setDouble(8, LR[0].sLength);
			}
			else
			{
			    objGetVesselID.setNull(8, OracleTypes.DECIMAL);
			}

			if (LR[0].sWidth!=null)
			{
			    objGetVesselID.setDouble(9, LR[0].sWidth );
			}
			else
			{
			    objGetVesselID.setNull(9, OracleTypes.DECIMAL);
			}


			objGetVesselID.execute();						
			Ves_id = objGetVesselID.getLong(1);

			objGetVesselID.close();

			if (Ves_id==0)
			{
			    VTLocationLogger.debug("Vessel:  Skip unknown vessel IMO: "+ LR[0].sImo + " ship ID:"+LR[0].shipId);
			    continue;
			}


			///////////////////////////
			//Get Vessel Product Type
			///////////////////////////					

			PreparedStatement objGetVesselType = DBConn.prepareStatement(GetVesselProductType);
			objGetVesselType.setLong(1, Ves_id);
			ResultSet objVesselTypeResult = objGetVesselType.executeQuery();

			String VesselProductType=null;

			if (objVesselTypeResult.next())
			{
			    VesselProductType=PatRootType.getInstance(objVesselTypeResult.getInt("pat_id")).getRootType();
			    objVesselTypeResult.close();
			    objGetVesselType.close();

			}
			else
			{
			    VTLocationLogger.debug(
				    "Vessel:  Skip vessel: "+ LR[0].sImo+" ship ID:"+LR[0].shipId + ", because of unknown vessel type!");
			    objVesselTypeResult.close();
			    objGetVesselType.close();
			    continue;
			}



			VTLocationLogger.debug(
				"Vessel location event: start processing current locations data for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			/////////////////////////////////////////
			//Get the last location record time 每 must be open events
			/////////////////////////////////////////	
			VTLocationLogger.debug(
				"Vessel:  start retrieving latest events for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

			CallableStatement objStatement = DBConn.prepareCall("{call vessel_load_pkg.get_lat_location_proc(?,?,?,?,?,?,?,?,?,?,?,?,?)}");
			objStatement.setLong(1, Ves_id);
			objStatement.registerOutParameter(2, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(3, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(4, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(5, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(6, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(7, OracleTypes.VARCHAR);						
			objStatement.registerOutParameter(8, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(9, OracleTypes.TIMESTAMP);
			objStatement.registerOutParameter(10, OracleTypes.FLOAT);
			objStatement.registerOutParameter(11, OracleTypes.DECIMAL);
			objStatement.registerOutParameter(12, OracleTypes.VARCHAR);
			objStatement.registerOutParameter(13, OracleTypes.FLOAT);
			objStatement.execute();


			long last_point_id=objStatement.getLong(2);
			Double pSpeed=objStatement.getDouble(3);
			if (objStatement.wasNull())
			{
			    pSpeed=null;
			}

			Double pLong=objStatement.getDouble(4);
			Double pLat=objStatement.getDouble(5);

			boolean IsSpeedChange=false;
			String Temp_SpeedChange=objStatement.getString(6);
			if (Temp_SpeedChange!=null && (Temp_SpeedChange.equals("Y")))
			{
			    IsSpeedChange=true;
			}

			boolean IsDud=false;
			String Temp_Dud=objStatement.getString(7);
			if (Temp_Dud!=null && (Temp_Dud.equals("Y")))
			{
			    IsDud=true;
			}

			Timestamp Previous_record_time=objStatement.getTimestamp(8);
			Timestamp ETA=objStatement.getTimestamp(9);
			if (objStatement.wasNull())
			{
			    ETA=null;
			}

			Double Heading=objStatement.getDouble(10);
			if (objStatement.wasNull())
			{
			    Heading=null;
			}

			Integer Dit_status_id=objStatement.getInt(11);
			if (objStatement.wasNull())
			{
			    Dit_status_id=null;
			}

			String Destination=objStatement.getString(12);
			Double Draught=objStatement.getDouble(13);
			if (objStatement.wasNull())
			{
			    Draught=null;
			}						


			DBLocation LastDBLocation=new DBLocation(last_point_id, Previous_record_time, pSpeed,
				pLat, pLong, IsSpeedChange, IsDud,
				ETA, Heading, Dit_status_id,
				Destination, Draught);


			objStatement.close();

			//////////////////////////////
			//Cache points and events

			//Database location cache
			ArrayList<DBLocation> DBLs=new ArrayList<DBLocation>();
			ArrayList<Event> DBEs=new ArrayList<Event>();
			int LogicType=0; 
			//1 for all shore-based locations
			//2 for all satellite locations
			//3 for half satellite half shore-based locations
			//4 for no location in db before



			if (LastDBLocation.locationId!=-1 )
			{
			    //Existing vessel
			    if (LR.length==1 && LR[0].pTime.equals(LastDBLocation.recordTime))
			    {
				if ((LR[0].vEta!=null && !LR[0].vEta.equals(LastDBLocation.ETA))|| (LR[0].vEta==null && LastDBLocation.ETA!=null)
					|| (LR[0].pHdg!=null && !LR[0].pHdg.equals(LastDBLocation.Heading)) || (LR[0].pHdg==null && LastDBLocation.Heading!=null)
					|| (LR[0].dit_status_id!=null && !LR[0].dit_status_id.equals(LastDBLocation.DIT_Status_ID)) || (LR[0].dit_status_id==null && LastDBLocation.DIT_Status_ID!=null)
					|| (LR[0].vDest != null && !LR[0].vDest.equals(LastDBLocation.Destination)) || (LR[0].vDest == null && LastDBLocation.Destination!=null)
					|| (LR[0].vDraught!=null && !LR[0].vDraught.equals(LastDBLocation.Draught)) || (LR[0].vDraught==null && LastDBLocation.Draught!=null )
					|| (LR[0].pSpeed!=null && !LR[0].pSpeed.equals(LastDBLocation.pSpeed)) ||(LR[0].pSpeed==null && LastDBLocation.pSpeed!=null)
					)
				{
				    PopulateLocation(DBConn,Ves_id,LR[0],LastDBLocation.IsSpeedChange,LastDBLocation.IsDud);
				    VTLocationLogger.debug("Update location to:"+ LR[0].toString());
				}
				else
				{
				    VTLocationLogger.debug("Ignore location: "+ LR[0].toString()+" because location with same record_time has existed in database");							    
				}
				continue;
			    }
			    else if (!LR[0].pTime.before(LastDBLocation.recordTime))
			    {	
				LogicType=1;
				//All the locations in file are shore-based, just need to cache
				//(1)events with last db point as exit


				PreparedStatement objGetEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				objGetEventsByLastPoints.setLong(1, LastDBLocation.locationId);
				ResultSet ObjEvents=objGetEventsByLastPoints.executeQuery();

				ObjEvents.setFetchSize(100);	

				while(ObjEvents.next())
				{
				    long event_id=ObjEvents.getInt("id");
				    int czo_id=ObjEvents.getInt("czo_id");
				    long entry_point_id=ObjEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjEvents.getTimestamp("entry_time");
				    long out_point_id=ObjEvents.getLong("out_point_id");
				    Timestamp out_time=ObjEvents.getTimestamp("entry_time");
				    boolean is_close=(ObjEvents.getString("is_closed")=="N") ? false : true;

				    Event thisEvent=new Event(event_id,czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);

				}	
				ObjEvents.close();
				objGetEventsByLastPoints.close();



			    }
			    else if (LR[LR.length-1].pTime.before(LastDBLocation.recordTime) )
			    {
				LogicType=2;
				//							//////////////////////////////////////////////////////////////////////////
				//All the locations in file are satellite points, need to cache
				//(1) from 2 db before points until 2 db after points
				Timestamp TheTimeStampOfFirstPoint=LR[0].pTime;
				Timestamp TheTimeStampOfLastPoint=LR[LR.length-1].pTime;


				PreparedStatement objGet2BeforeUntil2AfterLocations=DBConn.prepareStatement(Get2BeforeUntil2AfterLocations);
				objGet2BeforeUntil2AfterLocations.setLong(1, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(2, TheTimeStampOfFirstPoint );
				objGet2BeforeUntil2AfterLocations.setLong(3, Ves_id);
				objGet2BeforeUntil2AfterLocations.setTimestamp(4, TheTimeStampOfLastPoint );
				objGet2BeforeUntil2AfterLocations.setLong(5, Ves_id );
				objGet2BeforeUntil2AfterLocations.setTimestamp(6, TheTimeStampOfLastPoint );
				objGet2BeforeUntil2AfterLocations.setTimestamp(7, TheTimeStampOfFirstPoint );

				ResultSet PointsOf2BeforeUntil2After=objGet2BeforeUntil2AfterLocations.executeQuery();
				PointsOf2BeforeUntil2After.setFetchSize(1000);
				while(PointsOf2BeforeUntil2After.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntil2After.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntil2After.getDouble(2);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntil2After.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntil2After.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntil2After.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntil2After.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntil2After.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntil2After.getTimestamp(8);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntil2After.getDouble(9);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntil2After.getInt(10);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntil2After.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntil2After.getDouble(12);
				    if (PointsOf2BeforeUntil2After.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught);
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntil2After.close();
				objGet2BeforeUntil2AfterLocations.close();


				java.util.Collections.sort(DBLs);

				//							//////////////////////////////////////////////////////////////////////////
				//(2) all events with exit point later than the first point and entry point earlier than the last file point

				PreparedStatement objGetAllOverlappingEvents=DBConn.prepareStatement(GetAllOverlappingEvents);
				objGetAllOverlappingEvents.setLong(1, Ves_id);
				objGetAllOverlappingEvents.setTimestamp(2, TheTimeStampOfLastPoint );
				objGetAllOverlappingEvents.setTimestamp(3, TheTimeStampOfFirstPoint );

				ResultSet OverlappingEvents=objGetAllOverlappingEvents.executeQuery();

				while(OverlappingEvents.next())
				{
				    long event_id=OverlappingEvents.getLong("id");
				    int zone_czo_id=OverlappingEvents.getInt("czo_id");
				    long entry_point_id=OverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=OverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=OverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=OverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(OverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time, out_time);
				    DBEs.add(thisEvent);
				}
				OverlappingEvents.close();
				objGetAllOverlappingEvents.close();

				////////////////////////////////////////////////////////////////////////////
				//(3) events with before point as exit and events with after point as entry

				////////////////////////////////
				//Get all events which before point is related as exit point
				/////////////////////////////////							

				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(TheTimeStampOfFirstPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}


				if (BeforePoint!=null)
				{
				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();
				}

				/////////////////////////////////
				//Get all events which after point is related as entry point
				/////////////////////////////////
				//Finding after point
				DBLocation AfterPoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.after(TheTimeStampOfLastPoint))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				}

				PreparedStatement objGetAfterPointEventsByLastPoints=DBConn.prepareStatement(GetEventsByEntryPoint);
				objGetAfterPointEventsByLastPoints.setLong(1, AfterPoint.locationId);
				ResultSet ObjAfterPointEvents=objGetAfterPointEventsByLastPoints.executeQuery();

				while(ObjAfterPointEvents.next())
				{
				    long event_id=ObjAfterPointEvents.getLong("id");
				    int zone_czo_id=ObjAfterPointEvents.getInt("czo_id");
				    long entry_point_id=ObjAfterPointEvents.getLong("entry_point_id");
				    Timestamp entry_time=ObjAfterPointEvents.getTimestamp("entry_time");
				    long out_point_id=ObjAfterPointEvents.getLong("out_point_id");
				    Timestamp out_time=ObjAfterPointEvents.getTimestamp("out_time");
				    boolean is_close=(ObjAfterPointEvents.getString("is_closed").equals("N") ? false : true);

				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}	
				ObjAfterPointEvents.close();
				objGetAfterPointEventsByLastPoints.close();

			    }
			    else
			    {
				LogicType=3;
				//there are some satellite points in files, need to cache
				//(1) from 2 db before points until the last db point.


				Timestamp TheTimeStampOfFirstPoint=LR[0].pTime;
				PreparedStatement objGet2BeforeUntilLastLocation=DBConn.prepareStatement(Get2BeforeUntilLastLocation);
				objGet2BeforeUntilLastLocation.setLong(1, Ves_id);
				objGet2BeforeUntilLastLocation.setTimestamp(2, TheTimeStampOfFirstPoint );
				objGet2BeforeUntilLastLocation.setLong(3, Ves_id );
				objGet2BeforeUntilLastLocation.setTimestamp(4, TheTimeStampOfFirstPoint );

				ResultSet PointsOf2BeforeUntilLast=objGet2BeforeUntilLastLocation.executeQuery();
				PointsOf2BeforeUntilLast.setFetchSize(1000);
				while(PointsOf2BeforeUntilLast.next())
				{
				    //(1)id
				    //(2)speed
				    //(3)longitude
				    //(4)latitude
				    //(5)is_speed_change
				    //(6)is_dud
				    //(7)record_time
				    //(8)eta
				    //(9)heading
				    //(10)dit_status_id
				    //(11)destination
				    //(12)ais_draught
				    long  point_id=PointsOf2BeforeUntilLast.getLong(1);
				    Double point_Speed=PointsOf2BeforeUntilLast.getDouble(2);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_Speed=null;
				    }

				    Double point_Long=PointsOf2BeforeUntilLast.getDouble(3);
				    Double point_Lat=PointsOf2BeforeUntilLast.getDouble(4);

				    String temp_SC=PointsOf2BeforeUntilLast.getString(5);
				    boolean point_SpeedChange=false;
				    if (temp_SC!=null && temp_SC.equals("Y"))
				    {
					point_SpeedChange=true;
				    }

				    String temp_Dud=PointsOf2BeforeUntilLast.getString(6);
				    boolean point_Dud=false;
				    if (temp_Dud!=null && temp_Dud.equals("Y"))
				    {
					point_Dud=true;
				    }

				    Timestamp point_record_time=PointsOf2BeforeUntilLast.getTimestamp(7);

				    Timestamp point_eta=PointsOf2BeforeUntilLast.getTimestamp(8);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_eta=null;
				    }

				    Double point_heading=PointsOf2BeforeUntilLast.getDouble(9);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_heading=null;
				    }		

				    Integer point_dit_status_id=PointsOf2BeforeUntilLast.getInt(10);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_dit_status_id=null;
				    }

				    String point_destination=PointsOf2BeforeUntilLast.getString(11);
				    Double point_ais_draught=PointsOf2BeforeUntilLast.getDouble(12);
				    if (PointsOf2BeforeUntilLast.wasNull())
				    {
					point_ais_draught=null;
				    }


				    DBLocation DBLocation=new DBLocation(point_id, point_record_time,point_Speed,
					    point_Lat, point_Long, point_SpeedChange, point_Dud,
					    point_eta, point_heading, point_dit_status_id,
					    point_destination, point_ais_draught);
				    DBLs.add(DBLocation);
				}
				PointsOf2BeforeUntilLast.close();
				objGet2BeforeUntilLastLocation.close();

				java.util.Collections.sort(DBLs);							


				//(2) all events with exit point later than the first point in file


				PreparedStatement objGetBeforeOverlappingEvents=DBConn.prepareStatement(GetBeforeOverlappingEvents);
				objGetBeforeOverlappingEvents.setLong(1, Ves_id);
				objGetBeforeOverlappingEvents.setTimestamp(2, TheTimeStampOfFirstPoint );

				ResultSet BeforeOverlappingEvents=objGetBeforeOverlappingEvents.executeQuery();
				BeforeOverlappingEvents.setFetchSize(100);
				while(BeforeOverlappingEvents.next())
				{
				    long event_id=BeforeOverlappingEvents.getLong("id");
				    int zone_czo_id=BeforeOverlappingEvents.getInt("czo_id");
				    long entry_point_id=BeforeOverlappingEvents.getLong("entry_point_id");
				    Timestamp entry_time=BeforeOverlappingEvents.getTimestamp("entry_time");
				    long out_point_id=BeforeOverlappingEvents.getLong("out_point_id");
				    Timestamp out_time=BeforeOverlappingEvents.getTimestamp("out_time");
				    boolean is_close=(BeforeOverlappingEvents.getString("is_closed").equals("N") ? false : true);
				    Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
				    DBEs.add(thisEvent);
				}
				BeforeOverlappingEvents.close();
				objGetBeforeOverlappingEvents.close();							

				//(3) events with before point as exit
				//Finding before point
				DBLocation BeforePoint = null;
				for (DBLocation thisLocation: DBLs)
				{
				    if (thisLocation.recordTime.before(TheTimeStampOfFirstPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else
				    {
					break;
				    }
				}

				if (BeforePoint!=null)
				{

				    PreparedStatement objGetBeforePointEventsByLastPoints=DBConn.prepareStatement(GetEventsByOutPoint);
				    objGetBeforePointEventsByLastPoints.setLong(1, BeforePoint.locationId);
				    ResultSet ObjBeforePointEvents=objGetBeforePointEventsByLastPoints.executeQuery();
				    ObjBeforePointEvents.setFetchSize(100);
				    while(ObjBeforePointEvents.next())
				    {
					long event_id=ObjBeforePointEvents.getLong("id");
					int zone_czo_id=ObjBeforePointEvents.getInt("czo_id");
					long entry_point_id=ObjBeforePointEvents.getLong("entry_point_id");
					Timestamp entry_time=ObjBeforePointEvents.getTimestamp("entry_time");
					long out_point_id=ObjBeforePointEvents.getLong("out_point_id");
					Timestamp out_time=ObjBeforePointEvents.getTimestamp("out_time");
					boolean is_close=(ObjBeforePointEvents.getString("is_closed").equals("N") ? false : true);

					Event thisEvent=new Event(event_id,zone_czo_id,entry_point_id,out_point_id,is_close,entry_time,out_time);
					DBEs.add(thisEvent);
				    }	
				    ObjBeforePointEvents.close();
				    objGetBeforePointEventsByLastPoints.close();


				}
			    }

			}
			else
			{
			    LogicType=4;
			}

			//////////////////////////////
			long last_database_location_id=-1;

			HashMap<Integer, Event> PreviousZoneEvents =new HashMap<Integer, Event>();
			ArrayList<Event> DerivedEventList=new ArrayList<Event>();

			switch (LogicType)
			{
			case 1: //1 for all shore-based locations



			    for (Event thisEvent : DBEs)
			    {
				PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
			    }

			    //Check if the last db location needs to be set to speed point
			    if ((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1) )
			    {
				if (LR[0].pSpeed!=null && LR[0].pSpeed>=1)
				{
				    //update last DB location as speed change point.
				    LastDBLocation.IsSpeedChange=true;
				    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
				}
			    }


			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
				    continue;
				}

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;							

				if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if (i==0)
				    {
					if (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1)
					{
					    IsSpeedChange=true;

					}
					else if (LR.length>1 && (LR[1].pSpeed!=null && LR[1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}  
				    }
				    else if (i==(LR.length-1) && (LR.length>1))
				    {
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i==0)
				{
				    IsDud=IsDudPoint(LastDBLocation, thisLOCRow);
				}
				else
				{
				    IsDud=IsDudPoint(LR[i-1], thisLOCRow);
				}


				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.debug("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }



			    break;
			case 2:	//2 for all satellite locations

			    for(int i=0;i<LR.length;i++)
			    {



				///////////////////////////////////
				//find out before and after points
				Timestamp TheTimeStampOfCurrentPoint=LR[i].pTime;

				DBLocation BeforePoint = null;
				DBLocation AfterPoint = null;
				boolean HasExisted=false;
				DBLocation ExistingLocation=null;

				for (int a=0;a<DBLs.size();a++)
				{
				    DBLocation thisLocation=DBLs.get(a);
				    if (thisLocation.recordTime.before(TheTimeStampOfCurrentPoint))
				    {
					BeforePoint=thisLocation;
				    }
				    else if(thisLocation.recordTime.after(TheTimeStampOfCurrentPoint))
				    {
					AfterPoint=thisLocation;
					break;
				    }
				    else if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
				    {
					ExistingLocation=thisLocation;
					HasExisted=true;
					break;
				    }

				}

				/////////////////////////////
				//If point with same record time exists, then ignore
				if (HasExisted)
				{

				    if ((LR[i].vEta!=null && !LR[i].vEta.equals(ExistingLocation.ETA))|| (LR[i].vEta==null && ExistingLocation.ETA!=null)                                                                      
					    || (LR[i].pHdg!=null && !LR[i].pHdg.equals(ExistingLocation.Heading)) || (LR[i].pHdg==null && ExistingLocation.Heading!=null)                                             
					    || (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(ExistingLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && ExistingLocation.DIT_Status_ID!=null)      
					    || (LR[i].vDest != null && !LR[i].vDest.equals(ExistingLocation.Destination)) || (LR[i].vDest == null && ExistingLocation.Destination!=null)                                                                                                           
					    || (LR[i].vDraught!=null && !LR[i].vDraught.equals(ExistingLocation.Draught)) || (LR[i].vDraught==null && ExistingLocation.Draught!=null )
					    || (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(ExistingLocation.pSpeed)) ||(LR[i].pSpeed==null && ExistingLocation.pSpeed!=null)
					    )                                                                                                                                                                   
				    {                                                                                                                                                                       
					PopulateLocation(DBConn,Ves_id,LR[i],ExistingLocation.IsSpeedChange,ExistingLocation.IsDud);
					ExistingLocation.Destination=LR[i].vDest;
					ExistingLocation.ETA=LR[i].vEta;
					ExistingLocation.Heading=LR[i].pHdg;
					ExistingLocation.DIT_Status_ID=LR[i].dit_status_id;
					ExistingLocation.Draught=LR[i].vDraught;
					ExistingLocation.pSpeed=LR[i].pSpeed;
					VTLocationLogger.debug("Update location to:"+ LR[i].toString());
				    }                                                                                                                                                                       
				    else                                                                                                                                                                    
				    {    
					VTLocationLogger.debug("Ignore location: "+ LR[i].toString()+" because location with same record_time has existed in database");
				    }
				    continue;
				}

				//////////////////////////////////////////////////
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);							

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because no polygon matchs");
				    continue;
				}

				////////////////////////////
				//Collecting same range events
				HashMap<Integer, Event> SameRangeZoneEvents=new HashMap<Integer, Event>();

				for (Event thisEvent:DBEs)
				{
				    if (thisEvent.EntryTime.before(TheTimeStampOfCurrentPoint) && thisEvent.OutTime.after(TheTimeStampOfCurrentPoint))
				    {
					SameRangeZoneEvents.put(thisEvent.Czo_id, thisEvent);
				    }
				}

				////////////////////////////
				//Collecting before events
				HashMap<Integer, Event> BeforePointZoneEvents=new HashMap<Integer, Event>();

				if (BeforePoint!=null)
				{
				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.CurrentpointID==BeforePoint.locationId)
					{
					    BeforePointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }
				}

				////////////////////////////
				//Collecting after events
				HashMap<Integer, Event> AfterPointZoneEvents=new HashMap<Integer, Event>();
				if (AfterPoint!=null)
				{
				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.EntrypointID==AfterPoint.locationId)
					{
					    AfterPointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }
				}

				////////////////////////////
				//Calculate SpeedChange

				IsSpeedChange=false;
				if( LR[i].pSpeed!=null && LR[i].pSpeed<1)
				{
				    if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1))
				    {
					IsSpeedChange=true;

					//////////////////////////////////////////////////
					//Populate location to database
					//////////////////////////////////////////////////
					int IndexOfNextLocation=DBLs.indexOf(BeforePoint)+1;

					DBLocation nextLocation=null;
					DBLocation nextnextLocation=null;
					if ((IndexOfNextLocation<(DBLs.size())))
					{
					    nextLocation=DBLs.get(IndexOfNextLocation);
					}

					if ((IndexOfNextLocation<(DBLs.size()-1)))
					{
					    nextnextLocation=DBLs.get(IndexOfNextLocation+1);
					}   

					//update IsSpeedChange for next DB location 
					if((nextLocation!=null) && 
						((nextLocation.pSpeed != null && nextLocation.pSpeed<1) && nextLocation.IsSpeedChange) &&
						(((nextnextLocation!=null) && (nextnextLocation.pSpeed != null && nextnextLocation.pSpeed<1)) || (nextnextLocation==null)))
					{
					    nextLocation.IsSpeedChange=false;
					    UpdateLocationMark(DBConn,nextLocation.locationId,"S","N");
					}

				    }
				    else if (((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)))
				    {
					IsSpeedChange=true;
					//////////////////////////////////////////////////
					//Populate location to database
					//////////////////////////////////////////////////
					int IndexOfBeforeLocation=DBLs.indexOf(AfterPoint)-1;

					DBLocation beforeLocation=null;
					DBLocation beforebeforeLocation=null;
					if (IndexOfBeforeLocation>=0)
					{
					    beforeLocation=DBLs.get(IndexOfBeforeLocation);
					}

					if (IndexOfBeforeLocation>=1)
					{
					    beforebeforeLocation=DBLs.get(IndexOfBeforeLocation-1);
					}


					//update IsSpeedChange for previous DB location
					if ((beforeLocation!=null) &&
						((beforeLocation.pSpeed!=null && beforeLocation.pSpeed<1) && beforeLocation.IsSpeedChange) &&
						(((beforebeforeLocation!=null) && (beforebeforeLocation.pSpeed!=null && beforebeforeLocation.pSpeed<1)) || (beforebeforeLocation==null)))
					{
					    beforeLocation.IsSpeedChange=false;
					    UpdateLocationMark(DBConn,beforeLocation.locationId,"S","N");
					}
				    }

				}
				else if (LR[i].pSpeed!= null && LR[i].pSpeed>=1)
				{
				    if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed<1) && (!BeforePoint.IsSpeedChange))
				    {
					BeforePoint.IsSpeedChange=true;
					UpdateLocationMark(DBConn,BeforePoint.locationId,"S","Y");

				    }

				    if ((AfterPoint!=null) && (AfterPoint.pSpeed!=null &&  AfterPoint.pSpeed<1) && (!AfterPoint.IsSpeedChange))
				    {
					AfterPoint.IsSpeedChange=true;
					UpdateLocationMark(DBConn,AfterPoint.locationId,"S","Y");
				    }
				}


				if (BeforePoint!=null)
				{							
				    IsDud=IsDudPoint(BeforePoint, LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				if (AfterPoint!=null)
				{
				    boolean IsAfterPointDud=IsDudPoint(LR[i], AfterPoint);
				    if (AfterPoint.IsDud!=IsAfterPointDud)
				    {
					UpdateLocationMark(DBConn,AfterPoint.locationId,"D",(IsAfterPointDud ? "Y" : "N"));
					AfterPoint.IsDud=IsAfterPointDud;
				    }
				}

				//////////////////////////////////////////////////
				//Populate location to database
				//////////////////////////////////////////////////
				long database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange ,IsDud);
				DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				DBLs.add(newLocation);
				java.util.Collections.sort(DBLs);


				/////////////////////////////////////////////////
				//Split events
				//--update close events for old zones with before location as exit points
				//--create new close events for old zones with after location as entry point

				Iterator<Map.Entry<Integer, Event>> it = SameRangeZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int thisZone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(thisZone_czo_id))
				    {									
					////////////////////
					Event PreviousEvent=thisEntry.getValue();
					/////////////////////
					Event AfterEvent=new Event(0,thisZone_czo_id,AfterPoint.locationId,PreviousEvent.CurrentpointID,PreviousEvent.IsCloseEvent,AfterPoint.recordTime,PreviousEvent.OutTime); 
					DerivedEventList.add(AfterEvent);
					/////////////////////
					//Change value in event cache
					DBEs.add(AfterEvent);


					//update PreviousEvent
					PreviousEvent.CurrentpointID=BeforePoint.locationId;
					PreviousEvent.OutTime=BeforePoint.recordTime;
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}

				    }
				    else
				    {
					CurrentZones.remove((Object)thisZone_czo_id);
				    }
				}

				///////////////////////////
				//extend before events and after events
				for (Integer thisZone_czo_id:CurrentZones)
				{

				    if (BeforePointZoneEvents.containsKey(thisZone_czo_id))
				    {
					////////////////////////////////////////////////
					//  For before closed events which both before and current locations belong to, update exit point of before events with current locations. 每 must a close event
					////////////////////////////////////////////////

					Event BeforeOutEvent=BeforePointZoneEvents.get(thisZone_czo_id);
					BeforeOutEvent.CurrentpointID=database_location_id;
					BeforeOutEvent.OutTime=LR[i].pTime;									
					if (!DerivedEventList.contains(BeforeOutEvent))
					{
					    DerivedEventList.add(BeforeOutEvent);
					}


				    }
				    else if (AfterPointZoneEvents.containsKey(thisZone_czo_id))
				    {
					////////////////////////////////////////////////
					//  For after events which only current and after locations belong to, update entry point of after events with current locations.
					////////////////////////////////////////////////									

					Event AfterEntryEvent=AfterPointZoneEvents.get(thisZone_czo_id);
					AfterEntryEvent.EntrypointID=database_location_id;
					AfterEntryEvent.EntryTime=LR[i].pTime;
					if (!DerivedEventList.contains(AfterEntryEvent))
					{
					    DerivedEventList.add(AfterEntryEvent);
					}

				    }
				    else
				    {
					////////////////////////////////////////////////
					//  For zones which only current location belongs to, fire new close events for zones with current locations as both entry and exit point. 每 must a close events
					////////////////////////////////////////////////									
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id,database_location_id,true,LR[i].pTime,LR[i].pTime);
					DerivedEventList.add(NewEvent);
					DBEs.add(NewEvent);
				    }									
				}								

			    }


			    break;
			case 3:	//3 for half satellite half shore-based locations

			    PreviousZoneEvents =new HashMap<Integer, Event>();

			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];
				Timestamp TheTimeStampOfCurrentPoint=thisLOCRow.pTime;

				if (!TheTimeStampOfCurrentPoint.after(LastDBLocation.recordTime))
				{
				    ///////////////////////
				    //Satellite point

				    ///////////////////////////////////
				    //find out before and after points
				    DBLocation BeforePoint = null;
				    DBLocation AfterPoint = null;
				    boolean HasExisted=false;
				    DBLocation ExistingLocation=null;
				    for (int a=0;a<DBLs.size();a++)
				    {
					DBLocation thisLocation=DBLs.get(a);
					if (thisLocation.recordTime.before(TheTimeStampOfCurrentPoint))
					{
					    BeforePoint=thisLocation;
					}
					else if(thisLocation.recordTime.after(TheTimeStampOfCurrentPoint))
					{
					    AfterPoint=thisLocation;
					    break;
					}
					else if(thisLocation.recordTime.equals(TheTimeStampOfCurrentPoint))
					{
					    ExistingLocation=thisLocation;
					    HasExisted=true;
					    break;
					}

				    }

				    /////////////////////////////
				    //If point with same record time exists, then ignore
				    if (HasExisted)
				    {
					if ((LR[i].vEta!=null && !LR[i].vEta.equals(ExistingLocation.ETA))|| (LR[i].vEta==null && ExistingLocation.ETA!=null)                                                                      
						|| (LR[i].pHdg!=null && !LR[i].pHdg.equals(ExistingLocation.Heading)) || (LR[i].pHdg==null && ExistingLocation.Heading!=null)                                             
						|| (LR[i].dit_status_id!=null && !LR[i].dit_status_id.equals(ExistingLocation.DIT_Status_ID)) || (LR[i].dit_status_id==null && ExistingLocation.DIT_Status_ID!=null)      
						|| (LR[i].vDest != null && !LR[i].vDest.equals(ExistingLocation.Destination)) || (LR[i].vDest == null && ExistingLocation.Destination!=null)                                                                                        
						|| (LR[i].vDraught!=null && !LR[i].vDraught.equals(ExistingLocation.Draught)) || (LR[i].vDraught==null && ExistingLocation.Draught!=null )
						|| (LR[i].pSpeed!=null && !LR[i].pSpeed.equals(ExistingLocation.pSpeed)) ||(LR[i].pSpeed==null && ExistingLocation.pSpeed!=null)
						)                                                                                                                                                                   
					{                                                                                                                                                                       
					    PopulateLocation(DBConn,Ves_id,LR[i],ExistingLocation.IsSpeedChange,ExistingLocation.IsDud);
					    ExistingLocation.Destination=LR[i].vDest;
					    ExistingLocation.ETA=LR[i].vEta;
					    ExistingLocation.Heading=LR[i].pHdg;
					    ExistingLocation.DIT_Status_ID=LR[i].dit_status_id;
					    ExistingLocation.Draught=LR[i].vDraught;
					    ExistingLocation.pSpeed=LR[i].pSpeed;
					    VTLocationLogger.debug("Update location to:"+ LR[i].toString());
					}      
					else
					{
					    VTLocationLogger.debug("Ignore location: "+ LR[i].toString()+" because location with same record_time has existed in database");
					}
					continue;
				    }


				    //////////////////////////////////////////////////
				    //Calculate zones where current locations belong to
				    //////////////////////////////////////////////////							
				    ArrayList<Integer> CurrentZones = LocateCurrentZone(LR[i],VesselProductType);							

				    if (CurrentZones.isEmpty())
				    {
					VTLocationLogger.warn("Ignore location: "+ LR[i].toString() + " because no polygon matchs");
					continue;
				    }

				    ////////////////////////////
				    //Collecting same range events
				    HashMap<Integer, Event> SameRangeZoneEvents=new HashMap<Integer, Event>();

				    for (Event thisEvent:DBEs)
				    {
					if (thisEvent.EntryTime.before(TheTimeStampOfCurrentPoint) && thisEvent.OutTime.after(TheTimeStampOfCurrentPoint))
					{
					    SameRangeZoneEvents.put(thisEvent.Czo_id, thisEvent);
					}
				    }

				    ////////////////////////////
				    //Collecting before events
				    HashMap<Integer, Event> BeforePointZoneEvents=new HashMap<Integer, Event>();

				    if (BeforePoint!=null)
				    {
					for (Event thisEvent:DBEs)
					{
					    if (thisEvent.CurrentpointID==BeforePoint.locationId)
					    {
						BeforePointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}
				    }

				    ////////////////////////////
				    //Collecting after events
				    HashMap<Integer, Event> AfterPointZoneEvents=new HashMap<Integer, Event>();
				    if (AfterPoint!=null)
				    {
					for (Event thisEvent:DBEs)
					{
					    if (thisEvent.EntrypointID==AfterPoint.locationId)
					    {
						AfterPointZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}
				    }

				    ////////////////////////////
				    //Calculate SpeedChange

				    IsSpeedChange=false;
				    if(LR[i].pSpeed!=null && LR[i].pSpeed<1)
				    {
					if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed>=1))
					{
					    IsSpeedChange=true;

					    //////////////////////////////////////////////////
					    //Populate location to database
					    //////////////////////////////////////////////////
					    int IndexOfNextLocation=DBLs.indexOf(BeforePoint)+1;

					    DBLocation nextLocation=null;
					    DBLocation nextnextLocation=null;
					    if ((IndexOfNextLocation<(DBLs.size())))
					    {
						nextLocation=DBLs.get(IndexOfNextLocation);
					    }

					    if ((IndexOfNextLocation<(DBLs.size()-1)))
					    {
						nextnextLocation=DBLs.get(IndexOfNextLocation+1);
					    }   

					    //update IsSpeedChange for next DB location 
					    if((nextLocation!=null) && 
						    ((nextLocation.pSpeed != null && nextLocation.pSpeed<1) && nextLocation.IsSpeedChange) &&
						    (((nextnextLocation!=null) && (nextnextLocation.pSpeed != null && nextnextLocation.pSpeed<1)) || (nextnextLocation==null)))
					    {
						nextLocation.IsSpeedChange=false;
						UpdateLocationMark(DBConn,nextLocation.locationId,"S","N");
					    }

					}
					else if (((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed>=1)))
					{
					    IsSpeedChange=true;
					    //////////////////////////////////////////////////
					    //Populate location to database
					    //////////////////////////////////////////////////
					    int IndexOfBeforeLocation=DBLs.indexOf(AfterPoint)-1;

					    DBLocation beforeLocation=null;
					    DBLocation beforebeforeLocation=null;
					    if (IndexOfBeforeLocation>=0)
					    {
						beforeLocation=DBLs.get(IndexOfBeforeLocation);
					    }

					    if (IndexOfBeforeLocation>=1)
					    {
						beforebeforeLocation=DBLs.get(IndexOfBeforeLocation-1);
					    }


					    //update IsSpeedChange for previous DB location
					    if ((beforeLocation!=null) &&
						    ((beforeLocation.pSpeed!=null && beforeLocation.pSpeed<1) && beforeLocation.IsSpeedChange) &&
						    (((beforebeforeLocation!=null) && (beforebeforeLocation.pSpeed!=null && beforebeforeLocation.pSpeed<1)) || (beforebeforeLocation==null)))
					    {
						beforeLocation.IsSpeedChange=false;
						UpdateLocationMark(DBConn,beforeLocation.locationId,"S","N");
					    }
					}

				    }
				    else if (LR[i].pSpeed!=null && LR[i].pSpeed>=1)
				    {
					if ((BeforePoint!=null) && (BeforePoint.pSpeed!=null && BeforePoint.pSpeed<1) && (!BeforePoint.IsSpeedChange))
					{
					    BeforePoint.IsSpeedChange=true;
					    UpdateLocationMark(DBConn,BeforePoint.locationId,"S","Y");

					}

					if ((AfterPoint!=null) && (AfterPoint.pSpeed!=null && AfterPoint.pSpeed<1) && (!AfterPoint.IsSpeedChange))
					{
					    AfterPoint.IsSpeedChange=true;
					    UpdateLocationMark(DBConn,AfterPoint.locationId,"S","Y");
					    if (AfterPoint.locationId==LastDBLocation.locationId)
					    {
						LastDBLocation.IsSpeedChange=true;
					    }

					}
				    }

				    if (BeforePoint!=null)
				    {							
					IsDud=IsDudPoint(BeforePoint, LR[i]);
				    }
				    else
				    {
					IsDud=false;
				    }

				    if (AfterPoint!=null)
				    {
					boolean IsAfterPointDud=IsDudPoint(LR[i], AfterPoint);
					if (AfterPoint.IsDud!=IsAfterPointDud)
					{
					    UpdateLocationMark(DBConn,AfterPoint.locationId,"D",(IsAfterPointDud ? "Y" : "N"));
					    AfterPoint.IsDud=IsAfterPointDud;
					}
				    }


				    //////////////////////////////////////////////////
				    //Populate location to database
				    //////////////////////////////////////////////////
				    long database_location_id=PopulateLocation(DBConn,Ves_id,LR[i],IsSpeedChange,IsDud);

				    DBLocation newLocation=new DBLocation(database_location_id, LR[i].pTime, LR[i].pSpeed,LR[i].pLat,LR[i].pLong,  IsSpeedChange, IsDud,LR[i].vEta,LR[i].pHdg,LR[i].dit_status_id,LR[i].vDest,LR[i].vDraught);
				    DBLs.add(newLocation);
				    java.util.Collections.sort(DBLs);


				    /////////////////////////////////////////////////
				    //Split events
				    //--update close events for old zones with before location as exit points
				    //--create new close events for old zones with after location as entry point

				    Iterator<Map.Entry<Integer, Event>> it = SameRangeZoneEvents.entrySet().iterator();  
				    while (it.hasNext())
				    {
					Map.Entry<Integer, Event> thisEntry=it.next();
					int thisZone_czo_id = thisEntry.getKey();
					if (!CurrentZones.contains(thisZone_czo_id))
					{									
					    ////////////////////
					    Event PreviousEvent=thisEntry.getValue();
					    /////////////////////
					    Event AfterEvent=new Event(0,thisZone_czo_id,AfterPoint.locationId,PreviousEvent.CurrentpointID,PreviousEvent.IsCloseEvent,AfterPoint.recordTime,PreviousEvent.OutTime); 
					    DerivedEventList.add(AfterEvent);
					    DBEs.add(AfterEvent);//Change value in event cache

					    PreviousEvent.CurrentpointID=BeforePoint.locationId;
					    PreviousEvent.OutTime=BeforePoint.recordTime;
					    PreviousEvent.IsCloseEvent=true;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }



					}
					else
					{
					    CurrentZones.remove((Object)thisZone_czo_id);
					}
				    }

				    ///////////////////////////
				    //extend before events and after events
				    for (Integer thisZone_czo_id:CurrentZones)
				    {

					if (BeforePointZoneEvents.containsKey(thisZone_czo_id))
					{
					    ////////////////////////////////////////////////
					    //  For before closed events which both before and current locations belong to, update exit point of before events with current locations. 每 must a close event
					    ////////////////////////////////////////////////

					    Event BeforeOutEvent=BeforePointZoneEvents.get(thisZone_czo_id);
					    BeforeOutEvent.CurrentpointID=database_location_id;
					    BeforeOutEvent.OutTime=LR[i].pTime;									
					    if (!DerivedEventList.contains(BeforeOutEvent))
					    {
						DerivedEventList.add(BeforeOutEvent);
					    }


					}
					else if (AfterPointZoneEvents.containsKey(thisZone_czo_id))
					{
					    ////////////////////////////////////////////////
					    //  For after events which only current and after locations belong to, update entry point of after events with current locations.
					    ////////////////////////////////////////////////									

					    Event AfterEntryEvent=AfterPointZoneEvents.get(thisZone_czo_id);
					    AfterEntryEvent.EntrypointID=database_location_id;
					    AfterEntryEvent.EntryTime=LR[i].pTime;
					    if (!DerivedEventList.contains(AfterEntryEvent))
					    {
						DerivedEventList.add(AfterEntryEvent);
					    }

					}
					else
					{
					    ////////////////////////////////////////////////
					    //  For zones which only current location belongs to, fire new close events for zones with current locations as both entry and exit point. 每 must a close events
					    ////////////////////////////////////////////////									
					    Event NewEvent=new Event(0,thisZone_czo_id,database_location_id,database_location_id,true,LR[i].pTime,LR[i].pTime);
					    DerivedEventList.add(NewEvent);
					    DBEs.add(NewEvent);
					}									
				    }							
				}
				else
				{
				    //shore based point
				    ////////////////////////////////////////////////		//
				    //Calculate zones where current locations belong to
				    //////////////////////////////////////////////////							
				    ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				    if (CurrentZones.isEmpty())
				    {
					VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
					continue;
				    }

				    //////////////////////////////////////////////////
				    //Insert shore-based location to database
				    //////////////////////////////////////////////////
				    long database_location_id;

				    //Determine speedchange flag
				    IsSpeedChange=false;


				    if (!LR[i-1].pTime.after(LastDBLocation.recordTime))
				    {
					for (Event thisEvent : DBEs)
					{
					    if (thisEvent.CurrentpointID==LastDBLocation.locationId)
					    {
						PreviousZoneEvents.put(thisEvent.Czo_id, thisEvent);
					    }
					}


					if ((thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1) && ((LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed>=1) || (i<(LR.length-1)&&(LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))))
					{
					    IsSpeedChange=true;
					}
					else if (((!LastDBLocation.IsSpeedChange) && (LastDBLocation.pSpeed!=null && LastDBLocation.pSpeed<1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed>1) )
					{
					    //Check if the last db location needs to be set to speed point
					    //update last DB location as speed change point.
					    UpdateLocationMark(DBConn,LastDBLocation.locationId,"S","Y");
					}
				    }
				    else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				    {
					//last shore based location in current file
					if (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1)
					{
					    IsSpeedChange=true;
					}
				    }
				    else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				    {
					if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
					{
					    IsSpeedChange=true;
					}
				    }

				    if (LR[i-1].pTime.before(LastDBLocation.recordTime))
				    {
					IsDud=IsDudPoint(LastDBLocation,thisLOCRow);
				    }
				    else
				    {
					IsDud=IsDudPoint(LR[i-1],thisLOCRow);
				    }

				    database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				    if (database_location_id==-1)
				    {
					VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
					continue;
				    }

				    if (database_location_id==0)
				    {
					VTLocationLogger.debug("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
					continue;
				    }

				    //set last valid location id
				    last_database_location_id=database_location_id;								

				    //////////////////////////////////////////////////
				    //For previous zones which none of current locations belong to, fire close events with last location as exit point
				    //////////////////////////////////////////////////

				    Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				    while (it.hasNext())
				    {
					Map.Entry<Integer, Event> thisEntry=it.next();
					int Zone_czo_id = thisEntry.getKey();
					if (!CurrentZones.contains(Zone_czo_id))
					{
					    Event PreviousEvent=thisEntry.getValue();
					    PreviousEvent.IsCloseEvent=true;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }
					    //remove close event from PreviousZoneEvents;
					    it.remove();
					}
				    }


				    for (Integer thisZone_czo_id : CurrentZones) {

					if (PreviousZoneEvents.containsKey(thisZone_czo_id))
					{
					    //////////////////////////////////////////////////
					    //For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					    //////////////////////////////////////////////////
					    Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					    PreviousEvent.CurrentpointID=database_location_id ;
					    PreviousEvent.IsCloseEvent=false;

					    if (!DerivedEventList.contains(PreviousEvent))
					    {
						DerivedEventList.add(PreviousEvent);
					    }
					}
					else
					{
					    //////////////////////////////////////////////////
					    //For current zones which only current locations belong to, fire new open events
					    //////////////////////////////////////////////////
					    Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					    PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					    DerivedEventList.add(NewEvent);		

					}
				    }


				}
			    }

			    break;
			case 4:	//4 for no location in db before

			    for(int i=0;i<LR.length;i++)
			    {
				VTLocationRow thisLOCRow=LR[i];

				////////////////////////////////////////////////	//
				//Calculate zones where current locations belong to
				//////////////////////////////////////////////////							
				ArrayList<Integer> CurrentZones = LocateCurrentZone(thisLOCRow,VesselProductType);

				if (CurrentZones.isEmpty())
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because no polygon matchs");
				    continue;
				}

				//////////////////////////////////////////////////
				//Insert shore-based location to database
				//////////////////////////////////////////////////
				long database_location_id;

				//Determine speedchange flag
				IsSpeedChange=false;

				if ((i==0) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[1].pSpeed!=null && LR[1].pSpeed>=1) )
				    {
					IsSpeedChange=true;
				    }
				}
				else if ((i==(LR.length-1)) && (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1))
				{
				    if ((LR.length>=2) && (LR[LR.length-2].pSpeed!=null && LR[LR.length-2].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}
				else if (thisLOCRow.pSpeed!=null && thisLOCRow.pSpeed<1)
				{
				    if ((LR[i-1].pSpeed!=null && LR[i-1].pSpeed>=1) || (LR[i+1].pSpeed!=null && LR[i+1].pSpeed>=1))
				    {
					IsSpeedChange=true;
				    }
				}

				if (i>0)
				{
				    IsDud=IsDudPoint(LR[i-1],LR[i]);
				}
				else
				{
				    IsDud=false;
				}

				database_location_id=PopulateLocation(DBConn,Ves_id,thisLOCRow,IsSpeedChange,IsDud);

				if (database_location_id==-1)
				{
				    VTLocationLogger.warn("Ignore location: "+ thisLOCRow.toString() + " because of database exception");
				    continue;
				}

				if (database_location_id==0)
				{
				    VTLocationLogger.debug("Ignore location: "+ thisLOCRow.toString()+" because location with same record_time has existed in database");
				    continue;
				}

				//set last valid location id
				last_database_location_id=database_location_id;								

				//////////////////////////////////////////////////
				//For previous zones which none of current locations belong to, fire close events with last location as exit point
				//////////////////////////////////////////////////

				Iterator<Map.Entry<Integer, Event>> it = PreviousZoneEvents.entrySet().iterator();  
				while (it.hasNext())
				{
				    Map.Entry<Integer, Event> thisEntry=it.next();
				    int Zone_czo_id = thisEntry.getKey();
				    if (!CurrentZones.contains(Zone_czo_id))
				    {
					Event PreviousEvent=thisEntry.getValue();
					PreviousEvent.IsCloseEvent=true;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
					//remove close event from PreviousZoneEvents;
					it.remove();
				    }
				}


				for (Integer thisZone_czo_id : CurrentZones) {

				    if (PreviousZoneEvents.containsKey(thisZone_czo_id))
				    {
					//////////////////////////////////////////////////
					//For current zones which both previous and current locations belong to, update exit point of previous open events with current locations.
					//////////////////////////////////////////////////
					Event PreviousEvent=PreviousZoneEvents.get(thisZone_czo_id);
					PreviousEvent.CurrentpointID=database_location_id ;
					PreviousEvent.IsCloseEvent=false;

					if (!DerivedEventList.contains(PreviousEvent))
					{
					    DerivedEventList.add(PreviousEvent);
					}
				    }
				    else
				    {
					//////////////////////////////////////////////////
					//For current zones which only current locations belong to, fire new open events
					//////////////////////////////////////////////////
					Event NewEvent=new Event(0,thisZone_czo_id,database_location_id, database_location_id,false,thisLOCRow.pTime,thisLOCRow.pTime);
					PreviousZoneEvents.put(thisZone_czo_id, NewEvent);

					DerivedEventList.add(NewEvent);		

				    }
				}
			    }
			    break;

			}

			/////////////////////////////////
			//Populate Derived Events
			/////////////////////////////////
			for (Event aEvent:DerivedEventList)
			{
			    PopulateEvent(DBConn,Ves_id,aEvent);
			}

			/////////////////////////////////
			//Update flag of last location
			/////////////////////////////////
			if (last_database_location_id!=-1)
			{
			    UpdateLastLocationFlag(DBConn, Ves_id,last_database_location_id);
			}


			/////////////////////////////////////////
			//Update modify timestamp
			/////////////////////////////////////////
			CallableStatement csSetTime = DBConn.prepareCall("begin vessel_load_pkg.set_change_timestamp_proc(?);  end;");

			try {
			    csSetTime.setLong(1, Ves_id);
			    csSetTime.execute();	
			    csSetTime.close();

			} catch (SQLException e) {
			    // TODO Auto-generated catch block
			    VTLocationLogger.warn(
				    "Failed to change Physical Asset modify_date for I"+ LR[0].sImo+" ship ID:"+LR[0].shipId , e);					
			}		


			DBConn.commit();
			VTLocationLogger.debug(
				"Vessel:  loaded current location event data successfully for "+ LR[0].sImo+" ship ID:"+LR[0].shipId);

		    }
		    catch (Exception e) {

			VTLocationLogger.warn(
				"Vessel:  loading current location event data failed" , e);
			e.printStackTrace();
		    }
		}


		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_load_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}	

		try{
		    CallableStatement callableClearSessionSM = DBConn.prepareCall("{call  vessel_fact_maintain_pkg.release_session_variables()}");
		    callableClearSessionSM.execute();
		    callableClearSessionSM.close();
		} catch (SQLException e) {
		    // TODO Auto-generated catch block
		    VTLocationLogger.warn("SQLException", e);					
		}

	    }
	    finally
	    {
		try {
		    DBConn.close();
		} catch (SQLException e) {
		    VTLocationLogger.warn("SQLException", e);	
		}					

		synchronized (VTLocationLoader.this)
		{
		    VTLocationLoader.this.ThreadCounter--;

		    if (VTLocationLoader.this.ThreadCounter==0)
		    {			
			VTLocationLoader.this.notify();
		    }
		}
	    }
	}
    }

    private void UpdateLastLocationFlag(Connection DBConn,long ves_id, long database_location_id)
    {
	CallableStatement callableeventSm=null;
	try {
	    callableeventSm = DBConn.prepareCall("{call vessel_load_pkg.update_lat_loc_flag_proc(?,?)}");
	    callableeventSm.setLong(1, ves_id);
	    callableeventSm.setLong(2, database_location_id);

	    callableeventSm.execute();

	} catch (SQLException e) {
	    VTLocationLogger.warn(
		    "Vessel:  Updaing last location flag failed for database location id: "+ database_location_id,  e);
	}finally
	{
	    try {
		callableeventSm.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		VTLocationLogger.warn("SQL Exception",  e);
	    }
	}
    }


    private VTLocationRow[] ReadLocationRow(long[] Positions)
    {
	try {
	    VTLocationRow[] LR= new VTLocationRow[Positions.length];
	    RandomAccessFile RAF=new RandomAccessFile(this.InZipLocFile,"r");

	    int i=0;			
	    for (long pos:Positions)
	    {
		RAF.seek(pos);
		String strLoc=RAF.readLine();
		LR[i]=new VTLocationRow(strLoc);
		i++;				
	    }

	    RAF.close();
	    return LR;

	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    VTLocationLogger.warn( "Can not locate location data in file", e);
	    return null;
	}
    }

    private VTLocationRow[] ReadHistoryRow(long[] Positions,String[] header)
    {
	try {
	    VTLocationRow[] LR= new VTLocationRow[Positions.length];
	    RandomAccessFile RAF=new RandomAccessFile(this.InZipLocFile,"r");

	    int i=0;			
	    for (long pos:Positions)
	    {
		RAF.seek(pos);
		String strLoc=RAF.readLine();
		LR[i]=new VTLocationRow(header, strLoc);
		i++;				
	    }

	    RAF.close();
	    return LR;

	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    VTLocationLogger.warn( "Can not locate location data in file", e);
	    return null;
	}
    }





    private ArrayList<Integer> LocateCurrentZone(VTLocationRow lr, String VesselProductType, ArrayList<Integer> czo_ids)
    {

	GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
	Coordinate coord = new Coordinate(lr.pLong, lr.pLat);
	Point point = geometryFactory.createPoint(coord);

	ArrayList<Integer> CurrentZones = new ArrayList<Integer>();

	Integer BelongedGlobalZoneIndex=null;

	for (int i=0 ; i<Zone.GlobalZones.length ; i++)
	{
	    if (Zone.GlobalZones[i].covers(point))
	    {
		BelongedGlobalZoneIndex=i;
		break;
	    }
	}


	for (Map.Entry<Integer, Zone> thisEntry : Zone.ZoneMap.entrySet()) {

	    Zone thisZone=thisEntry.getValue();

	    if (!czo_ids.contains(thisZone.getCzo_ID()))
	    {
		continue;
	    }

	    if (thisZone.getZone_type().equals("ZONE"))
	    {
		if (VesselProductType.equals("Tankers"))
		{
		    if (!thisZone.HasClassification("TANKER"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Bulkers"))
		{
		    if (!thisZone.HasClassification("DRY"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Container / Roro"))
		{
		    if (!thisZone.HasClassification("LINER"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Miscellaneous") || VesselProductType.equals("Passenger"))
		{
		    continue;
		}
	    }

	    if (thisZone.IntersectedWithGlobalZone(BelongedGlobalZoneIndex))
	    {
		if (thisZone.getPolygon().covers(point)) {
		    CurrentZones.add(thisZone.getCzo_ID());
		}	
	    }					
	}

	return CurrentZones;

    }

    private ArrayList<Integer> LocateCurrentZone(VTLocationRow lr, String VesselProductType)
    {
	GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
	Coordinate coord = new Coordinate(lr.pLong, lr.pLat);
	Point point = geometryFactory.createPoint(coord);

	ArrayList<Integer> CurrentZones = new ArrayList<Integer>();

	Integer BelongedGlobalZoneIndex=null;

	for (int i=0 ; i<Zone.GlobalZones.length ; i++)
	{
	    if (Zone.GlobalZones[i].covers(point))
	    {
		BelongedGlobalZoneIndex=i;
		break;
	    }
	}


	for (Map.Entry<Integer, Zone> thisEntry : Zone.ZoneMap.entrySet()) {

	    Zone thisZone=thisEntry.getValue();
	    if (thisZone.getZone_type().equals("ZONE"))
	    {
		if (VesselProductType.equals("Tankers"))
		{
		    if (!thisZone.HasClassification("TANKER"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Bulkers"))
		{
		    if (!thisZone.HasClassification("DRY"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Container / Roro"))
		{
		    if (!thisZone.HasClassification("LINER"))
		    {
			continue;
		    }
		}
		else if (VesselProductType.equals("Miscellaneous") || VesselProductType.equals("Passenger"))
		{
		    continue;
		}
	    }

	    if (thisZone.IntersectedWithGlobalZone(BelongedGlobalZoneIndex))
	    {
		if (thisZone.getPolygon().covers(point)) {
		    CurrentZones.add(thisZone.getCzo_ID());
		}	
	    }					
	}

	return CurrentZones;
    }	

    private long PopulateEvent(Connection DBConn,long ves_id, Event ev)
    {
	CallableStatement callableeventSm=null;
	long event_id=-1;

	try {
	    callableeventSm = DBConn.prepareCall("{? = call vessel_load_pkg.insert_his_event_fn(?,?,?,?,?,?)}");
	    callableeventSm.registerOutParameter(1, Types.DECIMAL);
	    callableeventSm.setLong(2, ev.EventID);
	    callableeventSm.setLong(3, ves_id);
	    callableeventSm.setLong(4, ev.EntrypointID);
	    callableeventSm.setLong(5, ev.CurrentpointID);
	    callableeventSm.setString(6, (ev.IsCloseEvent ? "Y":"N"));
	    callableeventSm.setInt(7, ev.Czo_id);

	    callableeventSm.execute();
	    event_id=callableeventSm.getLong(1);

	} catch (SQLException e) {
	    VTLocationLogger.warn(
		    "Vessel:  loading current event data failed for event row: "+ ev.toString(),  e);
	}finally
	{
	    try {
		callableeventSm.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		VTLocationLogger.warn("SQL Exception",  e);
	    }
	}
	return event_id;
    }

    private void UpdateLocationMark(Connection DBConn, long ves_id, String mark_type, String Marker)
    {
	CallableStatement objStatement=null;
	try {
	    objStatement = DBConn.prepareCall("{call vessel_load_pkg.update_location_mark_proc(?,?,?)}");
	    objStatement.setLong(1, ves_id);
	    objStatement.setString(2, Marker);
	    objStatement.setString(3, mark_type);
	    objStatement.execute();
	} catch (SQLException e) {
	    // TODO Auto-generated catch block			
	    VTLocationLogger.warn(
		    "Update speed/dud marker failed!", e);
	} finally
	{
	    try {
		objStatement.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		VTLocationLogger.warn("SQL Exception",  e);
	    }
	}

    }


    private long PopulateLocation(Connection DBConn, long ves_id, VTLocationRow thisLOCRow, boolean IsSpeedChange,boolean IsDud)
    {
	CallableStatement callablelocationSm=null;
	long location_id=-1;

	try {
	    callablelocationSm = DBConn.prepareCall("{ ? = call vessel_load_pkg.insert_his_location_fn(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
	    callablelocationSm.registerOutParameter(1, OracleTypes.DECIMAL);
	    callablelocationSm.setNull(2, OracleTypes.DECIMAL);
	    callablelocationSm.setLong(3, ves_id);
	    callablelocationSm.setTimestamp(4, thisLOCRow.pTime);
	    callablelocationSm.setTimestamp(5, thisLOCRow.vTime);

	    callablelocationSm.setTimestamp(6, thisLOCRow.vEta);

	    if (thisLOCRow.pHdg!=null)
	    {
		callablelocationSm.setDouble(7, thisLOCRow.pHdg);
	    }
	    else
	    {
		callablelocationSm.setNull(7, OracleTypes.DECIMAL);
	    }

	    callablelocationSm.setDouble(8, thisLOCRow.pLat);
	    callablelocationSm.setDouble(9, thisLOCRow.pLong);

	    if (thisLOCRow.pSpeed!=null)
	    {
		callablelocationSm.setDouble(10, thisLOCRow.pSpeed);
	    }
	    else
	    {
		callablelocationSm.setNull(10, OracleTypes.DECIMAL);
	    }


	    if (thisLOCRow.dit_status_id!=null && thisLOCRow.dit_status_id!=-1)
	    {
		callablelocationSm.setInt(11, thisLOCRow.dit_status_id);
	    }
	    else
	    {
		callablelocationSm.setNull(11, OracleTypes.DECIMAL);
	    }

	    callablelocationSm.setInt(12, thisLOCRow.dit_datasource_id);
	    callablelocationSm.setString(13, thisLOCRow.vDest);
	    callablelocationSm.setString(14, thisLOCRow.vDestCleaned);
	    callablelocationSm.setString(15, thisLOCRow.vDestLocode);
	    callablelocationSm.setString(16, thisLOCRow.sCallsign);

	    if (thisLOCRow.vDraught!=null)
	    {
		callablelocationSm.setDouble(17, thisLOCRow.vDraught);
	    }
	    else
	    {
		callablelocationSm.setNull(17, OracleTypes.DECIMAL);
	    }

	    callablelocationSm.setString(18, thisLOCRow.sName);

	    if (thisLOCRow.sLength!=null)
	    {
		callablelocationSm.setDouble(19, thisLOCRow.sLength);
	    }
	    else
	    {
		callablelocationSm.setNull(19, OracleTypes.DECIMAL);
	    }

	    if (thisLOCRow.sWidth!=null)
	    {
		callablelocationSm.setDouble(20, thisLOCRow.sWidth);
	    }
	    else
	    {
		callablelocationSm.setNull(20, OracleTypes.DECIMAL);
	    }

	    if (IsSpeedChange)
	    {
		callablelocationSm.setString(21, "Y");
	    }
	    else
	    {
		callablelocationSm.setString(21, "N");
	    }

	    if (IsDud)
	    {
		callablelocationSm.setString(22, "Y");
	    }
	    else
	    {
		callablelocationSm.setString(22, "N");
	    }

	    callablelocationSm.execute();

	    location_id=callablelocationSm.getLong(1);

	} catch (Exception e) {
	    VTLocationLogger.warn(
		    "Vessel:  loading current location data failed for location row: "+thisLOCRow.toString() , e);
	} finally
	{
	    try {
		callablelocationSm.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		VTLocationLogger.warn("SQL Exception",  e);
	    }
	}

	return location_id;
    }

    private boolean IsDudPoint(DBLocation l1, DBLocation l2)
    {
	try
	{
	    Coordinate start = new Coordinate(l1.pLong, l1.pLat);
	    Coordinate end = new Coordinate(l2.pLong, l2.pLat);

	    // orthodromicDistance start
	    double distance_in_knot = JTS.orthodromicDistance(start, end, CRS)/1852.0;
	    double duration=Math.abs(l2.recordTime.getTime()- l1.recordTime.getTime())/3600000.0;

	    double speed=distance_in_knot/duration;

	    if (speed>speedthrottle)
	    {
		return true;
	    }
	    else
	    {
		return false;
	    }

	}
	catch(Exception e)
	{
	    throw new SystemException(e);
	}
    }

    private boolean IsDudPoint(DBLocation l1, VTLocationRow l2)
    {
	try
	{
	    Coordinate start = new Coordinate(l1.pLong, l1.pLat);
	    Coordinate end = new Coordinate(l2.pLong, l2.pLat);

	    // orthodromicDistance start
	    double distance_in_knot = JTS.orthodromicDistance(start, end, CRS)/1852.0;
	    double duration=Math.abs(l2.pTime.getTime()- l1.recordTime.getTime())/3600000.0;

	    double speed=distance_in_knot/duration;

	    if (speed>speedthrottle)
	    {
		return true;
	    }
	    else
	    {
		return false;
	    }

	}
	catch(Exception e)
	{
	    throw new SystemException(e);
	}
    }

    private boolean IsDudPoint(VTLocationRow l1,DBLocation l2)
    {
	try
	{
	    Coordinate start = new Coordinate(l1.pLong, l1.pLat);
	    Coordinate end = new Coordinate(l2.pLong, l2.pLat);

	    // orthodromicDistance start
	    double distance_in_knot = JTS.orthodromicDistance(start, end, CRS)/1852.0;
	    double duration=Math.abs(l2.recordTime.getTime()- l1.pTime.getTime())/3600000.0;

	    double speed=distance_in_knot/duration;

	    if (speed>speedthrottle)
	    {
		return true;
	    }
	    else
	    {
		return false;
	    }

	}
	catch(Exception e)
	{
	    throw new SystemException(e);
	}
    }

    private boolean IsDudPoint(VTLocationRow l1, VTLocationRow l2)
    {
	try
	{
	    Coordinate start = new Coordinate(l1.pLong, l1.pLat);
	    Coordinate end = new Coordinate(l2.pLong, l2.pLat);

	    // orthodromicDistance start
	    double distance_in_knot = JTS.orthodromicDistance(start, end, CRS)/1852.0;
	    double duration=Math.abs(l2.pTime.getTime()- l1.pTime.getTime())/3600000.0;

	    double speed=distance_in_knot/duration;

	    if (speed>speedthrottle)
	    {
		return true;
	    }
	    else
	    {
		return false;
	    }

	}
	catch(Exception e)
	{
	    return true;
	}
    }

    private void GenerateRicFile()
    {
	//Create a text file to RIC for monitoring
	try {
	    String ricfilename="VTLocation_SDI.Incremental";
	    BufferedWriter out = new BufferedWriter(new FileWriter(new File(ricfolder,
		    ricfilename)));
	    out.write(VTLocationStatus);
	    out.write(System.getProperty("line.separator"));
	    out.write(SDIStatus);
	    out.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
		VTLocationLogger.error("RIC file error!",e);
	}
    }

    private String getDBDate()
    {
	Connection DBConn = new EasyConnection(DBConnNames.CEF_CNR);
	PreparedStatement objPreStatement = null;
	ResultSet objResult = null;

	try {
	    ////////////////////////////////////////////////////////////////////////
	    //get attributes
	    objPreStatement = DBConn.prepareStatement("select systimestamp from dual");
	    objResult = objPreStatement.executeQuery();

	    objResult.next();
	    Timestamp DBDate = objResult.getTimestamp(1);

	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH_mm_ss_SSS'Z'"); 
	    String strDBDate = formatter.format(DBDate);

	    objResult.close();
	    objPreStatement.close();

	    return strDBDate;

	} catch (SQLException e) {
	    VTLocationLogger.warn("SQL Exception",  e);
	    return null;

	} finally
	{
	    try {
		DBConn.close();
	    } catch (SQLException e) {
		// TODO Auto-generated catch block
		VTLocationLogger.warn("SQL Exception",  e);
	    }		
	}
    }
    

}
