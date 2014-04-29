package com.thomsonreuters.ce.dbor.tools;

import java.io.Writer;
import java.sql.Connection;

import oracle.sql.CLOB;
/***
 * This class is to pass CLOB parameter to Oracle procedure. 
 * It's used in ZoneLoader class to pass polygon to oracle.
 * If use setCharacterStream to pass CLOB, 
 * the trigger in oracle can't be fired when insert/update. 
 * @author j.li
 *
 */

public class OracleHelper {
   @SuppressWarnings("deprecation")
   public static CLOB getCLOB(Connection conn,String clobData )    throws Exception
   {
       CLOB tempClob = null;

//        try
//        {
           //  create a new temporary CLOB
           tempClob = CLOB.createTemporary( conn, true, CLOB.DURATION_SESSION );
   
           // Open the temporary CLOB in readwrite mode to enable writing
           tempClob.open( CLOB.MODE_READWRITE );
   
   
           // Get the output stream to write
           Writer tempClobWriter = tempClob.getCharacterOutputStream();
   
           // Write the data into the temporary CLOB
           tempClobWriter.write( clobData );
   
           // Flush and close the stream
           tempClobWriter.flush(  );
           tempClobWriter.close(  );
   
           // Close the temporary CLOB
           tempClob.close( );

       /*}
       catch ( Exception exp )
       {
                // Free CLOB object
           tempClob.freeTemporary( );
       }*/
       return tempClob;
   }
}