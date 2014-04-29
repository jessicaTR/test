package com.thomsonreuters.ce.queue;

public class DiskCacheTest {

    public static void main(String[] Args) throws Exception
    {

	DiskCacheWriter<String> DW= new DiskCacheWriter<String>("D:\\temp", "Alerting1");

	DiskCacheTest DCT=new DiskCacheTest();
	
	for (int i=0 ; i<10 ;i++)
	{
	    Inputer In=new Inputer(DW, DCT);
	    new Thread(In).start();
	}

	System.out.println(DCT.Incounter);

	DW.Finish();

	DiskCacheReader<String> DR= new DiskCacheReader<String>("D:\\temp", "Alerting1");

	DR.Reset();

	for (int i=0 ; i<10 ;i++)
	{
	    Outputer Out=new Outputer(DR,DCT);
	    new Thread(Out).start();
	}

	System.out.println(DCT.Outcounter);
	DR.Delete();
    }

    public int Incounter=0;

    public int Outcounter=0;


}

class Outputer implements Runnable
{
    DiskCacheReader<String> DCFMT=null;

    DiskCacheTest dct=null;
    
    Outputer(DiskCacheReader<String> dcfmt, DiskCacheTest DCT )
    {
	DCFMT=dcfmt;
	dct=DCT;
    }

    public void run() {

	while(true)
	{

	    try {
		String x=DCFMT.GetNext();

		if (x!=null)
		{
		    synchronized(DCFMT)
		    {
			dct.Outcounter++;
		    }

		    System.out.println("Get:"+x);
		}
		else
		{
		    return;
		}
	    } catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}
    }

}

class Inputer implements Runnable
{

    DiskCacheWriter<String> DCFMT=null;
    
    DiskCacheTest dct=null;

    Inputer(DiskCacheWriter<String> dcfmt, DiskCacheTest DCT)
    {
	DCFMT=dcfmt;
	dct=DCT;
    }

    @Override
    public void run() {
	// TODO Auto-generated method stub

	while(true)
	{
	    synchronized(DCFMT)
	    {
		try {
		    if (dct.Incounter<100000)
		    {

			DCFMT.Append(String.valueOf(dct.Incounter));

			System.out.println(String.valueOf(dct.Incounter) + "has been added to cache" );
			dct.Incounter++;
		    }
		    else
		    {
			return;
		    }
		} catch (Exception e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	}
    }
}
