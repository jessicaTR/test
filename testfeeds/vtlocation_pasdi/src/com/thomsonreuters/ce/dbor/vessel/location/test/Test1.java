package com.thomsonreuters.ce.dbor.vessel.location.test;

import org.geotools.factory.FactoryRegistryException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.WKTReader2;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class Test1 {

    public static void main(String[] args) {
	// TODO Auto-generated method stub

	
	try {
	    GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
	    Coordinate coord = new Coordinate(39.7391816667, -16.62559);
	    Point point = geometryFactory.createPoint(coord);
	    
	    String WKT="POLYGON ((39.78379167555619 -16.60185032981765, 39.73311054675466 -16.55743595339145, 39.67002746591979 -16.599551826278, 39.6789587854195 -16.63555180192666, 39.72207748295831 -16.64260946590581, 39.78379167555619 -16.60185032981765))";
	    WKTReader2 reader = new WKTReader2(geometryFactory);
	    Geometry polygon = reader.read(WKT);
	    
	    if (polygon.covers(point))
	    {
		System.out.println("true");
	    }
	    else
	    {
		System.out.println("false");
	    }
	    
	} catch (FactoryRegistryException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (ParseException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	
		
    }

}
