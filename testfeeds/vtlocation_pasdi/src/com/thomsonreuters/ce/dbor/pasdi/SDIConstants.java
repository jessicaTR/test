package com.thomsonreuters.ce.dbor.pasdi;

import org.apache.log4j.Logger;

public class SDIConstants {
	public static final String ManiFest_Prefix = "CommodityPhysicalAssets.done.All.2.";
	public static final String CommodityPhysicalAsset_Prefix = "CommodityPhysicalAssets.CommodityPhysicalAsset.All.2.";
	public static final String PhysicalAssetIdentifier_Prefix = "CommodityPhysicalAssets.Identifier.All.2.";
	public static final String PlantAsset_Prefix = "CommodityPhysicalAssets.PlantAsset.All.2.";
	public static final String VesselAsset_Prefix = "CommodityPhysicalAssets.VesselAsset.All.2.";
	public static final String Editorial_Prefix = "CommodityPhysicalAssets.Editorial.All.2.";
	public static final String GenericAssetMetaData_Prefix="CommodityPhysicalAssets.GenericAssetMetadata.All.2.";
	public static final String GenericAsset_Prefix="CommodityPhysicalAssets.GenericAsset.All.2.";
	public static final String GenericAssetGroup_Prefix="CommodityPhysicalAssets.GenericAssetGroup.All.2.";
	public static final String Relationship_Prefix="CommodityPhysicalAssets.Relationship.All.4.";
	
	public static final String PhysicalAssetIdentifier_Prefix_v4 = "CommodityPhysicalAssets.Identifier.All.4.";
	public static final String ManiFest_Prefix_Inventory = "CommodityPhysicalAssets.2.";
	
	public final static String DATE_FORMAT_SSS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public final static String PHYSICAL_XSLT = "../cfg/pasdi/CommodityPhysicalAsset.xslt";
	public final static String PLANT_XSLT = "../cfg/pasdi/PlantAsset.xslt";
	public final static String VESSEL_XSLT = "../cfg/pasdi/VesselAsset.xslt";
	
	public final static Logger SDILogger=Logger.getLogger("pasdi");
	
	public static String tempfolder;
	
	public enum AssetName {
		FULL_NAME("FULL NAME"), ALIAS("ALIAS"), EX_NAME("EX NAME");

		private String displayName;

		AssetName(String displayName) {
			this.displayName = displayName;
		}

		public String displayName() {
			return displayName;
		}

		// Optionally and/or additionally, toString.
		@Override
		public String toString() {
			return displayName;
		}
	}

}
