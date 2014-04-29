package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import com.thomsonreuters.ce.dbor.pasdi.SDIPreLoadCache;
import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;

public class AssetZoom {

	private String AssetZoomRegions = null;

	private String AssetZoomRegionsRCS = null;

	private String AssetZoomRegionsBoundsLat = null;

	private String AssetZoomRegionsBoundsLon = null;

	private boolean exist = true;

	public AssetZoom(Map<CursorType, SDICursorRow[]> HM, SDIPreLoadCache SDIPLC) {
		SDICursorRow[] SCR = HM.get(CursorType.AST_BASE_INFO);
		String CommodityType = ((AstBase) SCR[0]).getPas_type();

		if (CommodityType != null) {
			if (CommodityType.equals("VESSEL")) {
				SDICursorRow[] VesselOpenEventArray = HM
						.get(CursorType.VESSEL_OPEN_EVENT_INFO);

				if (VesselOpenEventArray == null) {
					this.exist = false;
					return;
				}
				VesselEventOriginDestination[] OrderedVOE = new VesselEventOriginDestination[SDIPLC.VesselZoomCfgArray
						.size()];

				Iterator<ArrayList<String>> it1 = SDIPLC.VesselZoomCfgArray
						.iterator();
				int level = 0;

				while (it1.hasNext()) {
					OrderedVOE[level] = null;

					ArrayList<String> Level_event_array = it1.next();
					Iterator<String> it2 = Level_event_array.iterator();

					boolean HasFound = false;
					while (it2.hasNext() && !HasFound) {
						String event_type = it2.next();

						for (int i = 0; i < VesselOpenEventArray.length; i++) {
							VesselEventOriginDestination VOE = (VesselEventOriginDestination) VesselOpenEventArray[i];
							if (VOE.getZone_type().equals(event_type)) {
								OrderedVOE[level] = VOE;
								HasFound = true;
								break;
							}
						}
					}

					level++;
				}

				// getAssetZoomRegions

				for (VesselEventOriginDestination voe : OrderedVOE) {
					if (voe != null) {
						
						UniverseVesZoneDetail UVZD=SDIPLC.getVesselZoneDetail(voe.getZone_id(), voe.getZone_type());

						String Zone_name = UVZD.getZone_name();
						if (Zone_name == null) {
							Zone_name = "";
						}

						String Zone_code =UVZD.getZone_code();
						if (Zone_code == null) {
							Zone_code = "";
						}

						Float Ln = UVZD.getLatitude_north();
						String Latitude_north;
						if (Ln == null) {
							Latitude_north = "";
						} else {
							Latitude_north = Ln.toString();
						}

						Float Ls = UVZD.getLatitude_south();
						String Latitude_south;
						if (Ls == null) {
							Latitude_south = "";
						} else {
							Latitude_south = Ls.toString();
						}

						Float Le = UVZD.getLongitude_east();
						String Longitude_east;
						if (Le == null) {
							Longitude_east = "";
						} else {
							Longitude_east = Le.toString();
						}

						Float Lw = UVZD.getLongitude_west();
						String Longitude_west;
						if (Lw == null) {
							Longitude_west = "";
						} else {
							Longitude_west = Lw.toString();
						}

						if (AssetZoomRegions != null) {
							AssetZoomRegions = AssetZoomRegions + "\\"
									+ Zone_name;
							AssetZoomRegionsRCS = AssetZoomRegionsRCS + "\\"
									+ Zone_code;
							AssetZoomRegionsBoundsLat = AssetZoomRegionsBoundsLat
									+ "\\"
									+ Latitude_north
									+ "|"
									+ Latitude_south;
							AssetZoomRegionsBoundsLon = AssetZoomRegionsBoundsLon
									+ "\\"
									+ Longitude_east
									+ "|"
									+ Longitude_west;
						} else {
							AssetZoomRegions = Zone_name;
							AssetZoomRegionsRCS = Zone_code;
							AssetZoomRegionsBoundsLat = Latitude_north + "|"
									+ Latitude_south;
							AssetZoomRegionsBoundsLon = Longitude_east + "|"
									+ Longitude_west;
						}

					} else {

						if (AssetZoomRegions != null)

						{
							AssetZoomRegions = AssetZoomRegions + "\\";
							AssetZoomRegionsRCS = AssetZoomRegionsRCS + "\\";
							AssetZoomRegionsBoundsLat = AssetZoomRegionsBoundsLat
									+ "\\|";
							AssetZoomRegionsBoundsLon = AssetZoomRegionsBoundsLon
									+ "\\|";

						} else {
							AssetZoomRegions = "";
							AssetZoomRegionsRCS = "";
							AssetZoomRegionsBoundsLat = "|";
							AssetZoomRegionsBoundsLon = "|";

						}

					}
				}
			} else if (CommodityType.equals("PLANT")
					|| CommodityType.equals("POWER")
					|| CommodityType.equals("AGRICULTURE")
					|| CommodityType.equals("PORT")
					|| CommodityType.equals("ANCHORAGE")
					|| CommodityType.equals("BERTH")) {

				SDICursorRow[] AC = HM.get(CursorType.AST_COORDINATE_INFO);
				if (AC == null) {
					this.exist = false;
					return;
				} else if ((((AstCoordinate) AC[0]).getLatitude() == null)
						|| (((AstCoordinate) AC[0]).getLongitude() == null)) {
					this.exist = false;
					return;
				}

				SDICursorRow[] AL = HM.get(CursorType.AST_LOCATION_INFO);

				if (AL == null) {
					this.exist = false;
					return;
				}

				Long gun_id = ((AstLocation) AL[0]).getGun_id();
				Integer Hierarchy_classification_id = ((AstLocation) AL[0])
						.getHierarchy_classification_id();

				if (gun_id == null || Hierarchy_classification_id == null) {
					this.exist = false;
					return;
				}

				Geographic_Unit[] GUList = new Geographic_Unit[SDIPLC.ZoomLevelCount];

				Geographic_Hierarchy GH = SDIPLC.universe_gun_detail_info.get(
						gun_id).getGeographic_Hierarchy(
						Hierarchy_classification_id);

				if (GH == null) {
					this.exist = false;
					return;
				}

				String gun_id_hierarchy = GH.getGun_id_hierarchy().substring(1);
				String getZoom_level_hierarchy = GH.getZoom_level_hierarchy()
						.substring(1);

				String[] gun_ids = gun_id_hierarchy.split("\\|", -1);
				String[] zoom_levels = getZoom_level_hierarchy.split("\\|", -1);

				for (int i = 0; i < gun_ids.length; i++) {
					Long gid = Long.valueOf(gun_ids[i]);
					String posstr = zoom_levels[i];

					if (!posstr.equals("")) {
						int pos = Integer.parseInt(posstr);
						GUList[pos - 1] = SDIPLC.universe_gun_detail_info
								.get(gid);
					}
				}

				Geographic_Unit tempGU = null;

				for (int i = 0; i < GUList.length; i++) {
					if (GUList[i] == null) {
						GUList[i] = tempGU;
					} else {
						tempGU = GUList[i];
					}
				}

				for (Geographic_Unit gu : GUList) {
					if (gu != null) {

						String Gun_desc = gu.getGun_desc();
						if (Gun_desc == null) {
							Gun_desc = "";
						}

						String Rcs_code = gu.getRcs_code();
						if (Rcs_code == null) {
							Rcs_code = "";
						}

						Float Ln = gu.getLatitude_north();
						String Latitude_north;
						if (Ln == null) {
							Latitude_north = "";
						} else {
							Latitude_north = Ln.toString();
						}

						Float Ls = gu.getLatitude_south();
						String Latitude_south;
						if (Ls == null) {
							Latitude_south = "";
						} else {
							Latitude_south = Ls.toString();
						}

						Float Le = gu.getLongitude_east();
						String Longitude_east;
						if (Le == null) {
							Longitude_east = "";
						} else {
							Longitude_east = Le.toString();
						}

						Float Lw = gu.getLongitude_west();
						String Longitude_west;
						if (Lw == null) {
							Longitude_west = "";
						} else {
							Longitude_west = Lw.toString();
						}

						if (AssetZoomRegions != null) {
							AssetZoomRegions = AssetZoomRegions + "\\"
									+ Gun_desc;
							AssetZoomRegionsRCS = AssetZoomRegionsRCS + "\\"
									+ Rcs_code;
							AssetZoomRegionsBoundsLat = AssetZoomRegionsBoundsLat
									+ "\\"
									+ Latitude_north
									+ "|"
									+ Latitude_south;
							AssetZoomRegionsBoundsLon = AssetZoomRegionsBoundsLon
									+ "\\"
									+ Longitude_east
									+ "|"
									+ Longitude_west;
						} else {
							AssetZoomRegions = Gun_desc;
							AssetZoomRegionsRCS = Rcs_code;
							AssetZoomRegionsBoundsLat = Latitude_north + "|"
									+ Latitude_south;
							AssetZoomRegionsBoundsLon = Longitude_east + "|"
									+ Longitude_west;

						}

					} else {
						if (AssetZoomRegions != null)

						{
							AssetZoomRegions = AssetZoomRegions + "\\";
							AssetZoomRegionsRCS = AssetZoomRegionsRCS + "\\";
							AssetZoomRegionsBoundsLat = AssetZoomRegionsBoundsLat
									+ "\\|";
							AssetZoomRegionsBoundsLon = AssetZoomRegionsBoundsLon
									+ "\\|";
						} else {
							AssetZoomRegions = "";
							AssetZoomRegionsRCS = "";
							AssetZoomRegionsBoundsLat = "|";
							AssetZoomRegionsBoundsLon = "|";
						}
					}
				}
			}
		}

		if (AssetZoomRegions != null) {
			if (AssetZoomRegions.equals("\\\\\\\\\\")) {
				this.exist = false;
			}
		} else {
			this.exist = false;
		}

	}

	public boolean Exists() {
		return exist;
	}

	public String getAssetZoomRegions() {
		return AssetZoomRegions;
	}

	public String getAssetZoomRegionsBoundsLat() {
		return AssetZoomRegionsBoundsLat;
	}

	public String getAssetZoomRegionsBoundsLon() {
		return AssetZoomRegionsBoundsLon;
	}

	public String getAssetZoomRegionsRCS() {
		return AssetZoomRegionsRCS;
	}

	public Geographic_Unit[] getGeographicUnitsUptoCountry(Geographic_Unit GU,
			int hierarchy_classification_id, SDIPreLoadCache SDIPLC) {
		Geographic_Hierarchy GH = GU
				.getGeographic_Hierarchy(hierarchy_classification_id);

		if (GH != null) {
			String gun_id_hierarchy = GH.getGun_id_hierarchy().substring(1);
			String[] gun_ids = gun_id_hierarchy.split("\\|", -1);

			for (int i = 0; i < gun_ids.length; i++) {
				String gun_id = gun_ids[i];
				Long g_id = Long.valueOf(gun_id);
				Geographic_Unit gu = SDIPLC.universe_gun_detail_info.get(g_id);
				if (gu.getGuc_desc().equals("Country")) {
					Geographic_Unit[] gus = new Geographic_Unit[gun_ids.length
							- i];
					for (int j = i; j < gun_ids.length; j++) {
						Geographic_Unit temp_gu = SDIPLC.universe_gun_detail_info
								.get(Long.valueOf(gun_ids[j]));
						gus[j - i] = temp_gu;
					}
					return gus;
				}
			}

		}

		return null;

	}

}
