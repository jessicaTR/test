package com.thomsonreuters.ce.dbor.pasdi.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.Date;

import com.thomsonreuters.ce.queue.Pipe;


import com.thomsonreuters.ce.dbor.pasdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.SDICursorRow;
import com.thomsonreuters.ce.dbor.pasdi.util.Counter;
import com.thomsonreuters.ce.dbor.pasdi.SDIConstants;

public abstract class GZIPSDIFileGenerator {

	private int ActiveNum = 0;
	private int ThreadNum;
	private Pipe<HashMap<CursorType, SDICursorRow[]>> InQ;

	protected String Location;
	protected String FileName;

	protected Date CoverageStart;
	protected Date CoverageEnd;

	private BufferedWriter bw = null;
	private Counter count;
	private boolean IsEmpty = true;

	protected boolean isProd = true;

	public GZIPSDIFileGenerator(String Loc, String FN, Date CS, Date CE,
			int Thread_Num, Pipe<HashMap<CursorType, SDICursorRow[]>> InPort,
			Counter ct) {
		this.Location = Loc;
		this.FileName = FN;
		this.CoverageStart = CS;
		this.CoverageEnd = CE;

		this.ThreadNum = Thread_Num;
		this.InQ = InPort;
		this.count = ct;

		this.count.Increase();
	}

	public boolean IsFullLoad() {
		if (CoverageStart == null) {
			return true;
		} else {
			return false;
		}
	}

	public Date getCoverageEnd() {
		return CoverageEnd;
	}

	public Date getCoverageStart() {
		return CoverageStart;
	}

	public String getAction(Date EntityCreateDate) {
		if (CoverageStart == null) {
			return "Insert";
		} else if (EntityCreateDate.after(CoverageStart)) {
			return "Insert";
		} else {
			return "Overwrite";
		}
	}

	public final void Start() {

		File thisTempFile = new File(this.Location, this.FileName + ".temp");

		// Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}

		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(
					thisTempFile));
			bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));

			bw.write(getHeader());
			bw.flush();

			if (isProd) {
				Thread[] ThreadArray = new Thread[this.ThreadNum];
				for (int i = 0; i < this.ThreadNum; i++) {
					ThreadArray[i] = new Thread(new Worker());
				}

				for (int i = 0; i < this.ThreadNum; i++) {
					ThreadArray[i].start();
				}
			} else {
				(new Worker()).run();
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "FileNotFoundException", e);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "UnsupportedEncodingException", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "IOException", e);
		}

	}

	private void WriteContentItem(String CI) {
		synchronized (GZIPSDIFileGenerator.this) {
			try {
				if (!CI.equals("")) {
					this.IsEmpty = false;
				}
				bw.write(CI);
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				SDIConstants.SDILogger.error( "IOException", e);
			}
		}

	}

	private void WriteFooterAndCloseFile() {
		try {
			if (this.IsEmpty) {
				this.bw.write(getDefaultElement());
			}
			this.bw.write(getFooter());
			this.bw.flush();
			this.bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "IOException", e);
		}

		File thisTempFile = new File(this.Location, this.FileName + ".temp");
		File thisFile = new File(this.Location, this.FileName);

		// Delete temp file if it exists
		if (thisFile.exists() == true) {
			thisFile.delete();
		}

		this.MoveFile(thisTempFile, thisFile);
	}

	protected abstract String getHeader();

	protected abstract String getFooter();

	protected abstract String convertContentItem(
			final Map<CursorType, SDICursorRow[]> hmCTypeRows);

	public void MoveFile(File f1, File f2) {

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
				} else {
					out.write(buffer, 0, ins);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "FileNotFoundException", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			SDIConstants.SDILogger.error( "IOException", e);
		}

	}

	private class Worker implements Runnable {

		public Worker() {
			synchronized (GZIPSDIFileGenerator.this) {
				GZIPSDIFileGenerator.this.ActiveNum++;
			}
		}

		public void run() {

			try {
				while (true) {

					HashMap<CursorType, SDICursorRow[]> SCR = GZIPSDIFileGenerator.this.InQ
							.getObj();
					if (SCR != null) {
						try {
							GZIPSDIFileGenerator.this
									.WriteContentItem(convertContentItem(SCR));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							SDIConstants.SDILogger.error(
									"Unknown Exception", e);
						}
					} else {
						break;
					}

				}
			} finally {

				synchronized (GZIPSDIFileGenerator.this) {
					GZIPSDIFileGenerator.this.ActiveNum--;

					if (GZIPSDIFileGenerator.this.ActiveNum == 0) {
						try {
							GZIPSDIFileGenerator.this.WriteFooterAndCloseFile();
						} finally {
							SDIConstants.SDILogger.info( "SDI File: "
									+ GZIPSDIFileGenerator.this.FileName
									+ " has been generated");
							GZIPSDIFileGenerator.this.count.Decrease();
						}
					}

				}
			}

		}
	}

	public abstract String getDefaultElement();

}
