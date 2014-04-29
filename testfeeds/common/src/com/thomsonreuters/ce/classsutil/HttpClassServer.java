package com.thomsonreuters.ce.classsutil;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class HttpClassServer implements Runnable {

	private static HttpClassServer thisInstance = null;

	private static Object Lock = new Object();

	private ServerSocket server = null;

	private InetAddress IPAddress = null;

	private int PortNum = 80;

	private HttpClassServer(String HostName, int port) {

		try {
			this.IPAddress = InetAddress.getByName(HostName);
			this.PortNum = port;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			throw new ClassServerException(e);
		}
	}

	public static void Start(String HostName, int port) {
		synchronized (Lock) {
			if (thisInstance != null) {
				thisInstance.End();
			}

			thisInstance = new HttpClassServer(HostName, port);
			thisInstance.Begin();
		}

	}

	public static void Stop() {
		synchronized (Lock) {
			if (thisInstance != null) {
				thisInstance.End();
				thisInstance = null;
			}
		}
	}

	private void Begin() {
		try {
			server = new ServerSocket(this.PortNum, 50, this.IPAddress);
			System.setProperty("java.rmi.server.codebase", "http://".concat(
					server.getInetAddress().getHostAddress()).concat(":")
					.concat(String.valueOf(server.getLocalPort())).concat("/"));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new ClassServerException(e);
		}

		newListener();
	}

	private void End() {
		if (this.server != null) {
			try {
				this.server.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new ClassServerException(e);
			}
		}
	}

	public void run() {

		Socket socket = null;
		DataOutputStream out = null;
		InputStream ClassByteInput = null;

		try {
			try {
				socket = this.server.accept();

				newListener();

				out = new DataOutputStream(socket.getOutputStream());
				BufferedReader in = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				String ClassName = getClassName(in);

				ClassByteInput = getClassInputStream(ClassName);

				out.writeBytes("HTTP/1.1 200 OK\r\n");
				out.writeBytes("Content-Type: application/java\r\n");

				ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();

				byte[] buf = new byte[1024];
				int byteNum = -1;

				while ((byteNum = ClassByteInput.read(buf)) != -1) {
					outputBytes.write(buf, 0, byteNum);
				}
				ClassByteInput.close();

				byte[] ClassBytes = outputBytes.toByteArray();
				out.writeBytes("Content-Length: " + ClassBytes.length
						+ "\r\n\r\n");
				out.write(ClassBytes);
				out.flush();

			} catch (ClassServerException e) {

				out.writeBytes("HTTP/1.1 404 Class can not be located!\r\n");
				out.writeBytes("Content-Type: text/html\r\n");

				String html = "<html><body>".concat(e.getMessage()).concat(
						"</body></html>");
				out.writeBytes("Content-Length: " + html.length() + "\r\n\r\n");
				out.writeBytes(html);
				out.writeBytes("\r\n");
				out.flush();
			}

			out.flush();
							
		} catch (IOException e) {
			if (!this.server.isClosed())
			{
				throw new ClassServerException(e);
			}
		} finally {
			try {
				if (ClassByteInput != null) {
					ClassByteInput.close();
				}

				if (socket != null) {
					socket.close();
				}
			} catch (IOException e) {
				throw new ClassServerException(e);
			}
		}
	}

	private void newListener() {
		(new Thread(this)).start();
	}

	private static String getClassName(BufferedReader in) throws IOException {
		String line = in.readLine();

		if (line.startsWith("GET /")) {
			line = line.substring(5).trim();
			int index = line.indexOf(" ");
			if (index != -1) {
				line = line.substring(0, index);
			}

			if (line.endsWith(".class")) {
				line = line.substring(0, line.lastIndexOf(".class"));
			}

			return line.replace('/', '.');
			
		} else {
			throw new ClassServerException("Malformed Http Header");
		}
	}

	public static void main(String[] args) {

		HttpClassServer.Start("localhost", 8080);
		HttpClassServer.Stop();
		HttpClassServer.Start("localhost", 80);

	}

	private InputStream getClassInputStream(String clsAsResource) {
		URL result = null;

		Class cls = null;

		try {
			cls = Class.forName(clsAsResource);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			throw new ClassServerException("Class Not Found!");
		}

		ProtectionDomain pd = cls.getProtectionDomain();
		InputStream ClassIStream = null;

		if (pd != null) {
			CodeSource cs = pd.getCodeSource();

			if (cs != null) {
				result = cs.getLocation();

				if (result != null) {

					clsAsResource = clsAsResource.replace('.', '/');
					clsAsResource = clsAsResource.concat(".class");

					if ("file".equals(result.getProtocol())) {

						try {
							if (result.toExternalForm().endsWith(".jar")
									|| result.toExternalForm().endsWith(".zip")) {
								// in the jar file
								ZipFile jarFile = new ZipFile(result
										.toExternalForm().substring(6));
								ZipEntry ClassEntry = jarFile
										.getEntry(clsAsResource);
								ClassIStream = jarFile
										.getInputStream(ClassEntry);

							} else {
								// in some directory
								ClassIStream = new FileInputStream(result
										.toExternalForm().substring(6).concat(
												clsAsResource));
							}
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							throw new ClassServerException(e);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							throw new ClassServerException(e);
						}
					}
				}

			} else {
				throw new ClassServerException(
						"System class can not be located!");
			}
		}

		return ClassIStream;
	}

}