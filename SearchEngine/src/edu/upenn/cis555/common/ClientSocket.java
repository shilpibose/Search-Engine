package edu.upenn.cis555.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Represents a client socket and sets up the input and output streams for it.
 */
public class ClientSocket {

	public Socket socket = null;
	public String clientName = null;
	public String serverName = null;
	public String clientIP = null;
	public String serverIP = null;
	public BufferedReader in;
	public PrintWriter out;
	public OutputStream binaryOutputStream;

	public ClientSocket (Socket socket) throws IOException {
		this.socket = socket;
		clientName = socket.getInetAddress ().getHostName ();
		serverName = InetAddress.getLocalHost ().getHostName ();
		clientIP = socket.getInetAddress ().getHostAddress ();
		serverIP = InetAddress.getLocalHost ().getHostAddress ();
		in = new BufferedReader (new InputStreamReader (this.socket
				.getInputStream ()));
		out = new PrintWriter (this.socket.getOutputStream (), true);
		binaryOutputStream = this.socket.getOutputStream ();
	}

	/**
	 * Method to read a line on the socket
	 */
	public String readLine () throws IOException {
		String line = null;
		line = in.readLine ();
		System.err.println ("From: " + clientIP + " READ LINE :  " + line);

		if (line == null) {
			throw new IOException ();
		}
		return line;
	}

	/**
	 * Method to read only a character. Used while reading HTTP Request content
	 */
	public int readChar () throws IOException {
		return in.read ();
	}

	/**
	 * Method to write a line into the socket
	 */
	public void writeLine (String message) {
		System.err.println ("From: " + clientIP + " WRITE LINE: " + message);
		out.println (message);
	}

	/**
	 * Method to write a line into the socket
	 */
	public void writeChar (char c) {
		System.err.print (c);
		out.print (c);
	}

	public void close () {
		try {
			if (! (socket.isClosed ()))
				socket.close ();
		} catch (IOException e) {
		}
	}

	@Override
	protected void finalize () throws Throwable {
		super.finalize ();
		close ();
	}
}
