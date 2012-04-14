package edu.upenn.cis555.crawler.distributed.constants;

import java.util.HashMap;

public class CrawlerConstants {
	
	public static final int PRIORITY_MAX = 10;
		

	public static final HashMap<String,String> CONTENT_TYPE = new HashMap<String,String>();
	public static final String TYPE_SOAP ="application/soap+xml";
	public static final String EXT_BMP = "bmp";
	public static final String TYPE_BMP= "image/bmp";
	public static final String EXT_CSS ="css";
	public static final String TYPE_CSS = "text/css";
	public static final String EXT_DOC="doc";
	public static final String TYPE_DOC = "application/msword";
	public static final String EXT_GIF="gif";
	public static final String TYPE_GIF = "image/gif";
	public static final String EXT_EXE="exe";
	public static final String TYPE_EXE = "application/octet-stream";
	public static final String EXT_JPEG ="jpeg";
	public static final String TYPE_JPEG ="image/jpeg";
	public static final String EXT_JPG ="jpg";
	public static final String TYPE_JPG ="image/jpeg";
	public static final String EXT_LATEX ="latex";
	public static final String TYPE_LATEX ="application/x-latex";
	public static final String EXT_MP3 ="mp3";
	public static final String TYPE_MP3 ="audio/mpeg";
	public static final String EXT_MPEG ="mpeg";
	public static final String TYPE_MPEG ="video/mpeg";
	public static final String EXT_MPG ="mpg";
	public static final String TYPE_MPG ="video/mpeg";
	public static final String EXT_PDF ="pdf";
	public static final String TYPE_PDF ="application/pdf";
	public static final String EXT_PPT ="ppt";
	public static final String TYPE_PPT ="application/vnd.ms-powerpoint";
	public static final String EXT_QT ="qt";
	public static final String TYPE_QT ="video/quicktime";
	public static final String EXT_RTF="rtf";
	public static final String TYPE_RTF="application/rtf";
	
	public static final String EXT_SWF="swf";
	public static final String TYPE_SWF = "application/x-shockwave-flash";
	public static final String EXT_TXT="txt";
	public static final String TYPE_TXT="text/plain";
	public static final String EXT_WAV="wav";
	public static final String TYPE_WAV="audio/x-wav";
	public static final String EXT_XLS ="xls";
	public static final String TYPE_XLS ="application/vnd.ms-excel";
	public static final String EXT_ZIP ="zip";
	public static final String TYPE_ZIP ="application/zip";
	public static final String EXT_RAR="rar";
	public static final String TYPE_RAR="application/x-rar-compressed";

	public static final String EXT_DOCX="docx";
	public static final String EXT_PPTX="pptx";
	public static final String EXT_XLSX="xlsx";
	public static final String EXT_XML="xml";
	public static final String TYPE_DOCX="application/vnd.openxmlformats-officedocument.wordprocessingml.document";
	public static final String TYPE_PPTX="application/vnd.openxmlformats-officedocument.presentationml.presentation";
	public static final String TYPE_XLSX="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
	public static void initContentType(){
		
		/*SOAP_ATTRIBUTES.add("ServerPort");
		SOAP_ATTRIBUTES.add("ServerIP");*/
		CONTENT_TYPE.put("xsl", "text/xml");
		CONTENT_TYPE.put(EXT_XML,"text/xml");
		CONTENT_TYPE.put(EXT_BMP,TYPE_BMP);
		CONTENT_TYPE.put(EXT_CSS, TYPE_CSS);
		CONTENT_TYPE.put(EXT_DOC,TYPE_DOC);
		CONTENT_TYPE.put(EXT_GIF, TYPE_GIF);
		CONTENT_TYPE.put(EXT_EXE, TYPE_EXE);
		CONTENT_TYPE.put(EXT_JPEG,TYPE_JPEG);
		CONTENT_TYPE.put(EXT_JPG,TYPE_JPG);
		CONTENT_TYPE.put(EXT_LATEX, TYPE_LATEX);
		CONTENT_TYPE.put(EXT_MP3,TYPE_MP3);
		CONTENT_TYPE.put(EXT_MPEG, TYPE_MPEG);
		CONTENT_TYPE.put(EXT_MPG, TYPE_MPG);
		CONTENT_TYPE.put(EXT_PDF, TYPE_PDF);
		CONTENT_TYPE.put(EXT_PPT,TYPE_PPT);
		CONTENT_TYPE.put(EXT_QT, TYPE_QT);
		CONTENT_TYPE.put(EXT_RTF, TYPE_RTF);
	
		CONTENT_TYPE.put(EXT_SWF, TYPE_SWF);
		CONTENT_TYPE.put(EXT_TXT, TYPE_TXT);
		CONTENT_TYPE.put(EXT_WAV, TYPE_WAV);
		CONTENT_TYPE.put(EXT_XLS,TYPE_XLS);
		CONTENT_TYPE.put(EXT_ZIP,TYPE_ZIP);
		CONTENT_TYPE.put(EXT_RAR, TYPE_RAR);
		
		CONTENT_TYPE.put(EXT_DOCX, TYPE_DOCX);
		CONTENT_TYPE.put(EXT_PPTX,TYPE_PPTX);
		CONTENT_TYPE.put(EXT_XLSX, TYPE_XLSX);
		
	}

}
