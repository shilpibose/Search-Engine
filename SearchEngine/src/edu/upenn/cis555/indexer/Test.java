package edu.upenn.cis555.indexer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.tidy.Tidy;

import edu.upenn.cis555.node.utils.Stemmer;
import edu.upenn.cis555.node.utils.StopWords;

public class Test {

	public static void main(String[]args){
		String value = " Europe ";
		String word = value.toLowerCase();
		//System.out.println(word);
		
		Stemmer stemmer = new Stemmer();
		String stemWordInitial = StopWords.stemInitial(word);
		String stemWord = null;
		if(stemWordInitial != null) {
	//		stemWord = stemmer.stem(stemWordInitial);
		}
		System.out.println("Stemed word:"+stemWord);
		
		//Pattern integerPattern = Pattern.compile("^\\d*$");
		Pattern integerPattern = Pattern.compile("\\d*");
		
		Matcher matchesInteger = integerPattern.matcher(word);
		boolean isInteger = matchesInteger.matches();
		if(isInteger)System.out.println("Integer");
		else System.out.println("Not a Integer");		
	
	}
	/*
	public void test1(){
	String content = "content";
		OutputFormat format = new OutputFormat ();
        StringWriter stringOut = new StringWriter ();
        XMLSerializer serial = new XMLSerializer (stringOut, format);
        serial.serialize (crawlData.getContent ());
        content = stringOut.toString ();

	}
	
	public void test2(){
		Tidy tidy = new Tidy(); // obtain a new Tidy instance
	boolean xhtml = true;
		tidy.setXHTML(xhtml); // set desired config options using tidy setters 
		          // (equivalent to command line options)

		tidy.parse(inputStream, System.out);
	}
*/

}
