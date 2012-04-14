package edu.upenn.cis555.crawler.distributed;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenerateSnippet {

	public static String getTitle (String content) {
		String title = null;
		Pattern linkpattern = Pattern
				.compile ("(?i)(<title.*?>)(.+?)(</title>)");
		Matcher matcher = linkpattern.matcher (content);
		while (matcher.find ()) {
			title = matcher.group (2);
		}
		return title;
	}

	public static String extractSnippet (String sentence, String keyWord) {
		//sentence.replaceAll("<[A-Za-z][=/'\":A-Za-z0-9]*>","");
		//sentence.replaceAll("<[A-Za-z][A-Za-z0-9]*[^\\w](\\s*)*[A-Za-z0-9]*>", "");
		sentence = sentence.replaceAll("\\<.*?>", "");
		
		String snippet = "";
		
		String pattern
		//Match all characters except penn AND (combination of a space and a word) 
		//for at least 0 and at max 2 times (CHANGE 2 to n if you want to see n words BEFORE penn in result)
		= "([^"+keyWord+"]*?(?:\\s[^\\s]*?){0,5})" +
		//Then match for a combination of aspace, Penn and a space for EXACTLY once
		"(\\s"+keyWord+"{1}\\s)" +
		//Then, match all characters except penn AND (combination of a word and a space) 
		//for at least 0 and at max 2 times (CHANGE 2 to n if you want to see n words AFTER penn in result)
		"((?:[^\\s]*?\\s){0,5}[^"+keyWord+"]*?)";
		//System.out.println("Sentence "+sentence);
		Pattern p = Pattern.compile (pattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher (sentence);
		int count = 0;
		
		while (m.find () && count < 5) {
			// Append google style ... before and after the result
			
			// snippet.concat(m.group().replace("\n", "").replace("\r", "") +
			// "...");
			//snippet += m.group () + "...";
			
			snippet += m.group().replaceAll("\\s+", " ")+ "...";
			count++;
		}
		
		snippet = snippet.replaceAll ("[\r\n]", "");
		//System.out.println(snippet);
		return snippet;
	}

}
