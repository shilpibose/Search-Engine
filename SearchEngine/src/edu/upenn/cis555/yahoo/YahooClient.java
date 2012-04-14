package edu.upenn.cis555.yahoo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.upenn.cis555.crawler.distributed.MyHttpClient;

public class YahooClient {

	String query;
	int count;
	
	public YahooClient(String search, int count){
		this.query=search;
		this.count=count;
	}
	
	public YahooSearchResult getSearchResults(){
		YahooSearchResult result = new YahooSearchResult();
		ArrayList<String> links = new ArrayList<String>();
		try {
			MyHttpClient client = new MyHttpClient(new URL("http://boss.yahooapis.com/ysearch/web/v1/"+this.query+"?appid="+Constants.bossAppID+"&format=xml&count="+count+""));
			client.sendGetMessage();
			Pattern p = Pattern.compile("(?<=(<url>))(.*?)(?=(</url>))",
					Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(client.getContent());
			while(m.find()){
				links.add(m.group(2));
			}
			result.setLinks(links);
			result.setQuery(this.query);
			return result;
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args){
		YahooClient client = new YahooClient("google",30);
		YahooSearchResult result =client.getSearchResults();
		ArrayList<String> links = result.getLinks();
		for(String link : links){
			System.out.println(link);
		}
	}
}
