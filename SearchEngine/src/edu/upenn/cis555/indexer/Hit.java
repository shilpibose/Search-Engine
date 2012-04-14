package edu.upenn.cis555.indexer;

import java.io.Serializable;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class Hit implements Serializable {

	int position;
	boolean capital;
	int header;
	int font;
	boolean bold;
	boolean title;
	
	public Hit(){
		capital = false;
		bold = false;
		title = false;
	
	}
	public boolean isBold() {
		return bold;
	}
	public void setBold(boolean bold) {
		this.bold = bold;
	}
	public int getPosition() {
		return position;
	}
	public boolean isCapital() {
		return capital;
	}
	public int getFont() {
		return font;
	}
	public void setPosition(int position) {
		this.position = position;
	}
	public void setCapital(boolean capital) {
		this.capital = capital;
	}
	public void setFont(int font) {
		this.font = font;
	}
	public boolean isTitle() {
		return title;
	}
	public void setTitle(boolean title) {
		this.title = title;
	}
	
	
}
