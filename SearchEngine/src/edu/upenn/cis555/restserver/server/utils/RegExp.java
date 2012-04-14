package edu.upenn.cis555.restserver.server.utils;

public class RegExp {

	public static final String COMMA = "[ ]*,(?!([ ][0-3][0-9][ ](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)))[ ]*";

	public static final String COLON = "[ ]*:[ ]*";

	public static final String SEMI_COLON = "[ ]*;[ ]*";

	public static final String QUESTION_MARK = "\\?[ ]*";

	public static final String AMPERSAND = "&";

	public static final String EQUALS = "[ ]*=[ ]*";

	public static final String SPACE = "[ ]+";
}
