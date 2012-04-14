package edu.upenn.cis555.node.utils;

import java.util.HashSet;

public class StopWords {

	public static HashSet<String> stopWords;

	public static final String[] words = { "a", "able", "about", "above",
			"abst", "accordance", "according", "accordingly", "across", "act",
			"actually", "added", "adj", "adopted", "affected", "affecting",
			"affects", "after", "afterwards", "again", "against", "ah", "all",
			"almost", "alone", "along", "already", "also", "although",
			"always", "am", "among", "amongst", "an", "and", "announce",
			"another", "any", "anybody", "anyhow", "anymore", "anyone",
			"anything", "anyway", "anyways", "anywhere", "apparently",
			"approximately", "are", "aren", "arent", "arise", "around", "as",
			"aside", "ask", "asking", "at", "auth", "available", "away",
			"awfully", "b", "back", "be", "became", "because", "become",
			"becomes", "becoming", "been", "before", "beforehand", "begin",
			"beginning", "beginnings", "begins", "behind", "being", "believe",
			"below", "beside", "besides", "between", "beyond", "biol", "both",
			"brief", "briefly", "but", "by", "c", "ca", "came", "can",
			"cannot", "cause", "causes", "certain", "certainly", "co", "com",
			"come", "comes", "contain", "containing", "contains", "could",
			"couldnt", "d", "date", "did", "didn", "different", "do", "does",
			"doesn", "doing", "done", "don", "down", "downwards", "due",
			"during", "e", "each", "ed", "edu", "effect", "eg", "eight",
			"eighty", "either", "else", "elsewhere", "end", "ending", "enough",
			"especially", "et", "et-al", "etc", "even", "ever", "every",
			"everybody", "everyone", "everything", "everywhere", "ex",
			"except", "f", "far", "few", "ff", "fifth", "first", "five", "fix",
			"followed", "following", "follows", "for", "former", "formerly",
			"forth", "found", "four", "from", "further", "furthermore", "g",
			"gave", "get", "gets", "getting", "give", "given", "gives",
			"giving", "go", "goes", "gone", "got", "gotten", "h", "had",
			"happens", "hardly", "has", "hasn", "have", "haven", "having",
			"he", "hed", "hence", "her", "here", "hereafter", "hereby",
			"herein", "heres", "hereupon", "hers", "herself", "hes", "hi",
			"hid", "him", "himself", "his", "hither", "home", "how", "howbeit",
			"however", "hundred", "i", "id", "ie", "if", "im", "immediate",
			"immediately", "importance", "important", "in", "inc", "indeed",
			"index", "information", "instead", "into", "invention", "inward",
			"is", "isn", "it", "itd", "it", "its", "itself", "j", "just", "k",
			"keep", "keeps", "kept", "keys", "kg", "km", "know", "known",
			"knows", "l", "largely", "last", "lately", "later", "latter",
			"latterly", "least", "less", "lest", "let", "lets", "like",
			"liked", "likely", "line", "little", "ll", "look", "looking",
			"looks", "ltd", "m", "made", "mainly", "make", "makes", "many",
			"may", "maybe", "me", "mean", "means", "meantime", "meanwhile",
			"merely", "mg", "might", "miss", "ml", "more", "moreover", "most",
			"mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n",
			"na", "name", "namely", "nay", "nd", "near", "nearly",
			"necessarily", "necessary", "need", "needs", "neither", "never",
			"nevertheless", "new", "next", "nine", "ninety", "no", "nobody",
			"non", "none", "nonetheless", "noone", "nor", "normally", "nos",
			"not", "noted", "nothing", "now", "nowhere", "o", "obtain",
			"obtained", "obviously", "of", "off", "often", "oh", "ok", "okay",
			"old", "omitted", "on", "once", "one", "ones", "only", "onto",
			"or", "ord", "other", "others", "otherwise", "ought", "our",
			"ours", "ourselves", "out", "outside", "over", "overall", "owing",
			"own", "p", "page", "pages", "part", "particular", "particularly",
			"past", "per", "perhaps", "placed", "please", "plus", "poorly",
			"possible", "possibly", "potentially", "pp", "predominantly",
			"present", "previously", "primarily", "probably", "promptly",
			"proud", "provides", "put", "q", "que", "quickly", "quite", "qv",
			"r", "ran", "rather", "rd", "re", "readily", "really", "recent",
			"recently", "ref", "refs", "regarding", "regardless", "regards",
			"related", "relatively", "respectively", "resulted", "resulting",
			"results", "right", "run", "s", "said", "same", "saw", "say",
			"saying", "says", "sec", "section", "see", "seeing", "seem",
			"seemed", "seeming", "seems", "seen", "self", "selves", "sent",
			"seven", "several", "shall", "she", "shed", "she", "shes",
			"should", "shouldn", "show", "showed", "shown", "showns", "shows",
			"significant", "significantly", "similar", "similarly", "since",
			"six", "slightly", "so", "some", "somebody", "somehow", "someone",
			"somethan", "something", "sometime", "sometimes", "somewhat",
			"somewhere", "soon", "sorry", "specifically", "specified",
			"specify", "specifying", "state", "still", "stop", "strongly",
			"sub", "substantially", "successfully", "such", "sufficiently",
			"suggest", "sup", "sure", "t", "take", "taken", "taking", "tell",
			"tends", "th", "than", "thank", "thanks", "thanx", "that", "that",
			"thats", "that", "the", "their", "theirs", "them", "themselves",
			"then", "thence", "there", "thereafter", "thereby", "thered",
			"therefore", "therein", "there", "thereof", "therere", "theres",
			"thereto", "thereupon", "there", "these", "they", "theyd", "they",
			"theyre", "they", "think", "this", "those", "thou", "though",
			"thoughh", "thousand", "throug", "through", "throughout", "thru",
			"thus", "til", "tip", "to", "together", "too", "took", "toward",
			"towards", "tried", "tries", "truly", "try", "trying", "ts",
			"twice", "two", "u", "un", "under", "unfortunately", "unless",
			"unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us",
			"use", "used", "useful", "usefully", "usefulness", "uses", "using",
			"usually", "v", "value", "various", "ve", "very", "via", "viz",
			"vol", "vols", "vs", "w", "want", "wants", "was", "wasn", "way",
			"we", "wed", "welcome", "well", "went", "were", "weren", "what",
			"whatever", "what", "whats", "when", "whence", "whenever", "where",
			"whereafter", "whereas", "whereby", "wherein", "wheres",
			"whereupon", "wherever", "whether", "which", "while", "whim",
			"whither", "who", "whod", "whoever", "whole", "who", "whom",
			"whomever", "whos", "whose", "why", "widely", "willing", "wish",
			"with", "within", "without", "won", "words", "world", "would",
			"wouldn", "www", "x", "y", "yes", "yet", "you", "youd", "you",
			"your", "youre", "yours", "yourself", "yourselves", "you", "z" };

	static {
		stopWords = new HashSet<String> ();
		for (String value : words) {
			stopWords.add (value);
		}
	}

	public static boolean isStopWord (String value) {
		if (stopWords.contains (value))
			return true;
		else
			return false;
	}

	public static String stemInitial (String word) {
		String value = word;

		if (value.matches ("\\s+"))
			return null;
		if (value.equals (""))
			return null;
		if (value.length () == 1)
			return null;

		// removes space if any
		value = value.trim ();

		// converts to lower case.
		value = value.toLowerCase ();

		char fchar = value.charAt (0);

		// removes the first character if it is not a letter
		if (! (Character.isLetterOrDigit (fchar)))
			value = value.substring (1);

		char lchar = value.charAt (value.length () - 1);

		// removes the last character if it is not a letter
		if (! (Character.isLetterOrDigit (lchar)))
			value = value.substring (0, value.length () - 1);

		// to remove 's
		if (value.endsWith ("'s"))
			value = value.substring (0, value.length () - 2);

		return value;
	}
}
