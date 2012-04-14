package edu.upenn.cis555.restserver.service;

import java.util.Comparator;

public class ResultScoreComparator implements Comparator<ResultScore> {

	@Override
	public int compare (ResultScore arg0, ResultScore arg1) {
		if (arg0.getScore () == arg1.getScore ())
			return 0;
		if (arg0.getScore () > arg1.getScore ())
			return 1;
		return -1;
	}
}
