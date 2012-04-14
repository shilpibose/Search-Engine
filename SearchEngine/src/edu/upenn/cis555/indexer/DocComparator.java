package edu.upenn.cis555.indexer;

import java.util.Comparator;

public class DocComparator implements Comparator<DocumentHits> {

	@Override
	public int compare (DocumentHits arg0, DocumentHits arg1) {
		if (arg0.getWeight () == arg1.getWeight ())
			return 0;
		if (arg0.getWeight () > arg1.getWeight ())
			return 1;
		return -1;

	}
}
