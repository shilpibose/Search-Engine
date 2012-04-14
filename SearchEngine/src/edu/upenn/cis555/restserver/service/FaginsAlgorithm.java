package edu.upenn.cis555.restserver.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import edu.upenn.cis555.indexer.DocumentHits;
import edu.upenn.cis555.indexer.LexiconEntry;

public class FaginsAlgorithm {

	static private int VALUE_OF_K = 30;

	protected Hashtable<String, LexiconEntry> index;

	public FaginsAlgorithm (Hashtable<String, LexiconEntry> index) {
		this.index = index;
	}

	public Hashtable<DocumentHits, Double> calculateResult () {
		List<String> keyList = new ArrayList<String> (index.keySet ());

		// No hits for any of the search words
		if (keyList.size () == 0) {
			return new Hashtable<DocumentHits, Double> ();
		}

		Set<DocumentHits> intersectionDocs = new HashSet<DocumentHits> ();

		Hashtable<String, ArrayList<DocumentHits>> allDocs = new Hashtable<String, ArrayList<DocumentHits>> ();
		for (String s : keyList) {
			ArrayList<DocumentHits> hitList = allDocs.get (s);
			if (hitList == null) {
				hitList = new ArrayList<DocumentHits> ();
				allDocs.put (s, hitList);
			}
		}

		int CURRENT_VALUE_OF_K = VALUE_OF_K;
		for (String s : index.keySet ()) {
			int size = index.get (s).getInvertedBarrel ()
					.getDocumentHitsList ().size ();
			if (size < CURRENT_VALUE_OF_K) {
				CURRENT_VALUE_OF_K = size;
			}
		}

		List<DocumentHits> set = null;
		set = index.get (keyList.get (0)).getInvertedBarrel ()
				.getDocumentHitsList ().subList (0, CURRENT_VALUE_OF_K);
		intersectionDocs.addAll (set);
		allDocs.get (keyList.get (0)).addAll (set);

		for (int i = 1; i < keyList.size (); i++) {
			set = index.get (keyList.get (i)).getInvertedBarrel ()
					.getDocumentHitsList ().subList (0, CURRENT_VALUE_OF_K);
			intersectionDocs.retainAll (set);
			allDocs.get (keyList.get (i)).addAll (set);
		}

		DocumentHits next = null;
		boolean endReached = false;

		for (int i = CURRENT_VALUE_OF_K; (intersectionDocs.size () < CURRENT_VALUE_OF_K)
				&& (!endReached); i++) {
			next = index.get (keyList.get (0)).getInvertedBarrel ()
					.getDocumentHitsList ().get (i);
			intersectionDocs.add (next);
			allDocs.get (keyList.get (0)).add (next);

			for (int j = 1; j < keyList.size (); j++) {
				List<DocumentHits> hitList = index.get (keyList.get (j))
						.getInvertedBarrel ().getDocumentHitsList ();
				if (hitList.size () < (i + 1)) {
					endReached = true;
					break;
				}

				Collection<DocumentHits> collection = hitList
						.subList (0, i + 1);

				intersectionDocs.retainAll (collection);
				allDocs.get (keyList.get (j)).add (hitList.get (i));
			}
		}

		Set<DocumentHits> extraDocs = new HashSet<DocumentHits> ();
		Hashtable<DocumentHits, double[]> weights = new Hashtable<DocumentHits, double[]> ();

		for (int i = 0; i < keyList.size (); i++) {
			for (DocumentHits d : allDocs.get (keyList.get (i))) {
				double[] weightList = weights.get (d);

				if (weightList == null) {
					weightList = new double[keyList.size ()];
					for (int j = 0; j < keyList.size (); j++) {
						weightList[j] = 0;
					}
					weights.put (d, weightList);
				}
				weightList[i] = d.getWeight ();
			}
			System.err.println (allDocs.get (keyList.get (i)));
			extraDocs.addAll (allDocs.get (keyList.get (i)));
		}

		extraDocs.removeAll (intersectionDocs);
		System.err.println (extraDocs);
		System.err.println (intersectionDocs);

		for (DocumentHits d : extraDocs) {
			for (int i = 0; i < keyList.size (); i++) {
				double[] weightList = weights.get (d);
				if (weightList[i] == 0) {
					weightList[i] = getHitListFor (d.getDocumentId (), index
							.get (keyList.get (i)));
				}
			}
		}

		return calculateDotProduct (weights);
	}

	protected double getHitListFor (String docId, LexiconEntry lexEntry) {
		for (DocumentHits d : lexEntry.getInvertedBarrel ()
				.getDocumentHitsList ()) {
			if (d.getDocumentId ().equals (docId)) {
				return d.getWeight ();
			}
		}
		return 0;
	}

	protected Hashtable<DocumentHits, Double> calculateDotProduct (
			Hashtable<DocumentHits, double[]> weights) {
		Hashtable<DocumentHits, Double> docScore = new Hashtable<DocumentHits, Double> ();
		List<String> keyList = new ArrayList<String> (index.keySet ());

		for (DocumentHits d : weights.keySet ()) {
			double[] weightList = weights.get (d);
			double product = 0d;

			double denom_w = DocumentWeightManager.getInstance ().getResult (
					d.getUrl ());
			if (denom_w == 0) {
				denom_w = 3.0d;
			}
			System.err.println ("Document: " + d.getUrl () + " Weight: "
					+ denom_w);

			double denom_q = 0;
			for (int i = 0; i < index.size (); i++) {
				double idf = index.get (keyList.get (i)).getIdf ();
				product += weightList[i] * idf;
				denom_q += idf * idf;
			}
			product = product / (Math.sqrt (denom_q) * denom_w);
			docScore.put (d, product);
		}
		return docScore;
	}
}
