package com.romix.quickser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Various utilities used in JDBM
 */
class Utils {

	/**
	 * empty string is used as dummy value to represent null values in HashSet
	 * and TreeSet
	 */
	static final String EMPTY_STRING = "";

	/**
	 * Compares comparables. Default comparator for most of java types
	 */
	static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
		public int compare(Comparable o1, Comparable o2) {
			return o1.compareTo(o2);
		}
	};

	static String formatSpaceUsage(long size) {
		if (size < 1e4)
			return size + "B";
		else if (size < 1e7)
			return "" + Math.round(1D * size / 1024D) + "KB";
		else if (size < 1e10)
			return "" + Math.round(1D * size / 1e6) + "MB";
		else
			return "" + Math.round(1D * size / 1e9) + "GB";
	}

	static boolean allZeros(byte[] b) {
		for (int i = 0; i < b.length; i++) {
			if (b[i] != 0)
				return false;
		}
		return true;
	}

	static <E> E max(E e1, E e2, Comparator comp) {
		if (e1 == null)
			return e2;
		if (e2 == null)
			return e1;

		if (comp == null)
			comp = COMPARABLE_COMPARATOR;
		return comp.compare(e1, e2) < 0 ? e2 : e1;
	}

	static <E> E min(E e1, E e2, Comparator comp) {
		if (e1 == null)
			return e2;
		if (e2 == null)
			return e1;

		if (comp == null)
			comp = COMPARABLE_COMPARATOR;

		return comp.compare(e1, e2) > 0 ? e2 : e1;
	}

	static final Serializer<Object> NULL_SERIALIZER = new Serializer<Object>() {
		public void serialize(DataOutput out, Object obj) throws IOException {
			out.writeByte(11);
		}

		public Object deserialize(DataInput in) throws IOException,
				ClassNotFoundException {
			in.readByte();
			return null;
		}
	};

}
