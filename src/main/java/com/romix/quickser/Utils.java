package com.romix.quickser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Various utilities
 */
class Utils {

	/**
	 * empty string is used as dummy value to represent null values in HashSet
	 * and TreeSet
	 */
	static final String EMPTY_STRING = "";


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
