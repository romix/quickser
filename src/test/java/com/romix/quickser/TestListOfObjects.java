package com.romix.quickser;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.romix.quickser.Serialization;
import junit.framework.TestCase;

public class TestListOfObjects extends TestCase {

	public void testListsOfObjects() throws ClassNotFoundException, IOException {
		TestAction rar = new TestAction();
		rar.setDestinationName("abc");
		rar.setParameter("abc", new String[] { "abc123", "efdrtf" });
		rar.setParameter("abc3", new String[] { "abc123", "efdrtf" });
		TestNested1 message = new TestNested1();
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));
		message.getPairs().put(
				UUID.randomUUID().toString(),
				new ArrayList<TestNested2>(Collections
						.singletonList(new TestNested2(UUID.randomUUID()
								.toString(), UUID.randomUUID().toString()))));

		Serializable[] args2 = new Serializable[] { message };
		rar.setParameter("messsage", args2);

		long startTime = System.currentTimeMillis();
		int MAX = 3000;
		Serialization ser = new Serialization();
		for (int i = 0; i < MAX; i++) {
			ser.deserialize(ser.serialize(rar));
		}
		long endTime = System.currentTimeMillis();

		System.out.println("Speed = " + ((endTime - startTime) * 1.0));

	}

	public static class TestAction implements Serializable {

		private static final long serialVersionUID = 2640786333075945218L;

		private transient String destinationName;
		private Map<String, List<Object[]>> parameters = new HashMap<String, List<Object[]>>();
		private List<Object> addedObjects = new ArrayList<Object>();
		private List<Object> removedObjects = new ArrayList<Object>();

		public String getDestinationName() {
			return destinationName;
		}

		public void setDestinationName(String destinationName) {
			this.destinationName = destinationName;
		}

		public void setParameter(String key, Serializable[] value) {
			List<Object[]> list = getParameters().get(key);
			if (list == null) {
				list = new ArrayList<Object[]>();
				getParameters().put(key, list);
			}
			list.add(value);
		}

		public List<Object[]> getParameter(String key) {
			return getParameters().get(key);
		}

		public List<Object> getAddedObjects() {
			return addedObjects;
		}

		public void setAddedObjects(List<Object> addedObjects) {
			this.addedObjects = addedObjects;
		}

		public List<Object> getRemovedObjects() {
			return removedObjects;
		}

		public void setRemovedObjects(List<Object> removedObjects) {
			this.removedObjects = removedObjects;
		}

		public Map<String, List<Object[]>> getParameters() {
			return parameters;
		}

		public void setParameters(Map<String, List<Object[]>> parameters) {
			this.parameters = parameters;
		}

	}

	static public class TestNested1 implements Serializable {

		private static final long serialVersionUID = -9013973267486457642L;
		private Map<String, List<TestNested2>> pairs = new HashMap<String, List<TestNested2>>();

		public Map<String, List<TestNested2>> getPairs() {
			return pairs;
		}

		public void setPairs(Map<String, List<TestNested2>> pairs) {
			this.pairs = pairs;
		}

	}

	static public class TestNested2 implements Serializable {

		private static final long serialVersionUID = 4612516347771564832L;

		public TestNested2() {

		}

		public TestNested2(String attr1, String attr2) {
			this.attr1 = attr1;
			this.attr2 = attr2;
		}

		public String getAttr1() {
			return attr1;
		}

		public void setAttr1(String attr1) {
			this.attr1 = attr1;
		}

		public String getAttr2() {
			return attr2;
		}

		public void setAttr2(String attr2) {
			this.attr2 = attr2;
		}

		private String attr1;

		private String attr2;
	}
}
