package com.romix.quickser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import org.junit.Test;

import static junit.framework.Assert.*;

public class TestSerializationSpeed {

	private static final int NUM_ITRERATIONS = 30000;
	private static final int NUM_THREADS = 1;

	public interface Serializer {
		public byte[] serialize(Object obj, Class<?> clazz) throws IOException;

		public Object deserialize(byte[] buf, Class<?> clazz)
				throws IOException, ClassNotFoundException,
				InstantiationException, IllegalAccessException;

		public String getName();
	}

	static public class JavaSerializer implements Serializer {

		public String getName() {
			return "JavaSerializer";
		}

		@Override
		public byte[] serialize(Object obj, Class<?> clazz) throws IOException {
			return serializeJava(obj, clazz);
		}

		@Override
		public Object deserialize(byte[] buf, Class<?> clazz)
				throws IOException, ClassNotFoundException,
				InstantiationException, IllegalAccessException {
			return deserializeJava(buf, clazz);
		}

		public static byte[] serializeJava(Object obj,
				@SuppressWarnings("rawtypes") Class clazz) throws IOException {
			ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.close();
			byte[] byteBuffer = bos.toByteArray();
			bos.close();
			return byteBuffer;
		}

		public static Object deserializeJava(byte[] source,
				@SuppressWarnings("rawtypes") Class clazz) throws IOException,
				ClassNotFoundException, InstantiationException,
				IllegalAccessException {
			ByteArrayInputStream bis = new ByteArrayInputStream(source);
			ObjectInputStream ois = new ObjectInputStream(bis);
			Object obj = ois.readObject();
			ois.close();
			bis.close();
			return obj;
		}
	}

	static public class QuickserSerializer implements Serializer {

		private Serialization quickserSerializer = new Serialization();

		@Override
		public byte[] serialize(Object obj, Class<?> clazz) throws IOException {
			// jdbmSerializer = new Serialization();
			return quickserSerializer.serialize(obj);
		}

		@Override
		public Object deserialize(byte[] buf, Class<?> clazz)
				throws IOException, ClassNotFoundException,
				InstantiationException, IllegalAccessException {
			return quickserSerializer.deserialize(buf);
		}

		@Override
		public String getName() {
			return "QuickserSerializer";
		}

	}

	public final class GZIPSerializer implements Serializer {

		static final int BUF_SIZE = 3000;
		static final int COMPRESSION_LEVEL = 5;
		private final Serializer _serializer;
		private final Deflater deflater;
		private final Inflater inflater;

		/**
		 * Constructs a new GZIPCompressionSerializer with the specified wrapped
		 * serializer.
		 * 
		 * @param serializer
		 *            the wrapped serializer
		 */
		public GZIPSerializer(Serializer serializer) {
			_serializer = serializer;
			deflater = new Deflater(COMPRESSION_LEVEL, true);
			inflater = new Inflater(true);
		}

		@Override
		public byte[] serialize(Object obj, Class<?> clazz) throws IOException {
			byte[] serialized = _serializer.serialize(obj, clazz);
			deflater.reset();
			deflater.setInput(serialized, 0, serialized.length);
			deflater.finish();
			byte[] serout = new byte[BUF_SIZE];
			int outputBytes = deflater.deflate(serout);
			return Arrays.copyOf(serout, outputBytes);
		}

		@Override
		public Object deserialize(byte[] buf, Class<?> clazz)
				throws IOException, ClassNotFoundException,
				InstantiationException, IllegalAccessException {
			inflater.reset();
			inflater.setInput(buf, 0, buf.length);
			byte[] deserout = new byte[BUF_SIZE];
			try {
				int outputBytes = inflater.inflate(deserout);
				return _serializer.deserialize(
						Arrays.copyOf(deserout, outputBytes), clazz);
			} catch (DataFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public String getName() {
			return "GZIP-" + _serializer.getName();
		}

	}

	public static class Bean implements Serializable {
		public String name;

		public List firstList, secondList;

		public Object firstItem, secondItem;

		public Map firstMap, secondMap;

		public HasName firstHasName, secondHashname;

		public Named firstNamed, secondNamed;

		public Object firstObject, secondObject;

		Map<String, ?> firstStringMap, secondStringMap;

		Map<HasName, ?> firstHasNameMap, secondHasNameMap;

		Map<Named, ?> firstNamedMap, secondNamedMap;

		int[] firstIntArray, secondIntArray;

		Map<Set<String>, ?> firstSetMap, secondSetMap;

		Map<List<Order>, ?> firstListMap, secondListMap;

		Map<Set<Order>, ?> firstEnumSetMap, secondEnumSetMap;

		List<Map<String, ?>> firstMapList, secondMapList;

		List<Map<Order, Item>> firstEnumMapList, secondEnumMapList;

		Item[] firstItemArray, secondItemArray;

		Object itemArray;

		Item[][] itemArray2d;

		Object[] itemArrayWrapper;

		public Set firstSet, secondSet;

		public IdentityHashMap identityMap, anotherIdentityMap;

	}

	public interface HasName {
		String getName();
	}

	public static abstract class Named {
		public abstract String getName();
	}

	public static class SerializableObject implements Serializable {
		int x;

		public SerializableObject() {
		}
	}

	public static class Item extends Named implements HasName, Serializable {

		public String name;

		public Item() {

		}

		public Item(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public String toString() {
			return "name:" + name;
		}

		@Override
		public int hashCode() {
			return name.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Item other = (Item) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

	}

	enum Order {
		ASCENDING(1), DESCENDING(2);

		private int val;
		private int id;

		Order(int val) {
			this.val = val;
			this.id = 10;
		}

		public void setVal(int newVal) {
			this.val = newVal;
		}
	}

	static <T> Set<T> newSet(T... ts) {
		HashSet<T> set = new HashSet<T>();

		for (T t : ts)
			set.add(t);

		return set;
	}

	static <K, V> Map<K, V> newMap() {
		return new HashMap<K, V>();
	}

	static <T> Set<T> newSet() {
		return new HashSet<T>();
	}

	static <T> List<T> newList() {
		return new ArrayList<T>();
	}

	static <T> List<T> newList(T... ts) {
		ArrayList<T> list = new ArrayList<T>();

		for (T t : ts)
			list.add(t);

		return list;
	}

	static <K, V> Map<K, V> newMap(K key, V value) {
		HashMap<K, V> map = new HashMap<K, V>();
		map.put(key, value);
		return map;
	}

	static Bean fill(Bean bean) {
		bean.name = "test";

		Item share = new Item("share");

		List<Item> firstList = newList(share, new Item("new_a"));
		bean.firstList = firstList;

		List<Item> secondList = newList(share, new Item("new_a"));
		bean.secondList = secondList;

		Item share2;
		bean.firstItem = bean.secondItem = share2 = new Item("share2");

		Item share3 = new Item("share3");
		Map<String, Item> firstMap = newMap();
		firstMap.put("share", share);
		firstMap.put("share3", share3);
		bean.firstMap = firstMap;

		Map<String, Item> secondMap = newMap();
		secondMap.put("share", share);
		secondMap.put("share3", share3);
		bean.secondMap = secondMap;

		Item share4, share5;
		bean.firstHasName = bean.secondHashname = share4 = new Item("share4");

		bean.firstNamed = bean.secondNamed = share5 = new Item("share5");

		bean.firstObject = bean.secondObject = new SerializableObject();

		Item share6 = new Item("share6");
		Map<String, Item> firstStringMap = newMap();
		firstStringMap.put("share", share);
		firstStringMap.put("share6", share6);
		bean.firstStringMap = firstStringMap;

		Map<String, Item> secondStringMap = newMap();
		secondStringMap.put("share", share);
		secondStringMap.put("share6", share6);
		bean.secondStringMap = secondStringMap;

		Item share7 = new Item("share7");
		Map<HasName, Item> firstHasNameMap = newMap();
		firstHasNameMap.put(share, share);
		firstHasNameMap.put(share7, share7);
		bean.firstHasNameMap = firstHasNameMap;

		Map<HasName, Item> secondHasNameMap = newMap();
		secondHasNameMap.put(share, share);
		secondHasNameMap.put(share7, share7);
		bean.secondHasNameMap = secondHasNameMap;

		Item share8 = new Item("share8");
		Map<Named, Item> firstNamedMap = newMap();
		firstNamedMap.put(share, share);
		firstNamedMap.put(share8, share8);
		bean.firstNamedMap = firstNamedMap;

		Map<Named, Item> secondNamedMap = newMap();
		secondNamedMap.put(share, share);
		secondNamedMap.put(share8, share8);
		bean.secondNamedMap = secondNamedMap;

		bean.firstIntArray = bean.secondIntArray = new int[] { 1, 2, 3 };

		Item share9 = new Item("share9");
		Set<String> set = newSet("share9");
		Map<Set<String>, Item> firstSetMap = newMap();
		firstSetMap.put(set, share9);
		bean.firstSetMap = firstSetMap;

		Map<Set<String>, Item> secondSetMap = newMap();
		secondSetMap.put(set, share9);
		bean.secondSetMap = secondSetMap;

		Item share10 = new Item("share10");
		List<Order> orderList = newList(Order.ASCENDING, Order.DESCENDING,
				Order.ASCENDING);

		Map<List<Order>, Item> firstListMap = newMap();
		firstListMap.put(orderList, share10);
		bean.firstListMap = firstListMap;

		Map<List<Order>, Item> secondListMap = newMap();
		secondListMap.put(orderList, share10);
		bean.secondListMap = secondListMap;

		Item share11 = new Item("share11");
		Set<Order> orderEnumSet = new HashSet<Order>();

		Map<Set<Order>, Item> firstEnumSetMap = newMap();
		firstEnumSetMap.put(orderEnumSet, share11);
		bean.firstEnumSetMap = firstEnumSetMap;

		Map<Set<Order>, Item> secondEnumSetMap = newMap();
		secondEnumSetMap.put(orderEnumSet, share11);
		bean.secondEnumSetMap = secondEnumSetMap;

		Item share12 = new Item("share12");
		Map<String, Item> itemMap = newMap();
		itemMap.put("share12", share12);
		itemMap.put("share", share);

		List<Map<String, ?>> firstMapList = newList();
		firstMapList.add(itemMap);
		bean.firstMapList = firstMapList;

		List<Map<String, ?>> secondMapList = newList();
		secondMapList.add(itemMap);
		bean.secondMapList = secondMapList;

		Item share13 = new Item("share13");
		Map<Order, Item> orderMap = new HashMap<Order, Item>();
		orderMap.put(Order.ASCENDING, share13);
		orderMap.put(Order.DESCENDING, share);

		List<Map<Order, Item>> firstEnumMapList = newList();
		firstEnumMapList.add(orderMap);
		bean.firstEnumMapList = firstEnumMapList;

		List<Map<Order, Item>> secondEnumMapList = newList();
		secondEnumMapList.add(orderMap);
		bean.secondEnumMapList = secondEnumMapList;

		Item share14 = new Item("share14");
		bean.itemArray = bean.firstItemArray = bean.secondItemArray = new Item[] {
				share, share2, share3, share4, share5, share6, share7, share8,
				share9, share10, share11, share12, share13, share14 };

		bean.itemArray2d = new Item[1][];
		bean.itemArray2d[0] = bean.firstItemArray;

		bean.itemArrayWrapper = new Object[] { bean.itemArray,
				bean.itemArray2d, share };

		Item share15 = new Item("share15");
		Set<Item> firstSet = newSet(share15);
		bean.firstSet = firstSet;

		Set<Item> secondSet = newSet(share15);
		bean.secondSet = secondSet;

		IdentityHashMap<Item, Item> identityMap = new IdentityHashMap<Item, Item>();
		identityMap.put(share, share);

		bean.identityMap = identityMap;

		Item share16 = new Item("share16");
		IdentityHashMap<Order, Item> anotherIdentityMap = new IdentityHashMap<Order, Item>();
		anotherIdentityMap.put(Order.ASCENDING, share16);
		anotherIdentityMap.put(Order.DESCENDING, share);

		bean.anotherIdentityMap = anotherIdentityMap;

		return bean;
	}

	static void verify(Bean bean) {
		assertEquals(bean.name, "test");

		assertNotNull(bean.firstList);
		assertTrue(bean.firstList.size() == 2);
		assertTrue(bean.firstList.get(0) == bean.secondList.get(0));

		assertNotNull(bean.secondItem);
		assertTrue(bean.secondItem == bean.firstItem);

		assertNotNull(bean.firstMap);
		assertTrue(bean.firstMap.size() == 2);

		assertTrue(bean.firstMap.get("share") == bean.secondMap.get("share"));
		assertTrue(bean.firstMap.get("share3") == bean.secondMap.get("share3"));

		assertNotNull(bean.firstHasName);
		assertTrue(bean.firstHasName == bean.secondHashname);

		assertNotNull(bean.firstNamed);
		assertTrue(bean.firstNamed == bean.secondNamed);

		assertNotNull(bean.firstObject);
		assertTrue(bean.firstObject == bean.secondObject);

		assertNotNull(bean.firstStringMap);
		assertTrue(bean.firstStringMap.size() == 2);
		assertTrue(bean.firstStringMap.get("share") == bean.secondStringMap
				.get("share"));
		assertTrue(bean.firstStringMap.get("share6") == bean.secondStringMap
				.get("share6"));

		assertNotNull(bean.firstHasNameMap);
		assertTrue(bean.firstHasNameMap.size() == 2);
		assertTrue(bean.firstHasNameMap.get("share") == bean.secondHasNameMap
				.get("share"));
		assertTrue(bean.firstHasNameMap.get("share7") == bean.secondHasNameMap
				.get("share7"));

		assertNotNull(bean.firstNamedMap);
		assertTrue(bean.firstNamedMap.size() == 2);
		assertTrue(bean.firstNamedMap.get("share") == bean.secondNamedMap
				.get("share"));
		assertTrue(bean.firstNamedMap.get("share8") == bean.secondNamedMap
				.get("share8"));

		assertNotNull(bean.firstIntArray);
		assertTrue(bean.firstIntArray.length == 3);
		assertTrue(bean.firstIntArray == bean.secondIntArray);

		assertNotNull(bean.firstSetMap);
		assertTrue(bean.firstSetMap.size() == 1);

		assertTrue(bean.firstSetMap.values().iterator().next() == bean.secondSetMap
				.values().iterator().next());
		assertTrue(bean.firstSetMap.keySet().iterator().next() == bean.secondSetMap
				.keySet().iterator().next());

		assertNotNull(bean.firstListMap);
		assertTrue(bean.firstListMap.size() == 1);

		List<Order> orderList = bean.firstListMap.keySet().iterator().next();
		assertTrue(orderList.size() == 3);
		assertTrue(orderList.get(0) == Order.ASCENDING);
		assertTrue(orderList.get(1) == Order.DESCENDING);
		assertTrue(orderList.get(2) == Order.ASCENDING);

		assertTrue(orderList == bean.secondListMap.keySet().iterator().next());

		assertTrue(bean.firstListMap.values().iterator().next() == bean.secondListMap
				.values().iterator().next());

		assertNotNull(bean.firstEnumSetMap);
		assertTrue(bean.firstEnumSetMap.size() == 1);

		assertTrue(bean.firstEnumSetMap.values().iterator().next() == bean.secondEnumSetMap
				.values().iterator().next());
		assertTrue(bean.firstEnumSetMap.keySet().iterator().next() == bean.secondEnumSetMap
				.keySet().iterator().next());

		assertNotNull(bean.firstMapList);
		assertTrue(bean.firstMapList.size() == 1);
		assertTrue(bean.firstMapList.get(0) == bean.secondMapList.get(0));

		assertNotNull(bean.firstEnumMapList);
		assertTrue(bean.firstEnumMapList.size() == 1);
		assertTrue(bean.firstEnumMapList.get(0) == bean.secondEnumMapList
				.get(0));

		assertNotNull(bean.firstItemArray);
		assertTrue(bean.firstItemArray.length == 14);

		assertTrue(bean.firstItemArray == bean.secondItemArray);
		assertTrue(bean.firstItemArray == bean.itemArray);

		assertEquals(bean.firstItemArray[0].name, "share");
		assertEquals(bean.firstItemArray[1].name, "share2");
		assertEquals(bean.firstItemArray[2].name, "share3");
		assertEquals(bean.firstItemArray[3].name, "share4");
		assertEquals(bean.firstItemArray[4].name, "share5");
		assertEquals(bean.firstItemArray[5].name, "share6");
		assertEquals(bean.firstItemArray[6].name, "share7");
		assertEquals(bean.firstItemArray[7].name, "share8");
		assertEquals(bean.firstItemArray[8].name, "share9");
		assertEquals(bean.firstItemArray[9].name, "share10");
		assertEquals(bean.firstItemArray[10].name, "share11");
		assertEquals(bean.firstItemArray[11].name, "share12");
		assertEquals(bean.firstItemArray[12].name, "share13");
		assertEquals(bean.firstItemArray[13].name, "share14");

		assertNotNull(bean.itemArray2d);
		assertTrue(bean.itemArray2d.length == 1);
		assertTrue(bean.itemArray2d[0] == bean.firstItemArray);

		assertNotNull(bean.itemArrayWrapper);
		assertTrue(bean.itemArrayWrapper.length == 3);
		assertTrue(bean.itemArrayWrapper[0] == bean.itemArray);
		assertTrue(bean.itemArrayWrapper[1] == bean.itemArray2d);
		assertTrue(bean.itemArrayWrapper[2] == bean.firstItemArray[0]);

		assertNotNull(bean.firstSet);
		assertTrue(bean.firstSet.size() == 1);
		assertTrue(bean.firstSet.iterator().next() == bean.secondSet.iterator()
				.next());

		assertNotNull(bean.identityMap);
		assertTrue(bean.identityMap.size() == 1);
		assertTrue(bean.identityMap.keySet().iterator().next() == bean.identityMap
				.values().iterator().next());

		Item share = bean.firstItemArray[0];
		assertEquals(share.name, "share");
		assertTrue(bean.identityMap.containsKey(share));
		assertTrue(bean.identityMap.get(share) == share);

		assertNotNull(bean.anotherIdentityMap);
		assertTrue(bean.anotherIdentityMap.size() == 2);
		assertEquals(bean.anotherIdentityMap.get(Order.ASCENDING), new Item(
				"share16"));
		assertTrue(bean.anotherIdentityMap.get(Order.DESCENDING) == share);
	}

	@Test
	public void testJavaSerialization() throws Exception {
		Serializer ser = new JavaSerializer();
		doChecks(new Serializer[] { ser, ser, ser, ser });
	}

//	@Test
	public void testJavaSerializationWithCompression() throws Exception {
		Serializer ser = new JavaSerializer();
		doChecks(new Serializer[] { new GZIPSerializer(ser),
				new GZIPSerializer(ser), new GZIPSerializer(ser),
				new GZIPSerializer(ser) });
	}

	@Test
	public void testQuickserSerialization() throws Exception {
		Serializer ser = new QuickserSerializer();
		doChecks(new Serializer[] { ser, ser, ser, ser });
	}

	@Test
	public void testQuickserSerializationWithCompression() throws Exception {
		Serializer ser = new QuickserSerializer();
		doChecks(new Serializer[] { new GZIPSerializer(ser),
				new GZIPSerializer(ser), new GZIPSerializer(ser),
				new GZIPSerializer(ser) });
	}

	private void doChecks(Serializer[] ser) throws Exception {
		Bean bean = fill(new Bean());
		verify(bean);
		checkSerializedSize(ser[0], bean);
		checkPerformance(ser, bean, NUM_ITRERATIONS);
	}

	private void checkPerformance(final Serializer ser[], final Bean bean,
			final int numItrerations) throws Exception {

		ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);

		final CountDownLatch counter = new CountDownLatch(NUM_THREADS);
		long startTime = System.currentTimeMillis();
		for (int thread = 0; thread < NUM_THREADS; thread++) {
			final int threadNum = thread;
			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						for (int i = 0; i < numItrerations; ++i) {
							byte[] serializedBuf = ser[threadNum].serialize(
									bean, bean.getClass());
							Bean deBean = (Bean) ser[threadNum].deserialize(
									serializedBuf, bean.getClass());
						}
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					counter.countDown();
				}
			});
		}
		counter.await();
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		System.out.println("Speed of " + ser[0].getName() + ": "
				+ numItrerations + " iterations in " + duration + " ms; "
				+ numItrerations * NUM_THREADS * 1.0 / duration
				+ " iterations/ms");
		threadPool.shutdown();
	}

	private void checkSerializedSize(Serializer ser, Bean bean)
			throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		byte[] serializedBuf = ser.serialize(bean, bean.getClass());
		System.out.println("Size of serialized representation using "
				+ ser.getName() + ": " + serializedBuf.length);
		Bean deBean = (Bean) ser.deserialize(serializedBuf, bean.getClass());
		verify(deBean);
	}
}
