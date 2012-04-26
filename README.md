Quickser (QUICK SERialization) is a library for quick and efficient serialization of Java classes.
It is derived from the serialization framework of the JDBM3 project.

Features
========
*   Very fast - often faster than Kryo or protostuff
*   Much faster than usual Java serialization (up to 11 times faster on some tests)
*   Produces a very compact serialized representation, often smaller than Kryo or protostuff
*   Supports cyclic references and shared objects inside object graph
*   Has minimum external dependencies
*   Very small - just a few classes (33 KB jar)

Usage
=======

The API of Quickser is very simple:

   Serialization ser = new Serialization();
   Object obj1 = ...;
   byte[] binary = ser.serialize(obj1);
   Object obj2 = ser.deserialize(binary);

