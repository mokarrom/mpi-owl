MPI-OWL: An NPI-backed parallel DL reasoner for Java
----------------------------------------------------

MPI-OWL is an MPI-backed prototype reasoner for tableau-based description logic reasoning in a distribute memory environment. It is developed based on Pellet by means of MPI as a distributed memory program using [MPJ Express](http://mpj-express.org/) library, an open source Java message passing library.
 
* [open source](https://github.com/mokarrom/qc-owl/blob/master/LICENSE.txt) (AGPL)
* pure Java
* developed on top of [Pellet](https://github.com/Complexible/pellet). 

Technical Challenge:
There are many technical challenges in implementing this dynamic manager-worker algorithm in Java by means MPI. One of the major challenges is passing an object (e.g., an ABox) from one process to another. As the object is not a primitive data type, to pass an object using MPI, all classes of that object must implement the Serializable interface. Since we are working on legacy code, it is not feasible for every class to implement the Serializable interface. Moreover, standard Java serialization is inefficient both in terms of speed and size. To deal with these problems, we converted an object to byte vectors using [Kryo](https://github.com/EsotericSoftware/kryo), a fast and efficient serialization framework for Java, and sent these byte vectors using the same method as primitive byte buffers. At the receiving end, the object is reconstructed using these byte buffers.

```java
public static byte[] serialize (final Object obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    Output output = new Output(bos);
    kryo.writeObject(output, obj);
    output.flush(); 
    output.close();
    return bos.toByteArray();
}

public static Object deserialize (final byte[] bytes, final
    Class<?> clazz) {
    Input input = new Input(bytes);
    input.close();
    return kryo.readObject(input, clazz);
}
```

Sending and receiving object...

```java
public static void sendObject (Object obj, int sendTo) {
   int size[] = new int[1];
   byte sByteArray[] = KryoSerializer.serialize(obj);
   size[0] = sByteArray.length;
   
   MPI.COMM_WORLD.Send (size, 0, 1, MPI.INT, sendTo, LENGTH_TAG);
   MPI.COMM_WORLD.Send (sByteArray, 0, size[0], MPI.BYTE, sendTo, OBJECT_TAG);
}

public static Object recvObject (int recvFrom) {
   MPI.COMM_WORLD.Recv (count, 0, 1, MPI.INT, recvFrom, LENGTH_TAG);
   byte rByteArray[] = new byte[count[0]];
   
   Status status = MPI.COMM_WORLD.Recv (rByteArray, 0, count[0], MPI.BYTE, recvFrom, OBJECT_TAG);
   Object obj = (Object) KryoSerializer.deserialize (rByteArray, Object. class);
   
   return obj;
}
``
This project is tested on [ACENET](http://www.ace-net.ca/) cluster.
