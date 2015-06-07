package org.mindswap.pellet.kryoserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public final class KryoSerializer {

   private static final Kryo kryo = new Kryo();

   static {
	   kryo.setReferences(true);
	   kryo.setRegistrationRequired(true);
	   kryo.setAsmEnabled(true);
	   ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
   }
   
   public static void register(Class... classes) {
      for(Class clazz : classes) {
         kryo.register(clazz);
      }
   }

   public static void register() {
	   //<Class.forName("")> may throw ClassNotFoundException.
	   try {
		   kryo.register(org.mindswap.pellet.ABox.class);
		   kryo.register(com.clarkparsia.pellet.datatypes.DatatypeReasonerImpl.class);
		   kryo.register(java.util.HashSet.class);
		   kryo.register(com.clarkparsia.pellet.datatypes.NamedDataRangeExpander.class);
		   kryo.register(java.util.HashMap.class);
		   kryo.register(aterm.pure.ATermApplImpl.class);
		   kryo.register(aterm.pure.ATermListImpl.class);
		   kryo.register(aterm.pure.PureFactory.class);
		   kryo.register(shared.SharedObjectFactory.class);
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment"));
		   kryo.register(Array.newInstance(Class.forName("shared.SharedObjectFactory$Segment"), 0).getClass());
		   kryo.register(Array.newInstance(Class.forName("shared.SharedObjectFactory$Segment$Entry"), 0).getClass());
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment$EntryWithID"));
		   kryo.register(Class.forName("java.lang.ref.ReferenceQueue$Null"));
		   kryo.register(Class.forName("java.lang.ref.ReferenceQueue$Lock"));
			
		   kryo.register(aterm.ATerm[].class);
		   kryo.register(aterm.pure.ATermIntImpl.class);
		   kryo.register(aterm.pure.AFunImpl.class);
		   kryo.register(int[].class);
			
		   kryo.register(org.mindswap.pellet.Individual.class);
		   kryo.register(org.mindswap.pellet.DependencySet.class);
		   kryo.register(org.mindswap.pellet.utils.intset.ArrayIntSet.class);
		   kryo.register(Class.forName("org.mindswap.pellet.utils.SetUtils$EmptySet"));
			
		   kryo.register(org.mindswap.pellet.EdgeList.class);
		   kryo.register(org.mindswap.pellet.Edge[].class);
		   kryo.register(java.util.ArrayList[].class);
		   kryo.register(java.util.ArrayList.class);
			
		   kryo.register(org.mindswap.pellet.ABoxStats.class);
		   kryo.register(org.mindswap.pellet.tableau.cache.ConceptCacheLRU.class);
		   kryo.register(org.mindswap.pellet.tableau.cache.CachedConstantNode.class);
		   kryo.register(Class.forName("org.mindswap.pellet.tableau.cache.CachedConstantNode$CachedNodeType"));
		   kryo.register(org.mindswap.pellet.KnowledgeBase.class);
		   kryo.register(org.mindswap.pellet.utils.MultiValueMap.class);
		   kryo.register(Class.forName("org.mindswap.pellet.KnowledgeBase$AssertionType"));
		   kryo.register(Class.forName("java.util.RegularEnumSet"));
			
		   kryo.register(Class.forName("org.mindswap.pellet.KnowledgeBase$ChangeType"));
		   kryo.register(Class.forName("org.mindswap.pellet.KnowledgeBase$DatatypeVisitor"));
		   kryo.register(org.mindswap.pellet.utils.SizeEstimate.class);
		   kryo.register(com.clarkparsia.pellet.expressivity.ExpressivityChecker.class);
		   kryo.register(com.clarkparsia.pellet.expressivity.DLExpressivityChecker.class);
		   kryo.register(com.clarkparsia.pellet.expressivity.Expressivity.class);
		   kryo.register(Class.forName("com.clarkparsia.pellet.expressivity.DLExpressivityChecker$Visitor"));
		   kryo.register(com.clarkparsia.pellet.el.ELExpressivityChecker.class);
		   kryo.register(Class.forName("org.mindswap.pellet.KnowledgeBase$FullyDefinedClassVisitor"));
		   kryo.register(org.mindswap.pellet.RBox.class);
			
		   kryo.register(org.mindswap.pellet.Role.class, new RoleSerializer()); 
			
		   kryo.register(Class.forName("java.util.Collections$EmptyMap"));
		   kryo.register(Class.forName("java.util.Collections$SingletonSet"));
		   kryo.register(org.mindswap.pellet.PropertyType.class);
		   kryo.register(org.mindswap.pellet.FSMBuilder.class);
		   kryo.register(Class.forName("org.mindswap.pellet.KnowledgeBase$ReasoningState"));
		   kryo.register(org.mindswap.pellet.tbox.impl.TBoxExpImpl.class);
		   kryo.register(org.mindswap.pellet.tbox.impl.TgBox.class);
		   kryo.register(Class.forName("org.mindswap.pellet.tbox.impl.Unfolding$Unconditional"));
		   kryo.register(Class.forName("java.util.Collections$EmptySet"));
		   kryo.register(org.mindswap.pellet.tbox.impl.TuBox.class);
		   kryo.register(java.util.IdentityHashMap.class);
		   kryo.register(org.mindswap.pellet.tbox.impl.TermDefinition.class);
		   kryo.register(com.clarkparsia.pellet.utils.IdentityHashSet.class);
		   kryo.register(org.mindswap.pellet.utils.Timers.class);
		   kryo.register(org.mindswap.pellet.utils.Timer.class);
		   kryo.register(java.util.LinkedHashMap.class);
		   
		 //Just for without MPJ
		   kryo.register(java.lang.ref.WeakReference.class);
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment$GarbageCollectionDetector"));
	   }
	   catch (ClassNotFoundException clNotFoundEx) {
		   System.out.println("Exception in register " + clNotFoundEx);
	   }   
   }
   
   public static byte[] serialize(final Object obj) {
	   
	   ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
	   Output output = new Output(bos);
	   
	   kryo.writeObject(output, obj);
	   output.flush();
	   output.close();
	   
	   return bos.toByteArray();
   }
   
   public static Object deserialize(final byte[] bytes, final Class<?> clazz) {
	  
	   ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
	   Input input = new Input(bis);
	   input.close();

	   return kryo.readObject(input, clazz);
   }
   
   public static void ByteHex(byte[] bytes) {
	   
	   StringBuilder sb = new StringBuilder();
	   for (byte b : bytes) {
	       sb.append(String.format("%02X ", b));
	   }
	   System.out.println("Byet in Hex : "+sb.toString());
   }
   
}
