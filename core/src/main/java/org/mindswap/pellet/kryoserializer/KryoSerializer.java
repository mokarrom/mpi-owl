package org.mindswap.pellet.kryoserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Array;
import java.util.Collections;

import org.mindswap.pellet.Node;
import org.mindswap.pellet.Role;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

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
		   kryo.register(aterm.pure.ATermIntImpl.class);
		   kryo.register(aterm.pure.PureFactory.class);
		   kryo.register(shared.SharedObjectFactory.class);
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment"));
		   kryo.register(Array.newInstance(Class.forName("shared.SharedObjectFactory$Segment"), 0).getClass());
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment$Entry"));
		   kryo.register(Array.newInstance(Class.forName("shared.SharedObjectFactory$Segment$Entry"), 0).getClass());
		   kryo.register(Class.forName("shared.SharedObjectFactory$Segment$EntryWithID"));
		   kryo.register(Array.newInstance(Class.forName("shared.SharedObjectFactory$Segment$EntryWithID"), 0).getClass());
		   kryo.register(Class.forName("java.lang.ref.ReferenceQueue$Null"));
		   kryo.register(Class.forName("java.lang.ref.ReferenceQueue$Lock"));
		   
		   kryo.register(int[].class);
		   kryo.register(aterm.ATerm[].class);
		   kryo.register(aterm.pure.AFunImpl.class);
		   kryo.register(aterm.ATermAppl[].class);
		   
		   kryo.register(org.mindswap.pellet.Individual.class);
		   kryo.register(org.mindswap.pellet.DependencySet.class);
		   kryo.register(org.mindswap.pellet.DependencySet[].class);
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
		   kryo.register(Role.class, new FieldSerializer<Role>(kryo, Role.class) {
			    public int compare (CachedField o1, CachedField o2) {
			    	if (o1.getField().getName().equals("name")) return -1;
			        if (o2.getField().getName().equals("name")) return 1;
			        return super.compare(o1, o2);
			    }
			});
		   
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
		   
		 //CompletionStrategy class registration
		   kryo.register(org.mindswap.pellet.tableau.completion.MPIALCStrategy.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.SimpleAllValuesRule.class);
		   kryo.register(Class.forName("org.mindswap.pellet.tableau.completion.rule.AbstractTableauRule$BlockingType"));
		   kryo.register(org.mindswap.pellet.tableau.completion.queue.NodeSelector.class);
		   kryo.register(org.mindswap.pellet.tableau.blocking.SubsetBlocking.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.ChooseRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.DataCardinalityRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.DataSatisfiabilityRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.DisjunctionRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.GuessRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.MaxRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.MinRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.NominalRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.SelfRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.SomeValuesRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.UnfoldingRule.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.CompletionStrategy.class);   
		   //new classes
		   kryo.register(org.mindswap.pellet.DefaultEdge.class);
		   kryo.register(org.mindswap.pellet.Literal.class);
		   kryo.register(org.mindswap.pellet.Clash.class);
		   kryo.register(org.mindswap.pellet.utils.fsm.TransitionGraph.class);
		   kryo.register(org.mindswap.pellet.utils.fsm.State.class);
		   kryo.register(org.mindswap.pellet.utils.fsm.Transition.class);
		   kryo.register(org.mindswap.pellet.tableau.completion.rule.AllValuesRule.class);
		   kryo.register(Class.forName("org.mindswap.pellet.Clash$ClashType"));
		   kryo.register(org.mindswap.pellet.tableau.blocking.OptimizedDoubleBlocking.class);
		   kryo.register(org.mindswap.pellet.tableau.blocking.EqualityBlocking.class);
		   kryo.register(org.mindswap.pellet.tableau.branch.DisjunctionBranch.class);
		   kryo.register(org.mindswap.pellet.NodeMerge.class);
		   kryo.register(Class.forName("java.lang.ref.Finalizer"));
		   kryo.register(Class.forName("java.util.WeakHashMap$Entry"));
	   }
	   catch (ClassNotFoundException clNotFoundEx) {
		   System.out.println("Exception in register " + clNotFoundEx);
	   }   
   }
   
   private static void register(String name) throws ClassNotFoundException {
       Class<?> type = load(name);
       FieldSerializer serializer = new FieldSerializer(kryo, type);
       serializer.setIgnoreSyntheticFields(true);
       kryo.register(type, serializer);
   }

   private static Class<?> load(String name) throws ClassNotFoundException {
       //return Class.forName(name, false, getClass().getClassLoader());
	   return Class.forName(name);
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
	  
	   Input input = new Input(bytes);
	   input.close();

	   return kryo.readObject(input, clazz);
   }
   
   public static void ByteHex(byte[] bytes) {
	   
	   StringBuilder sb = new StringBuilder();
	   for (byte b : bytes) {
	       sb.append(String.format("%02X ", b));
	   }
	   System.out.println("Byte in Hex : "+sb.toString());
   }
   
}
