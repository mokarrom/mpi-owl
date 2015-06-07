package org.mindswap.pellet.tableau.completion;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import mpi.*;

import org.mindswap.pellet.ABox;
import org.mindswap.pellet.IndividualIterator;
import org.mindswap.pellet.kryoserializer.KryoSerializer;
import org.mindswap.pellet.NodeMerge;
import org.mindswap.pellet.PelletOptions;
import org.mindswap.pellet.tableau.branch.Branch;
import org.mindswap.pellet.tableau.completion.rule.TableauRule;

import com.clarkparsia.pellet.expressivity.Expressivity;

public class MPIALCStrategy extends CompletionStrategy {
	
	static final int MASTER 		= 0;
	static final int CHECK_TAG 		= 101;
	static final int WAIT_TAG 		= 102;
	static final int STOP_TAG 		= 103;
	static final int ABOX 			= 104;
	static final int WORK_REQ_TAG 	= 201;
	static final int CLASH_TAG 		= 202;
	static final int COMPLETE_TAG 	= 203;
	static final int NEW_ABOX_TAG 	= 204;
	static final int NEW_ABOX 		= 205;
	
	protected List<ABox> aboxList;
	protected List<ABox> closedAboxList;
	
	public MPIALCStrategy(ABox abox) {
		super( abox );
	}
	
	public void complete(Expressivity expr) {
		
		initialize( expr );
		
		int rank = MPI.COMM_WORLD.Rank();
		int numProcs = MPI.COMM_WORLD.Size();
		
		KryoSerializer.register();
		
		if (rank == MASTER) {
			System.out.println("Maanager");
			//initialize( expr );
			//mpiOwlManager (numProcs - 1);

			int size[] = new int[1];
			byte sByteArray[] = KryoSerializer.serialize(abox);	//Serializer.serialize (abox);
			size[0] = sByteArray.length; System.out.println("Master : Length "+sByteArray.length); //KryoSerializer.ByteHex(sByteArray);
			MPI.COMM_WORLD.Send(size, 0, 1, MPI.INT, 1, CHECK_TAG);
			MPI.COMM_WORLD.Send(sByteArray, 0, size[0], MPI.BYTE, 1, ABOX);
		}
		else {
			System.out.println("Worker");
			//mpiOwlWorker();
			
			int count[] = new int[1];		
			MPI.COMM_WORLD.Recv(count, 0, 1, MPI.INT, MPI.ANY_SOURCE, CHECK_TAG);
			byte rByteArray[] = new byte[count[0]];		
			Status st = MPI.COMM_WORLD.Recv(rByteArray, 0, count[0], MPI.BYTE, MPI.ANY_SOURCE, ABOX);System.out.println("Worker : Length "+rByteArray.length);//KryoSerializer.ByteHex(rByteArray);
			ABox a = (ABox) KryoSerializer.deserialize(rByteArray, ABox.class);	//Serializer.deserialize(rByteArray);
			System.out.println("Object received");
		}
	}
	
	public void mpiOwlManager (int numOfWorker) {
		aboxList = new ArrayList<ABox>();
		closedAboxList = new ArrayList<ABox>();
		ABox[] D = new ABox[numOfWorker + 1];	//Index 0 will never be used. It is reserved for Master.
		int[] mSize = new int[1];
		int[] mDummy = new int[1];
		
		aboxList.add(abox);
		
		boolean isClosed = false, isCompleted = false;
		
		Status mStatus;
		int ptr = 0;
		
		while (!isCompleted && !isClosed) {
			mStatus = MPI.COMM_WORLD.Recv(mSize, 0, 1, MPI.INT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			if (mStatus.tag == WORK_REQ_TAG) {
				if (ptr < aboxList.size()) {
					ABox sABox = aboxList.get(ptr);
					int size[] = new int[1];
					byte sByteArray[] = KryoSerializer.serialize (sABox);
					size[0] = sByteArray.length;
					MPI.COMM_WORLD.Send(size, 0, 1, MPI.INT, mStatus.source, CHECK_TAG);
					MPI.COMM_WORLD.Send(sByteArray, 0, size[0], MPI.BYTE, mStatus.source, ABOX);
					D[mStatus.source] = sABox;
					ptr++;
				}
				else {
					MPI.COMM_WORLD.Send(mDummy, 0, 0, MPI.INT, mStatus.source, WAIT_TAG);
				}
			}
			else if (mStatus.tag == NEW_ABOX_TAG) {
				byte byteArray[] = new byte[mSize[0]];
				MPI.COMM_WORLD.Recv(byteArray, 0, mSize[0], MPI.BYTE, mStatus.source, NEW_ABOX);
				ABox mABox = (ABox) KryoSerializer.deserialize (byteArray, ABox.class);
				aboxList.add(mABox);	
			}
			else if (mStatus.tag == CLASH_TAG) {
				if (D[mStatus.source] != null) {
					closedAboxList.add(D[mStatus.source]);
					D[mStatus.source] = null;
				}		
				else
					System.out.println("Error: CLASH_TAG");
			}
			else if (mStatus.tag == COMPLETE_TAG) {
				isCompleted = true;
			}
			else {
				System.out.println("Error: Invalid TAG");
			}
			
			if (aboxList.size() == closedAboxList.size())
				isClosed = true;
 		}
		
		for (int j = 0; j < numOfWorker; j++) {
			mStatus = MPI.COMM_WORLD.Recv(mDummy, 0, 0, MPI.INT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			MPI.COMM_WORLD.Send(mDummy, 0, 0, MPI.INT, mStatus.source, STOP_TAG);
		}
		
	}
	
	public void mpiOwlWorker() {
		int[] wSize = new int[1];
		int[] wDummy = new int[1];		
		
		MPI.COMM_WORLD.Send(wDummy, 0, 0, MPI.INT, MASTER, WORK_REQ_TAG);
		Status wStatus = MPI.COMM_WORLD.Recv(wSize, 0, 1, MPI.INT, MASTER, MPI.ANY_TAG);
	
		while (wStatus.tag != STOP_TAG) {
			if (wStatus.tag == CHECK_TAG) {
				
				byte byteArray[] = new byte[wSize[0]];
				
				MPI.COMM_WORLD.Recv(byteArray, 0, wSize[0], MPI.BYTE, MASTER, ABOX);
				
				ABox wABox = (ABox) KryoSerializer.deserialize (byteArray, ABox.class);
				
				boolean isConsistent;
				if (wABox != null) {
					isConsistent = mpiIsConsistent (wABox);
					if (isConsistent) {
						MPI.COMM_WORLD.Send(wDummy, 0, 0, MPI.INT, MASTER, COMPLETE_TAG);
					}
					else {
						MPI.COMM_WORLD.Send(wDummy, 0, 0, MPI.INT, MASTER, CLASH_TAG);
					}
					
					MPI.COMM_WORLD.Send(wDummy, 0, 0, MPI.INT, MASTER, WORK_REQ_TAG);
				}
				else {
					System.out.println("Error: wABox null");
				}				
			}
			else if (wStatus.tag == WAIT_TAG) {
				try {
				    Thread.sleep(10000);                 //1000 milliseconds is one second.
				} catch(InterruptedException ex) {
				    Thread.currentThread().interrupt();
				}
				
				MPI.COMM_WORLD.Send(wDummy, 0, 0, MPI.INT, MASTER, WORK_REQ_TAG);
			}
			else {
				System.out.println("Error: Invalid TAG");
			}
			
			wSize[0] = 0;
			wStatus = MPI.COMM_WORLD.Recv(wSize, 0, 1, MPI.INT, MASTER, MPI.ANY_TAG);
		}
	}
	
	public boolean mpiIsConsistent (ABox abox) {
		System.out.println("Rank "+MPI.COMM_WORLD.Rank()+" : Consistency checking on "+abox.toString());
		initialize (abox);
		
		while( abox.isChanged() && !abox.isClosed() ) {
			completionTimer.check();

			abox.setChanged( false );

			if( log.isLoggable( Level.FINE ) ) {
				log.fine( "Branch: " + abox.getBranch() + ", Depth: " + abox.stats.treeDepth
						+ ", Size: " + abox.getNodes().size() + ", Mem: "
						+ (Runtime.getRuntime().freeMemory() / 1000) + "kb" );
				abox.validate();
				printBlocked();
				abox.printTree();
			}

			IndividualIterator i = abox.getIndIterator();

			for( TableauRule tableauRule : tableauRules ) {
				tableauRule.apply( i );
				if( abox.isClosed() )
					break;
			}
		}

		if( abox.isClosed() ) {
			if( log.isLoggable( Level.FINE ) )
				log.fine( "Clash at Branch (" + abox.getBranch() + ") " + abox.getClash() );
			return false;
		}
		else {
			abox.setComplete( true );
			return true;
		}
	}

}
