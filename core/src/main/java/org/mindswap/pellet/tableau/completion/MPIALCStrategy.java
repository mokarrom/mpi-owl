package org.mindswap.pellet.tableau.completion;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import mpi.*;

import org.mindswap.pellet.ABox;
import org.mindswap.pellet.IndividualIterator;
import org.mindswap.pellet.exceptions.InternalReasonerException;
import org.mindswap.pellet.kryoserializer.KryoSerializer;
import org.mindswap.pellet.NodeMerge;
import org.mindswap.pellet.PelletOptions;
import org.mindswap.pellet.tableau.branch.Branch;
import org.mindswap.pellet.tableau.completion.rule.TableauRule;

import com.clarkparsia.pellet.expressivity.Expressivity;

public class MPIALCStrategy extends CompletionStrategy {
	
	static final int dummy 					= 111;
	static final int MASTER 				= 0;
	static final int CHECK_TAG 				= 101;
	static final int WAIT_TAG 				= 102;
	static final int STOP_TAG 				= 103;
	static final int ABOX 					= 104;
	static final int WORK_REQ_TAG 			= 201;
	static final int CLASH_TAG 				= 202;
	static final int COMPLETE_TAG 			= 203;
	static final int NEW_ABOX_TAG 			= 204;
	static final int NEW_ABOX 				= 205;
	static final int AVAILABLE_WORKER_TAG 	= 301;
	
	public Expressivity expr;
	private int myRank = -1;
	private boolean isConsistent;
	
	protected List<ABox> aboxList;
	
	public MPIALCStrategy(ABox abox, Expressivity expressivity) {
		super( abox );
		this.expr = expressivity;
	}
	
	public void complete(Expressivity expr) {
		//System.out.println("My rank = "+MPI.COMM_WORLD.Rank());  return;
		//initialize( expr );
		
		myRank = MPI.COMM_WORLD.Rank();
		int numProcs = MPI.COMM_WORLD.Size();
		double startTime = MPI.Wtime();
		
		//KryoSerializer.register();
		
		if (myRank == MASTER) {
			//System.out.println("Maanager");
			//initialize( expr );
			mpiOwlManager (numProcs - 1);

			/*int size[] = new int[1];
			byte sByteArray[] = KryoSerializer.serialize(abox);	//Serializer.serialize (abox);
			size[0] = sByteArray.length; System.out.println("Master : Length "+sByteArray.length); //KryoSerializer.ByteHex(sByteArray);
			MPI.COMM_WORLD.Send(size, 0, 1, MPI.INT, 1, CHECK_TAG);
			MPI.COMM_WORLD.Send(sByteArray, 0, size[0], MPI.BYTE, 1, ABOX);*/
		}
		else {
			//System.out.println("Worker");
			mpiOwlWorker();
			
			/*int count[] = new int[1];		
			MPI.COMM_WORLD.Recv(count, 0, 1, MPI.INT, MPI.ANY_SOURCE, CHECK_TAG);
			byte rByteArray[] = new byte[count[0]];		
			Status st = MPI.COMM_WORLD.Recv(rByteArray, 0, count[0], MPI.BYTE, MPI.ANY_SOURCE, ABOX);System.out.println("Worker : Length "+rByteArray.length);//KryoSerializer.ByteHex(rByteArray);
			ABox a = (ABox) KryoSerializer.deserialize(rByteArray, ABox.class);	//Serializer.deserialize(rByteArray);
			System.out.println("Object received");*/
		}
		
		double elapsedTime  = MPI.Wtime() - startTime;
		
		if (myRank == MASTER) {
			String str = "\n...................................................................\n";
			str += "###   Consistent : " + isConsistent +" || Time : " + elapsedTime + " seconds  || Workers : "+ (numProcs-1) +"   ###\n";
			str += "...................................................................";
			System.out.println(str);
			if( log.isLoggable( Level.FINE ) ) 
				log.fine( str );
		}
		
	}
	
	public void mpiOwlManager (int numOfWorkers) {
		aboxList = new ArrayList<ABox>(999);
		
		int numOfABoxes, numOfClosedABoxes = 0;
		int[] rBuf = new int[1];
		int[] sDummyBuf = new int[1];
		
		boolean[] workers = new boolean [numOfWorkers + 1]; //Index 0 will never be used. It is reserved for Master.
		Arrays.fill(workers, true);	//Initially all workers are available.
		
		aboxList.add(abox);
		numOfABoxes = aboxList.size();
		
		boolean isClosed = false, isCompleted = false;
		
		Status mStatus;
		sDummyBuf[0] = dummy;
		
		while (!isCompleted && !isClosed) {
			mStatus = MPI.COMM_WORLD.Recv(rBuf, 0, 1, MPI.INT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			if (mStatus.tag == WORK_REQ_TAG) {
				if (aboxList.size() > 0) {
					ABox sABox = aboxList.get(0);
					int size[] = new int[1];
					byte sByteArray[] = KryoSerializer.serialize (sABox);
					size[0] = sByteArray.length;
					MPI.COMM_WORLD.Send(size, 0, 1, MPI.INT, mStatus.source, CHECK_TAG);
					MPI.COMM_WORLD.Send(sByteArray, 0, size[0], MPI.BYTE, mStatus.source, ABOX);
					aboxList.remove(0);
					workers[mStatus.source] = false;
				}
				else {
					MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, mStatus.source, WAIT_TAG);
					workers[mStatus.source] = true;
				}
			}
			else if (mStatus.tag == AVAILABLE_WORKER_TAG) {		
				int availableWorkers = 0;
				for (int n = 1; n <= numOfWorkers; n++) {	//Index 0 is reserved for Master. So indices 1 to numOfWorker are valid.
					if (workers[n] == true)
						availableWorkers++;
				}
				int[] tempBuf = new int[1];
				tempBuf[0] = availableWorkers;
				MPI.COMM_WORLD.Send(tempBuf, 0, 1, MPI.INT, mStatus.source, AVAILABLE_WORKER_TAG);
			}
			else if (mStatus.tag == NEW_ABOX_TAG) {
				byte byteArray[] = new byte[rBuf[0]];
				MPI.COMM_WORLD.Recv(byteArray, 0, rBuf[0], MPI.BYTE, mStatus.source, NEW_ABOX);
				ABox mABox = (ABox) KryoSerializer.deserialize (byteArray, ABox.class);
				aboxList.add(mABox);	
				numOfABoxes++;
			}
			else if (mStatus.tag == CLASH_TAG) {
				numOfClosedABoxes++;
			}
			else if (mStatus.tag == COMPLETE_TAG) {
				isCompleted = true;
			}
			else {
				System.out.println("Error: Invalid TAG");
			}
		
			if (numOfABoxes == numOfClosedABoxes)
				isClosed = true;
 		}
		
		for (int j = 0; j < numOfWorkers; j++) {
			mStatus = MPI.COMM_WORLD.Recv(sDummyBuf, 0, 1, MPI.INT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, mStatus.source, STOP_TAG);
		}
		
		if (isCompleted) {
			isConsistent = true;
		}
		else if (isClosed) {
			isConsistent = false;
		}
		else {
			System.out.println("### Should not print!");
		}
	}
	
	public void mpiOwlWorker() {
		if( log.isLoggable( Level.FINE ) )
			log.fine( "Worker " + myRank + " has started" );
		
		int[] rBuf = new int[1];
		int[] sDummyBuf = new int[1];		
		
		sDummyBuf[0] = dummy;
		MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, MASTER, WORK_REQ_TAG);
		Status wStatus = MPI.COMM_WORLD.Recv(rBuf, 0, 1, MPI.INT, MASTER, MPI.ANY_TAG);
	
		while (wStatus.tag != STOP_TAG) {
			if (wStatus.tag == CHECK_TAG) {
				
				byte byteArray[] = new byte[rBuf[0]];
				
				MPI.COMM_WORLD.Recv(byteArray, 0, rBuf[0], MPI.BYTE, MASTER, ABOX);
				
				ABox wABox = (ABox) KryoSerializer.deserialize (byteArray, ABox.class);

				boolean isConsistent;
				if (wABox != null) {
					isConsistent = mpiIsConsistent (wABox);
					if (isConsistent) {
						MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, MASTER, COMPLETE_TAG);
					}
					else {
						MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, MASTER, CLASH_TAG);
					}
					
					MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, MASTER, WORK_REQ_TAG);
				}
				else {
					System.out.println("Error: wABox null");
				}				
			}
			else if (wStatus.tag == WAIT_TAG) {
				try {
				    Thread.sleep(100);                 //1000 milliseconds is one second. Should not be very less or high.
				} catch(InterruptedException ex) {
				    Thread.currentThread().interrupt();
				}
				
				MPI.COMM_WORLD.Send(sDummyBuf, 0, 1, MPI.INT, MASTER, WORK_REQ_TAG);
			}
			else {
				System.out.println("Error: Invalid TAG");
			}
			
			rBuf[0] = 0;
			wStatus = MPI.COMM_WORLD.Recv(rBuf, 0, 1, MPI.INT, MASTER, MPI.ANY_TAG);
		}
		
		if( log.isLoggable( Level.FINE ) )
			log.fine( "Worker " +myRank + " has terminated" );
	}
	
	public boolean mpiIsConsistent (ABox abox) {
		if( log.isLoggable( Level.FINE ) )
			log.fine( "### Worker "+myRank +" : Checking consistency on Branch ("+abox.getBranch() + ")" );
		
		initialize (abox);
		initialize(expr);
		
		while ( !abox.isComplete() ) {
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

				if( backtrack() ) {
					abox.setClash( null );
				}
				else {
					abox.setComplete( true );
				}
			}
			else {
				abox.setComplete( true );
			}
		}

		return !abox.isClosed();
	}
	
	protected boolean backtrack() {
		boolean branchFound = false;
		abox.stats.backtracks++;
		while( !branchFound ) {
			completionTimer.check();

			int lastBranch = abox.getClash().getDepends().max();

			// not more branches to try
			if( lastBranch <= 0 )
				return false;
			else if( lastBranch > abox.getBranches().size() )
				throw new InternalReasonerException( "Backtrack: Trying to backtrack to branch "
						+ lastBranch + " but has only " + abox.getBranches().size()
						+ " branches. Clash found: " + abox.getClash() );
			else if( PelletOptions.USE_INCREMENTAL_DELETION ) {
				// get the last branch
				Branch br = abox.getBranches().get( lastBranch - 1 );

				// if this is the last disjunction, merge pair, etc. for the
				// branch (i.e, br.tryNext == br.tryCount-1) and there are no
				// other branches to test (ie.
				// abox.getClash().depends.size()==2),
				// then update depedency index and return false
				if( (br.getTryNext() == br.getTryCount() - 1)
						&& abox.getClash().getDepends().size() == 2 ) {
					abox.getKB().getDependencyIndex().addCloseBranchDependency( br,
							abox.getClash().getDepends() );
					return false;
				}
			}

			List<Branch> branches = abox.getBranches();
			abox.stats.backjumps += (branches.size() - lastBranch);
			// CHW - added for incremental deletion support
			if( PelletOptions.USE_TRACING && PelletOptions.USE_INCREMENTAL_CONSISTENCY ) {
				// we must clean up the KB dependecny index
				List<Branch> brList = branches.subList( lastBranch, branches.size() );
				for( Iterator<Branch> it = brList.iterator(); it.hasNext(); ) {
					// remove from the dependency index
					abox.getKB().getDependencyIndex().removeBranchDependencies( it.next() );
				}
				brList.clear();
			}
			else {
				// old approach
				branches.subList( lastBranch, branches.size() ).clear();
			}

			// get the branch to try
			Branch newBranch = branches.get( lastBranch - 1 );

			if( log.isLoggable( Level.FINE ) )
				log.fine( "JUMP: Branch " + lastBranch );

			if( lastBranch != newBranch.getBranch() )
				throw new InternalReasonerException( "Backtrack: Trying to backtrack to branch "
						+ lastBranch + " but got " + newBranch.getBranch() );

			// set the last clash before restore
			if( newBranch.getTryNext() < newBranch.getTryCount() ) {
				newBranch.setLastClash( abox.getClash().getDepends() );
			}

			// increment the counter
			newBranch.setTryNext( newBranch.getTryNext() + 1 );

			// no need to restore this branch if we exhausted possibilities
			if( newBranch.getTryNext() < newBranch.getTryCount() ) {
				// undo the changes done after this branch
				restore( newBranch );
			}

			// try the next possibility
			branchFound = newBranch.tryNext();

			if( !branchFound ) {
				if( log.isLoggable( Level.FINE ) )
					log.fine( "FAIL: Branch " + lastBranch );
			}
		}

		return branchFound;
	}

}
