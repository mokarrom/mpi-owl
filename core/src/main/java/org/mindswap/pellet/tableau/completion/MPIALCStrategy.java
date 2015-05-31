package org.mindswap.pellet.tableau.completion;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import mpi.*;

import org.mindswap.pellet.ABox;
import org.mindswap.pellet.IndividualIterator;
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
	static final int WORK_REQ_TAG 	= 201;
	static final int CLASH_TAG 		= 202;
	static final int COMPLETE_TAG 	= 203;
	static final int NEW_ABOX_TAG 	= 204;
	
	protected List<ABox> aboxList;
	protected List<ABox> closedAboxList;
	
	public MPIALCStrategy(ABox abox) {
		super( abox );
	}
	
	public void complete(Expressivity expr) {
		
		int rank = MPI.COMM_WORLD.Rank();
		int numProcs = MPI.COMM_WORLD.Size();
		
		if (rank == MASTER) {
			System.out.println("Maanager");
			mpiOwlManager (numProcs - 1);
		}
		else {
			System.out.println("Worker");
			initialize( expr );
			mpiOwlWorker();
		}
	}
	
	public void mpiOwlManager(int numWorker){
		aboxList = new ArrayList<ABox>();
		closedAboxList = new ArrayList<ABox>();
		ABox[] D = new ABox[numWorker];
		
		aboxList.add(abox);
		
		boolean isClosed = false, isCompleted = false;
		ABox mABox = null;
		Status mStatus;
		int cnt = 0;
		
		while (!isCompleted && !isClosed) {
			mStatus = MPI.COMM_WORLD.Recv(mABox, 0, 1, MPI.OBJECT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			if (mStatus.tag == WORK_REQ_TAG) {
				if (cnt < aboxList.size()) {
					ABox sABox = aboxList.get(cnt);
					MPI.COMM_WORLD.Send(sABox, 0, 1, MPI.OBJECT, mStatus.source, CHECK_TAG);
					D[mStatus.source] = sABox;
					cnt++;
				}
				else {
					MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, mStatus.source, WAIT_TAG);
				}
			}
			else if (mStatus.tag == NEW_ABOX_TAG) {
				if (mABox != null)
					aboxList.add(mABox);
				else
					System.out.println("Error: NEW_ABOX_TAG");
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
		
		for (int j = 0; j < numWorker; j++) {
			mStatus = MPI.COMM_WORLD.Recv(null, 0, 0, MPI.OBJECT, MPI.ANY_SOURCE, MPI.ANY_TAG);
			MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, mStatus.source, STOP_TAG);
		}
		
	}
	
	public void mpiOwlWorker() {
		ABox wABox = null;
		
		MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, MASTER, WORK_REQ_TAG);
		Status wStatus = MPI.COMM_WORLD.Recv(wABox, 0, 1, MPI.OBJECT, MASTER, MPI.ANY_TAG);
System.out.println("Chk # 1");		
		while (wStatus.tag != STOP_TAG) {
			if (wStatus.tag == CHECK_TAG) {
				boolean isConsistent;
				if (wABox != null) {
					isConsistent = mpiIsConsistent (wABox);
					if (isConsistent) {
						MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, MASTER, COMPLETE_TAG);
					}
					else {
						MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, MASTER, CLASH_TAG);
					}
					
					MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, MASTER, WORK_REQ_TAG);
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
				
				MPI.COMM_WORLD.Send(null, 0, 0, MPI.OBJECT, MASTER, WORK_REQ_TAG);
			}
			else {
				System.out.println("Error: Invalid TAG");
			}
System.out.println("Chk # 2");			
			wStatus = MPI.COMM_WORLD.Recv(wABox, 0, 1, MPI.OBJECT, MASTER, MPI.ANY_TAG);
		}
	}
	
	public boolean mpiIsConsistent (ABox abox) {
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
