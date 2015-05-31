// Copyright (c) 2006 - 2008, Clark & Parsia, LLC. <http://www.clarkparsia.com>
// This source code is available under the terms of the Affero General Public
// License v3.
//
// Please see LICENSE.txt for full license terms, including the availability of
// proprietary exceptions.
// Questions, comments, or requests for clarification: licensing@clarkparsia.com

package org.mindswap.pellet.tableau.completion.rule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import mpi.MPI;

import org.mindswap.pellet.ABox;
import org.mindswap.pellet.Clash;
import org.mindswap.pellet.DependencySet;
import org.mindswap.pellet.Individual;
import org.mindswap.pellet.Node;
import org.mindswap.pellet.PelletOptions;
import org.mindswap.pellet.exceptions.InternalReasonerException;
import org.mindswap.pellet.tableau.branch.DisjunctionBranch;
import org.mindswap.pellet.tableau.completion.CompletionStrategy;
import org.mindswap.pellet.tableau.completion.queue.NodeSelector;
import org.mindswap.pellet.utils.ATermUtils;

import aterm.ATermAppl;
import aterm.ATermList;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2009
 * </p>
 * <p>
 * Company: Clark & Parsia, LLC. <http://www.clarkparsia.com>
 * </p>
 * 
 * @author Evren Sirin
 */
public class DisjunctionRule extends AbstractTableauRule {
	public DisjunctionRule(CompletionStrategy strategy) {
		super( strategy, NodeSelector.DISJUNCTION, BlockingType.COMPLETE );
	}

	public void apply(Individual node) {
		if( !node.canApply( Node.OR ) )
			return;

		List<ATermAppl> types = node.getTypes( Node.OR );

		int size = types.size();
		ATermAppl[] disjunctions = new ATermAppl[size - node.applyNext[Node.OR]];
		types.subList( node.applyNext[Node.OR], size ).toArray( disjunctions );
		if( PelletOptions.USE_DISJUNCTION_SORTING != PelletOptions.NO_SORTING )
			sortDisjunctions( node, disjunctions );

		for( int j = 0, n = disjunctions.length; j < n; j++ ) {
			ATermAppl disjunction = disjunctions[j];

			applyDisjunctionRule( node, disjunction );

			if( strategy.getABox().isClosed() || node.isMerged() )
				return;
		}
		node.applyNext[Node.OR] = size;
	}

	private static void sortDisjunctions(final Individual node, ATermAppl[] disjunctions) {
		if( PelletOptions.USE_DISJUNCTION_SORTING == PelletOptions.OLDEST_FIRST ) {
			Comparator<ATermAppl> comparator = new Comparator<ATermAppl>() {
				public int compare(ATermAppl d1, ATermAppl d2) {
					return node.getDepends( d1 ).max() - node.getDepends( d2 ).max();
				}
			};

			Arrays.sort( disjunctions, comparator );
		}
		else
			throw new InternalReasonerException( "Unknown disjunction sorting option "
					+ PelletOptions.USE_DISJUNCTION_SORTING );
	}

	/**
	 * Apply the disjunction rule to an specific label for an individual
	 * 
	 * @param node
	 * @param disjunction
	 */
	protected void applyDisjunctionRule(Individual node, ATermAppl disjunction) {
		// disjunction is now in the form not(and([not(d1), not(d2), ...]))
		ATermAppl a = (ATermAppl) disjunction.getArgument( 0 );
		ATermList disjuncts = (ATermList) a.getArgument( 0 );
		ATermAppl[] disj = new ATermAppl[disjuncts.getLength()];

		for( int index = 0; !disjuncts.isEmpty(); disjuncts = disjuncts.getNext(), index++ ) {
			disj[index] = ATermUtils.negate( (ATermAppl) disjuncts.getFirst() );
			if( node.hasType( disj[index] ) )
				return;
		}
		
		mpiApplyDisjunctionRule (node, disjunction, disj);

//		DisjunctionBranch newBranch = new DisjunctionBranch( strategy.getABox(), strategy, node,
//				disjunction, node.getDepends( disjunction ), disj );
//		strategy.addBranch( newBranch );
//
//		newBranch.tryNext();
	}
	
	protected void mpiApplyDisjunctionRule (Individual node, ATermAppl disjunction, ATermAppl[] disj) {
		ABox abox = strategy.getABox();
		int j, k;
		
		for ( j = 0; j < disj.length; j++) {
			ATermAppl d = disj[j];
			DependencySet ds = node.getDepends( disjunction );
			ATermAppl notD = ATermUtils.negate(d);
			DependencySet clashDepends = node.getDepends(notD);
			if(clashDepends == null) {
			    strategy.addType(node, d, ds);
				// we may still find a clash if concept is allValuesFrom
				// and there are some conflicting edges
				if(abox.isClosed()) 
					clashDepends = abox.getClash().getDepends();
			}
			else {
			    clashDepends = clashDepends.union(ds, abox.doExplanation());
			}
			
			// if there is a clash
			if(clashDepends != null) {
				if( log.isLoggable( Level.FINE ) ) {
					Clash clash = abox.isClosed() ? abox.getClash() : Clash.atomic(node, clashDepends, d);
		            log.fine("CLASH: Branch " + abox.getBranch() + " " + clash + "!" + " " + clashDepends.getExplain());
				}
			}
			else {
				break;
			}
		}
		// If there are more ABoxes, send them to the manager.
		ATermAppl nodeName = node.getTerm();
		
		for ( k = j; k < disj.length; k++ ) {
			ABox newAbox = abox.copy();
			Node newNode = newAbox.getNode(nodeName);
			
			ATermAppl d = disj[k];
			DependencySet ds = newNode.getDepends( disjunction );
			ATermAppl notD = ATermUtils.negate(d);
			DependencySet clashDepends = newNode.getDepends(notD);
			
			if(clashDepends == null) {	
			    strategy.addType(newNode, d, ds);
				// we may still find a clash if concept is allValuesFrom
				// and there are some conflicting edges
				if(newAbox.isClosed()) 
					clashDepends = newAbox.getClash().getDepends();
			}
			else {
			    clashDepends = clashDepends.union(ds, newAbox.doExplanation());
			}
			
			// if there is a clash
			if(clashDepends != null) {
				if( log.isLoggable( Level.FINE ) ) {
					Clash clash = newAbox.isClosed() ? newAbox.getClash() : Clash.atomic(newNode, clashDepends, d);
		            log.fine("CLASH: Branch " + newAbox.getBranch() + " " + clash + "!" + " " + clashDepends.getExplain());
				}
			}
			else {	//MASTER = 0;	NEW_ABOX_TAG = 204;
				MPI.COMM_WORLD.Send(newAbox, 0, 1, MPI.OBJECT, 0, 204);
				newAbox = null;
			}
		}
	}
	
}



