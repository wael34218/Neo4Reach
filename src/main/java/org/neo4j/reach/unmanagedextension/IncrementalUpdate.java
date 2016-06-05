package org.neo4j.reach.unmanagedextension;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class IncrementalUpdate implements TransactionEventHandler<Object> {
	public IncrementalUpdate(){}
	
	@Override
	public void afterCommit(TransactionData data, Object arg1) {
		System.out.println("-----Commmit-----");
		// TODO Auto-generated method stub
		for(Relationship r : data.createdRelationships()){
			
		}
		
		for(Relationship r : data.deletedRelationships()){
			
		}
		
		for(Node n : data.createdNodes()){
			
		}
		
		for(Node n : data.deletedNodes()){
			
		}
	}

	@Override
	public void afterRollback(TransactionData arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object beforeCommit(TransactionData arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
