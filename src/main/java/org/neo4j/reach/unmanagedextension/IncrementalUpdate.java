package org.neo4j.reach.unmanagedextension;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class IncrementalUpdate implements TransactionEventHandler<Object> {
	public IncrementalUpdate(){}
	
	@Override
	public void afterCommit(TransactionData data, Object arg1) {
		System.out.println("-----After Commmit-----");
		
		for(Node n : data.createdNodes()){
			IndexDB.createNode(n);
		}
		
		for(Relationship r : data.createdRelationships()){
			System.out.println("1- Create Relationship");
			IndexDB.createRelationship(r);
		}
		
		for(Relationship r : data.deletedRelationships()){
			IndexDB.deleteRelationship(r);
		}
		
		for(Node n : data.deletedNodes()){
			for(Relationship r : n.getRelationships()){
				IndexDB.deleteRelationship(r);
			}
			IndexDB.deleteNode(n);
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
