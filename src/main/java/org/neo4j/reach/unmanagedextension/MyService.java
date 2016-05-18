package org.neo4j.reach.unmanagedextension;

import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;


@Path("/reachability")
public class MyService {
    static Result result;
    static ResourceIterator<Node> nodes;
    
    private final GraphDatabaseService database;
    private final IndexDB indexDb;
    
    
    /////////////////////////////////////////////////////////////////
    /////////////////   Initialize
    /////////////////////////////////////////////////////////////////
    public MyService(@Context GraphDatabaseService database){
        this.database = database;
        new IndexDB();
		this.indexDb = IndexDB.getInstance();
		if(indexDb.empty()){
			indexDb.reIndex(database);
		}else{
			indexDb.loadIndex();
		}
    }

    /////////////////////////////////////////////////////////////////
    /////////////////   Rest Calls
    /////////////////////////////////////////////////////////////////
    @GET
    @Path( "/noindex/source/{s}/target/{t}" )
    public String graphReachability(@PathParam("s") Long s, @PathParam("t") Long t){
        result = this.database.execute( "MATCH p=((s)-[*..50]->(t)) WHERE ID(s)="+s+" AND ID(t)="+t+" RETURN length(p) LIMIT 1" );
        ResourceIterator<Node> answer = result.columnAs("s");
        if(answer.hasNext()){
        	return "true\n";
        }else{
        	return "false\n";
        }
    }
    
    @GET
    @Path( "/source/{s}/target/{t}" )
    public String indexReachability(@PathParam("s") Long s, @PathParam("t") Long t){
        Long si = indexDb.mapping.get(s);
        Long ti = indexDb.mapping.get(t);
        Node startScc = null;
        Node endScc = null;

        // Get Start SCC node
        try{
	        result = indexDb.connection.execute("match (s) WHERE ID(s) = "+si+" return s");
	        ResourceIterator<Node> startSccResults = result.columnAs("s");
	        startScc = startSccResults.next();
        }catch(RuntimeException e){
        	return "Source: " + s + " ==> " + si + " NOT FOUND";
        }

        // Get End SCC node
        try{
	        result = indexDb.connection.execute( "match (t) WHERE ID(t) = "+ti+" return t" );
	        ResourceIterator<Node> endSccResults = result.columnAs("t");
	        endScc = endSccResults.next();
	    }catch(RuntimeException e){
	    	return "Target: " + t + " ==> " + ti + " NOT FOUND";
	    }
        
        Set<Long> l_out_temp = IndexDB.fromString((String) startScc.getProperty("l_out"));
        Set<Long> l_in_temp = IndexDB.fromString((String) endScc.getProperty("l_in"));
        l_out_temp.add(startScc.getId());
        l_in_temp.add(endScc.getId());

        l_out_temp.retainAll(l_in_temp);
        
        String res;
        if(l_out_temp.size() == 0){
            res = "false\n";
        }else{
            res = "true\n";
        }
        return res;
    }

    @GET
    @Path("/helloworld")
    public String helloWorld() {
        return "Hello World!\n";
    }

}
