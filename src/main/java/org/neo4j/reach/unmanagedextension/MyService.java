package org.neo4j.reach.unmanagedextension;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.io.fs.FileUtils;


@Path("/reachability")
public class MyService {
    static String index_path = "/Users/wael/Documents/Neo4j/drwhoindex";
    static Result result;
    static ResourceIterator<Node> nodes;
    
    private final GraphDatabaseService database;
    private final IndexDB indexDb;
    
    
    /////////////////////////////////////////////////////////////////
    /////////////////   Initialize
    /////////////////////////////////////////////////////////////////
    public MyService(@Context GraphDatabaseService database)
    {
        boolean reset = false;
        // Delete index database if it exists and start over.
        
        this.database = database;
        File index = new File(index_path);
        if(reset){
            try {
                FileUtils.deleteRecursively(index);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.indexDb = new IndexDB().getInstance();
    }

    /////////////////////////////////////////////////////////////////
    /////////////////   Rest Calls
    /////////////////////////////////////////////////////////////////
    @GET
    @Produces( MediaType.TEXT_PLAIN )
    @Path( "/noindex/source/{s}/target/{t}" )
    public Response graphReachability(Long s, Long t){
        result = this.database.execute( "MATCH p=((s)-[*..50]->(t)) WHERE ID(s)="+s+" AND ID(t)="+t+" RETURN length(p) LIMIT 1" );
        ResourceIterator<Node> answer = result.columnAs("s");
        return Response.status( Status.OK ).entity(answer.hasNext()).build();
    }
    
    @GET
    @Path( "/source/{s}/target/{t}" )
    public String indexReachability(@PathParam("s") Long s, @PathParam("t") Long t){
        Long si = indexDb.mapping.get(s);
        Long ti = indexDb.mapping.get(t);

        // Get Start SCC node
        result = indexDb.connection.execute("match (s) WHERE ID(s) = "+si+" return s");
        ResourceIterator<Node> startSccResults = result.columnAs("s");
        Node startScc = startSccResults.next();

        // Get End SCC node
        result = indexDb.connection.execute( "match (t) WHERE ID(t) = "+ti+" return t" );
        ResourceIterator<Node> endSccResults = result.columnAs("t");
        Node endScc = endSccResults.next();
        
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

    @GET
    @Path("/hello/{name}")
    public String helloWorld(@PathParam("name") String name) {
        return "Hello World2"+name+"!\n";
    }

}
