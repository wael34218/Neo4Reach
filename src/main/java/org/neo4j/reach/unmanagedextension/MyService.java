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
	///////////////// Initialize
	/////////////////////////////////////////////////////////////////
	public MyService(@Context GraphDatabaseService database) {
		this.database = database;
		new IndexDB();
		this.indexDb = IndexDB.getInstance();
		indexDb.setDatabase(database);
		if (indexDb.empty()) {
			IndexDB.reIndex();
		}
		this.database.registerTransactionEventHandler(new IncrementalUpdate());
	}

	/////////////////////////////////////////////////////////////////
	///////////////// Rest Calls
	/////////////////////////////////////////////////////////////////
	@GET
	@Path("/noindex/source/{s}/target/{t}")
	public String graphReachability(@PathParam("s") Long s, @PathParam("t") Long t) {
		result = this.database.execute("MATCH p=((s)-[*..50]->(t)) WHERE ID(s)=" + s + " AND ID(t)=" + t
				+ " RETURN length(p) LIMIT 1");
		ResourceIterator<Node> answer = result.columnAs("s");
		if (answer.hasNext()) {
			return "true\n";
		} else {
			return "false\n";
		}
	}

	@GET
	@Path("/source/{s}/target/{t}")
	public String indexReachability(@PathParam("s") Long s, @PathParam("t") Long t) {
		Long si = indexDb.mapping.get(s);
		Long ti = indexDb.mapping.get(t);
		
		String res;
		if (indexDb.reachQuery(si, ti)) {
			res = "false\n";
		} else {
			res = "true\n";
		}
		return res;
	}
	
	@GET
	@Path("/get_scc/{s}")
	public String getScc(@PathParam("s") Long s) {
		Long si = indexDb.mapping.get(s);
		return si+"\n";
	}
	
	@GET
	@Path("/create_index")
	public String createIndex() {
		IndexDB.reIndex();
		return "Done\n";
	}

	@GET
	@Path("/helloworld")
	public String helloWorld() {
		return "Hello World!\n";
	}

}
