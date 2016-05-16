package org.neo4j.reach.unmanagedextension;

import java.io.File;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class IndexDB {
	private static IndexDB instance = null;
	public GraphDatabaseService connection;
	public Hashtable<Long, Long> mapping = new Hashtable<Long, Long>();
	static String index_path = "/Users/wael/Documents/Neo4j/drwhoindex";
	
	protected IndexDB()
    {
    }
	
	public static IndexDB getInstance() {
		if(instance == null){
			System.out.println("Create New INSTANCE ====================");
			instance = new IndexDB();
			instance.connection = new GraphDatabaseFactory().newEmbeddedDatabase(new File(index_path));
			loadIndex();
			System.out.println("Loaded ====================");
		}else{
			System.out.println("Instance already created ====================");
		}
		return instance;
    }
	
	public static void loadIndex(){
		Result result = instance.connection.execute("match (n) return n");
        ResourceIterator<Node> nodes = result.columnAs("n");
        Node n;
        Set<Long> temp;
        
        while(nodes.hasNext()){
            n = nodes.next();
            temp = fromString((String) n.getProperty("containing"));
            for(Long i : temp){
            	instance.mapping.put(i, n.getId());
            }
        }
    }
	
	public static Set<Long> fromString(String string) {
        Set<Long> result = new HashSet<Long>();
        if(string.equals("[]")){
            return result;
        }
        
        String[] strings = string.replace("[", "").replace("]", "").split(", ");
        for (int i = 0; i < strings.length; i++) {
          result.add(Long.parseLong((strings[i])));
        }
        return result;
    }
}
