package org.neo4j.reach.unmanagedextension;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class IndexDB {
	private static IndexDB instance = null;

	public GraphDatabaseService connection;
	public GraphDatabaseService database;
	public Hashtable<Long, Long> mapping = new Hashtable<Long, Long>();
	static String index_path = "data/databases/reachindex";

	static Hashtable<Long, Integer> S_out = new Hashtable<Long, Integer>();
	static Hashtable<Long, Integer> S_in = new Hashtable<Long, Integer>();
	static Hashtable<Long, Boolean> Vorder = new Hashtable<Long, Boolean>();
	static Result result;
	static ResourceIterator<Node> nodes;
	private static final Label SCC = Label.label("SCC");
	
	private static enum RelTypes implements RelationshipType
	{
	    REACH
	}

	protected IndexDB() {
	}

	public static IndexDB getInstance() {
		if (instance == null) {
			System.out.println("Create new instance");
			instance = new IndexDB();
			instance.connection = new GraphDatabaseFactory().newEmbeddedDatabase(new File(index_path));
			instance.loadIndex();
		} else {
			System.out.println("Instance already created");
			instance.loadIndex();
		}
		return instance;
	}
	
	public void setDatabase(GraphDatabaseService database){
		instance.database = database;
	}

	public void loadIndex() {
		result = this.connection.execute("match (n) return n");
		nodes = result.columnAs("n");
		Node n;
		Set<Long> temp;
		this.mapping = new Hashtable<Long, Long>();

		while (nodes.hasNext()) {
			n = nodes.next();
			temp = fromString((String) n.getProperty("containing"));
			for (Long i : temp) {
				instance.mapping.put(i, n.getId());
			}
		}
	}

	public static Set<Long> fromString(String string) {
		Set<Long> result = new HashSet<Long>();
		if (string.equals("[]")) {
			return result;
		}

		String[] strings = string.replace("[", "").replace("]", "").split(", ");
		for (int i = 0; i < strings.length; i++) {
			result.add(Long.parseLong((strings[i])));
		}
		return result;
	}

	public boolean empty() {
		result = connection.execute("MATCH (n) RETURN count(n) as c;");
		ResourceIterator<Long> count = result.columnAs("c");
		Long nodes_total = (Long) count.next();
		System.out.println("= Nodes = : " + nodes_total);
		return (nodes_total == 0);
	}

	public boolean reachQuery(Long si, Long ti){
		Node startScc = getScc(si);
		Node endScc = getScc(ti);
		return reachQuery(startScc, endScc);
	}
	
	public boolean reachQuery(Node startScc, Node endScc){
		Set<Long> l_out_temp = IndexDB.fromString((String) startScc.getProperty("L_out"));
		Set<Long> l_in_temp = IndexDB.fromString((String) endScc.getProperty("L_in"));
		l_out_temp.add(startScc.getId());
		l_in_temp.add(endScc.getId());

		l_out_temp.retainAll(l_in_temp);
		if(l_out_temp.size() > 0){
			return true;
		}else{
			return false;
		}
	}
	
	public Node getScc(Long id){
		try {
			result = this.connection.execute("match (t) WHERE ID(t) = " + id + " return t");
			ResourceIterator<Node> endSccResults = result.columnAs("t");
			return endSccResults.next();
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Target: ==> " + id + " NOT FOUND");
		}
	}
	
	public void addNodeToProperty(Long added_node_id, Node node, String property){
		Set<Long> attr_list = fromString((String) node.getProperty(property));
		attr_list.add(added_node_id);
		node.setProperty(property, attr_list.toString());
	}
	
	public void removeNodeFromProperty(Long added_node_id, Node node, String property){
		Set<Long> attr_list = fromString((String) node.getProperty(property));
		attr_list.remove(added_node_id);
		node.setProperty(property, attr_list.toString());
	}
	/////////////////////////////////////////////////////////////////
	///////////////// Incremental Updates
	/////////////////////////////////////////////////////////////////
	
	//////////////////////////////////////
	// Add Node - Part 5.1
	//////////////////////////////////////
	public static void addSCC(Set<Long> outgoing, Set<Long> incoming, Set<Long> containing){
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Node v = instance.connection.createNode(SCC);
			for(Long o : outgoing){
				v.createRelationshipTo(instance.getScc(o), RelTypes.REACH);
			}
			for(Long i : outgoing){
				instance.getScc(i).createRelationshipTo(v, RelTypes.REACH);
			}
			v.setProperty("containing", containing.toString());
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
		decideVertixLevel(instance.connection);
		butterfly(instance.connection);
		
	}
	
	//////////////////////////////////////
	// Delete Node - Part 5.2
	//////////////////////////////////////
	public static void deleteSCC(Node v){
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Long order = (Long) v.getProperty("order");
			instance.connection.execute("MATCH (s)-[r?]-() WHERE ID(s) = " + v.getId() + " DELETE r, s");
			instance.connection.execute("MATCH (s) WHERE s.order > " + order + " SET s.order=s.order-1");
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
		decideVertixLevel(instance.connection);
		butterfly(instance.connection);
	}
	
	public static void createNode(Node n){
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Node v = instance.connection.createNode();
			v.setProperty("L_in", new HashSet<Long>().toString());
			v.setProperty("L_out", new HashSet<Long>().toString());
			v.setProperty("I_in", new HashSet<Long>().toString());
			v.setProperty("I_out", new HashSet<Long>().toString());
			Set<Long> containing = new HashSet<Long>();
			containing.add(n.getId());
			v.setProperty("containing", containing.toString());
			result = instance.connection.execute("MATCH (n) RETURN MAX(n.order) AS o");
			ResourceIterator<Long> count_query = result.columnAs("o");
			Long initial_order = (Long) count_query.next() + 1;
			v.setProperty("order", initial_order);
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void deleteNode(Node n){
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Long SccId = (Long) n.getProperty("_scc_id");
			Node scc = instance.getScc(SccId);
			Long order = (long) scc.getProperty("order");
			instance.connection.execute("MATCH (s) WHERE s.order > " + order + " SET s.order=s.order-1");
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void deleteRelationship(Relationship r){
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Long startSccId = instance.mapping.get(r.getStartNode().getId());
			Long endSccId = instance.mapping.get(r.getEndNode().getId());
			if (startSccId.equals(endSccId)) {
				boolean connected = false;
				Stack<Node> dfs_stack = new Stack<Node>();
				Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();
				dfs_stack.push(instance.getScc(endSccId));
				while (!dfs_stack.empty()){
					Node node = (Node) dfs_stack.pop();
					if (visited.containsKey(node.getId())) {
						continue;
					}
					if(startSccId.equals(node.getId())){
						connected = true;
						break;
					}
					for(Relationship rel : node.getRelationships(Direction.OUTGOING)){
						dfs_stack.push(rel.getEndNode());
					}
				}
				if(!connected){
					reIndex();
				}
			} else {
				result = instance.connection.execute("MATCH (s)-[r]->(t) WHERE ID(s)=" + startSccId + " AND ID(t)=" + endSccId
						+ " RETURN r LIMIT 1");
				ResourceIterator<Relationship> answer = result.columnAs("r");
				if (answer.hasNext()) {
					Relationship edge = answer.next();
					Long count = (Long) edge.getProperty("count");
					if(count > 1){
						edge.setProperty("count", count - 1);
					}else if(count == 1){
						edge.delete();
					}else{
						System.out.println("Edge with count less that 1!");
					}
					edge.setProperty("count", (Long) edge.getProperty("count") + (long) 1);
				} else {
					System.out.println("Edge does not exist in DAG!");
				}
			}
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void createRelationship(Relationship r) {
		try (Transaction tindex = instance.connection.beginTx(); Transaction tgraph = instance.database.beginTx()) {
			Long startSccId = instance.mapping.get(r.getStartNode().getId());
			Long endSccId = instance.mapping.get(r.getEndNode().getId());
			if (!startSccId.equals(endSccId)) {
				System.out.println("Case not same SCC : " + startSccId + " -> " + endSccId);
				if(instance.reachQuery(endSccId, startSccId)){
					System.out.println("They reach each other");
					// Combine all paths into one SCC
					Stack<Node> dfs_stack = new Stack<Node>();
					Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();
					dfs_stack.push(instance.getScc(endSccId));
					Node startScc = instance.getScc(startSccId);
					Set<Long> outgoing = new HashSet<Long>();
					Set<Long> incoming = new HashSet<Long>();
					Set<Long> containing = new HashSet<Long>();
					while (!dfs_stack.empty()){
						Node node = (Node) dfs_stack.pop();
						if (visited.containsKey(node.getId())) {
							continue;
						}
						visited.put(node.getId(), true);
						if(instance.reachQuery(node, startScc)){
							if(startSccId.equals(node.getId())){
								continue;
							}
							containing.addAll(fromString((String) startScc.getProperty("containing")));
							for(Relationship rel : node.getRelationships(Direction.OUTGOING)){
								outgoing.add(rel.getEndNode().getId());
								dfs_stack.push(rel.getEndNode());
							}
							for(Relationship rel : node.getRelationships(Direction.INCOMING)){
								incoming.add(rel.getEndNode().getId());
							}
							System.out.println("Delete SCC "+node.getId());
							deleteSCC(node);
						}
					}
					if(!containing.isEmpty()){
						System.out.println("Build new SCC");
						containing.addAll(fromString((String) startScc.getProperty("containing")));
						for(Relationship rel : startScc.getRelationships(Direction.OUTGOING)){
							outgoing.add(rel.getEndNode().getId());
						}
						for(Relationship rel : startScc.getRelationships(Direction.INCOMING)){
							incoming.add(rel.getEndNode().getId());
						}
						deleteSCC(startScc);
						System.out.println("Add SCC node");
						addSCC(outgoing, incoming, containing);
					}
				}else{
					System.out.println("They CANNOT reach each other");
					result = instance.connection.execute("MATCH (s)-[r]->(t) WHERE ID(s)=" + startSccId + " AND ID(t)=" + endSccId
							+ " RETURN r LIMIT 1");
					ResourceIterator<Relationship> answer = result.columnAs("r");
					if (answer.hasNext()) {
						
						Relationship edge = answer.next();
						System.out.println("Increment existing edge : " + edge.getProperty("count"));
						int count = (Integer) edge.getProperty("count") + 1;
						edge.setProperty("count", count);
					} else {
						System.out.println("Add new edge");
						Node startScc = instance.getScc(startSccId);
						Node endScc = instance.getScc(endSccId);
						Relationship edge = startScc.createRelationshipTo(endScc, RelTypes.REACH);
						edge.setProperty("count", 1);
						decideVertixLevel(instance.connection);
						butterfly(instance.connection);
						instance.loadIndex();
					}
				}
				tindex.success();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/////////////////////////////////////////////////////////////////
	///////////////// ReIndex
	/////////////////////////////////////////////////////////////////
	public static void reIndex() {
		instance.connection.shutdown();
		GraphDatabaseService indexDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(index_path));
		buildSCC(instance.database, indexDb);
		decideVertixLevel(indexDb);
		butterfly(indexDb);
		System.out.println("Indexing done.");
		indexDb.shutdown();
		instance.connection = new GraphDatabaseFactory().newEmbeddedDatabase(new File(index_path));
		instance.loadIndex();
	}

	// ////////////////////////////////////
	// BuildSCC - Part 2
	// ////////////////////////////////////
	public static void buildSCC(GraphDatabaseService graphDb, GraphDatabaseService indexDb) {
		// Formulate SCC using Kosaraju's algorithm
		Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();
		Stack<Node> scc_stack = new Stack<Node>();
		Stack<Node> dfs_stack = new Stack<Node>();
		Hashtable<Long, Set<Node>> scc = new Hashtable<Long, Set<Node>>();

		// Kosaraju's step 1: DFS
		Result result = graphDb.execute("match (n) return n");
		ResourceIterator<Node> nodes = result.columnAs("n");
		while (nodes.hasNext()) {
			dfs_stack.push(nodes.next());
			while (!dfs_stack.empty()) {
				Node node = (Node) dfs_stack.pop();
				if (visited.containsKey(node.getId())) {
					continue;
				}
				visited.put(node.getId(), false);
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING);
				for (Relationship rel : rels) {
					dfs_stack.push(rel.getEndNode());
				}
				scc_stack.push(node);
			}
		}

		// Kosaraju's step 2: Reversed graph DFS
		try (Transaction tindex = indexDb.beginTx(); Transaction tgraph = graphDb.beginTx()) {
			while (!scc_stack.empty()) {
				Node root = scc_stack.pop();
				if (visited.get(root.getId())) {
					continue;
				}
				dfs_stack.push(root);
				Node sccNode = indexDb.createNode(SCC);

				Set<Node> component = new HashSet<Node>();
				while (!dfs_stack.empty()) {
					Node node = (Node) dfs_stack.pop();
					if (visited.get(node.getId())) {
						continue;
					}
					visited.put(node.getId(), true);

					component.add(node);
					node.setProperty("_scc_id", sccNode.getId());
					instance.mapping.put(node.getId(), sccNode.getId());
					Iterable<Relationship> rels = node.getRelationships(Direction.INCOMING);
					for (Relationship rel : rels) {
						dfs_stack.push(rel.getStartNode());
					}
				}

				// Store SCC node in the index database with "containing"
				// property: a serialized string of node ids.
				Set<Long> containing = new HashSet<Long>();
				for (Node c : component) {
					containing.add(c.getId());
				}
				sccNode.setProperty("containing", containing.toString());
				// sccNode.setProperty("order", sccNode.getId());
				scc.put(sccNode.getId(), component);
			}
			tindex.success();
			tgraph.success();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Add relations
		result = graphDb.execute("match ()-[r]->() return r");
		ResourceIterator<Relationship> relations = result.columnAs("r");
		Relationship rel;
		Long startSccId, endSccId;
		Node startScc, endScc;
		Hashtable<String, Boolean> added_relations = new Hashtable<String, Boolean>();

		try (Transaction tindex = indexDb.beginTx()) {
			while (relations.hasNext()) {
				rel = relations.next();
				startSccId = instance.mapping.get(rel.getStartNode().getId());
				endSccId = instance.mapping.get(rel.getEndNode().getId());
				if (!startSccId.equals(endSccId)) {
					if (!added_relations.containsKey(startSccId + "-" + endSccId)) {
						added_relations.put(startSccId + "-" + endSccId, true);

						// Get Start SCC node
						result = indexDb.execute("match (s) WHERE ID(s) = " + startSccId + " return s");
						ResourceIterator<Node> startSccResults = result.columnAs("s");
						startScc = startSccResults.next();

						// Get End SCC node
						result = indexDb.execute("match (s) WHERE ID(s) = " + endSccId + " return s");
						ResourceIterator<Node> endSccResults = result.columnAs("s");
						endScc = endSccResults.next();

						// Store Relation
						Relationship relationship = startScc.createRelationshipTo(endScc, RelTypes.REACH);
						relationship.setProperty("count", 1);
					} else {
						result = indexDb.execute("match (s)-[r]->(t) WHERE ID(s)=" + startSccId + " AND ID(t)="
								+ endSccId + " SET r.count=r.count+1");
					}
				}
			}
			tindex.success();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// ////////////////////////////////////
	// Decide Vertex Level - Part 7.1
	// ////////////////////////////////////
	private static void decideVertixLevel(GraphDatabaseService indexDb) {
		Long order_level = (long) 1;
		S_out = new Hashtable<Long, Integer>();
		S_in = new Hashtable<Long, Integer>();
		Vorder = new Hashtable<Long, Boolean>();

		try (Transaction tindex = indexDb.beginTx()) {
			result = indexDb.execute("MATCH (n) RETURN count(n) as c");
			ResourceIterator<Long> count_query = result.columnAs("c");
			Long nodes_total = (Long) count_query.next();

			while (order_level <= nodes_total) {
				result = indexDb.execute("MATCH (n) RETURN n");
				nodes = result.columnAs("n");
				Long count = (long) 0;
				while (nodes.hasNext()) {
					Node node = nodes.next();
					if (Vorder.containsKey(node.getId())) {
						S_out.put(node.getId(), -1);
						S_in.put(node.getId(), -1);
						continue;
					}
					findOut(node);
					findIn(node);
					count += 1;
				}
				
				Long maxNodeID = findMax();
				System.out.println(count);
				result = indexDb.execute("MATCH (n) WHERE ID(n)=" + maxNodeID + " SET n.order=" + order_level);
				Vorder.put(maxNodeID, true);
				order_level++;
			}
			//indexDb.execute("CREATE CONSTRAINT ON (s:SCC) ASSERT s.order IS UNIQUE");
			tindex.success();
		}
	}

	private static Long findMax() {
		// Loop through nodes and find the maximum f(v,G) =
		// (|Sin(v,G)|.|Sout(v,G)| + |Sin(v,G)| + |Sout(v,G)|) / (|Sin(v,G)| +
		// |Sout(v,G)|)
		Long nodeID = (long) -1;
		float max_cost = -1;
		float cost;
		for (Long k : S_out.keySet()) {
			if (Vorder.containsKey(k)) {
				continue;
			}
			if (S_in.get(k) + S_out.get(k) == 0) {
				cost = 0;
			} else {
				cost = (S_in.get(k) * S_out.get(k) + S_in.get(k) + S_out.get(k)) / (S_in.get(k) + S_out.get(k));
			}
			if (cost > max_cost) {
				nodeID = k;
				max_cost = cost;
			}
		}
		System.out.println(nodeID + " : " + max_cost);
		return nodeID;
	}

	private static int findIn(Node node) {
		if (S_in.containsKey(node.getId())) {
			return S_in.get(node.getId());
		}

		int in = 0;
		Iterable<Relationship> rels = node.getRelationships(Direction.INCOMING);
		for (Relationship rel : rels) {
			if (Vorder.containsKey(rel.getStartNode().getId())) {
				continue;
			}
			in += findIn(rel.getStartNode()) + 1;
		}
		S_in.put(node.getId(), in);
		return in;
	}

	private static int findOut(Node node) {
		if (S_out.containsKey(node.getId())) {
			return S_out.get(node.getId());
		}

		int out = 0;
		Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING);
		for (Relationship rel : rels) {
			if (Vorder.containsKey(rel.getEndNode().getId())) {
				continue;
			}
			out += findOut(rel.getEndNode()) + 1;
		}
		S_out.put(node.getId(), out);
		return out;
	}

	// ////////////////////////////////////
	// Butterfly - Part 7.2
	// ////////////////////////////////////
	public static void butterfly(GraphDatabaseService indexDb) {
		Hashtable<Long, Set<Long>> l_out = new Hashtable<Long, Set<Long>>();
		Hashtable<Long, Set<Long>> l_in = new Hashtable<Long, Set<Long>>();
		Hashtable<Long, Set<Long>> i_out = new Hashtable<Long, Set<Long>>();
		Hashtable<Long, Set<Long>> i_in = new Hashtable<Long, Set<Long>>();
		Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();

		try (Transaction tindex = indexDb.beginTx()) {
			result = indexDb.execute("MATCH (n) RETURN n ORDER BY n.order");
			nodes = result.columnAs("n");
			while (nodes.hasNext()) {
				Queue<Node> bfs_queue = new LinkedList<Node>();
				ArrayList<Node> b_plus = new ArrayList<Node>();
				ArrayList<Node> b_minus = new ArrayList<Node>();

				Node node = nodes.next();
				visited.put(node.getId(), true);

				// Find B Plus
				bfs_queue.add(node);
				while (!bfs_queue.isEmpty()) {
					Node bn = bfs_queue.remove();
					Iterable<Relationship> rels = bn.getRelationships(Direction.OUTGOING);
					for (Relationship rel : rels) {
						if (visited.containsKey(rel.getEndNode().getId())) {
							// Consider this node is deleted from the graph
							continue;
						}
						// if((Long) bn.getProperty("order") > (Long)
						// rel.getEndNode().getProperty("order")){
						bfs_queue.add(rel.getEndNode());
						b_plus.add(rel.getEndNode());
						// }
					}
				}

				// Find B Minus
				bfs_queue.add(node);
				while (!bfs_queue.isEmpty()) {
					Node bn = bfs_queue.remove();
					Iterable<Relationship> rels = bn.getRelationships(Direction.INCOMING);

					for (Relationship rel : rels) {
						if (visited.containsKey(rel.getStartNode().getId())) {
							// Consider this node is deleted from the graph
							continue;
						}

						// if((Long) bn.getProperty("order") < (Long)
						// rel.getStartNode().getProperty("order")){
						bfs_queue.add(rel.getStartNode());
						b_minus.add(rel.getStartNode());
						// }
					}
				}

				// Update L_in
				System.out.println("---- bplus : " + b_plus.size());
				for (Node u : b_plus) {
					Set<Long> l_out_temp = new HashSet<Long>();
					Set<Long> l_in_temp = new HashSet<Long>();

					if (l_out.containsKey(node.getId())) {
						l_out_temp = l_out.get(node.getId());
					}
					if (l_in.containsKey(u.getId())) {
						l_in_temp = l_in.get(u.getId());
					}

					l_out_temp.retainAll(l_in_temp);
					if (l_out_temp.size() == 0) {
						l_in_temp.add(node.getId());
						l_in.put(u.getId(), l_in_temp);
					}
				}

				// Update L_out
				System.out.println("---- BMINUS : " + b_minus.size());
				for (Node u : b_minus) {
					Set<Long> l_out_temp = new HashSet<Long>();
					Set<Long> l_in_temp = new HashSet<Long>();

					if (l_out.containsKey(u.getId())) {
						l_out_temp = l_out.get(u.getId());
					}
					if (l_in.containsKey(node.getId())) {
						l_in_temp = l_in.get(node.getId());
					}

					l_in_temp.retainAll(l_out_temp);
					if (l_in_temp.size() == 0) {
						l_out_temp.add(node.getId());
						l_out.put(u.getId(), l_out_temp);
					}
				}
			}
			
			// Compute I_in and I_out which are inverse lookup for L_in and L_out
			for(Entry<Long, Set<Long>> entry : l_out.entrySet()) {
				for(Long n : entry.getValue()){
					Set<Long> i_out_temp = new HashSet<Long>();
					if (l_out.containsKey(n)) {
						i_out_temp = l_out.get(n);
					}
					i_out_temp.add(entry.getKey());
					i_out.put(n, i_out_temp);
				}
			}
			for(Entry<Long, Set<Long>> entry : l_in.entrySet()) {
				for(Long n : entry.getValue()){
					Set<Long> i_in_temp = new HashSet<Long>();
					if (l_in.containsKey(n)) {
						i_in_temp = l_in.get(n);
					}
					i_in_temp.add(entry.getKey());
					i_in.put(n, i_in_temp);
				}
			}

			// Store L_out and L_in in Neo4j
			result = indexDb.execute("MATCH (n) RETURN n ORDER BY n.order");
			nodes = result.columnAs("n");
			while (nodes.hasNext()) {
				Node node = nodes.next();
				Set<Long> l_out_temp = new HashSet<Long>();
				Set<Long> l_in_temp = new HashSet<Long>();
				Set<Long> i_out_temp = new HashSet<Long>();
				Set<Long> i_in_temp = new HashSet<Long>();

				if (l_out.containsKey(node.getId())) {
					l_out_temp = l_out.get(node.getId());
				}
				if (l_in.containsKey(node.getId())) {
					l_in_temp = l_in.get(node.getId());
				}
				if (i_out.containsKey(node.getId())) {
					i_out_temp = i_out.get(node.getId());
				}
				if (i_in.containsKey(node.getId())) {
					i_in_temp = i_in.get(node.getId());
				}

				node.setProperty("L_out", l_out_temp.toString());
				node.setProperty("L_in", l_in_temp.toString());
				node.setProperty("I_out", i_out_temp.toString());
				node.setProperty("I_in", i_in_temp.toString());
			}
			tindex.success();
		}
	}
}
