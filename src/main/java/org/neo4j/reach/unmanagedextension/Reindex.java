package org.neo4j.reach.unmanagedextension;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Reindex {
    static Hashtable<Long, Integer> S_out = new Hashtable<Long, Integer>();
    static Hashtable<Long, Integer> S_in = new Hashtable<Long, Integer>();
    static Hashtable<Long, Boolean> Vorder = new Hashtable<Long, Boolean>();
    static String index_path = "/Users/wael/Documents/Neo4j/drwhoindex";
    static String graph_path = "/Users/wael/Documents/Neo4j/drwho";
    static Result result;
    static ResourceIterator<Node> nodes;
    static Hashtable<Long, Long> mapping = new Hashtable<Long, Long>();
    
    public Reindex(){
        File file = new File(graph_path);
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(file);
        File index = new File(index_path);
        GraphDatabaseService indexDb = new GraphDatabaseFactory().newEmbeddedDatabase(index);

        try (Transaction tdb = graphDb.beginTx(); Transaction tindex = indexDb.beginTx()) {
            // Delete index database before re-indexing
            buildSCC(graphDb, indexDb);
            System.out.println("SCC Done");

            decideVertixLevel(indexDb);
            System.out.println("Decide Vertex Level Done");

            butterfly(indexDb);
            System.out.println("Butterfly Done");
            
            tindex.success();
        }
    }
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////   Build SSC - Part 2
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void buildSCC(GraphDatabaseService graphDb, GraphDatabaseService indexDb){
        // Formulate SCC using Kosaraju's algorithm
        Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();
        Stack<Node> scc_stack = new Stack<Node>();
        Stack<Node> dfs_stack = new Stack<Node>();
        Hashtable<Long, Set<Node>> scc = new Hashtable<Long, Set<Node>>();
        
        // Kosaraju's step 1: DFS
        result = graphDb.execute("match (n) return n");
        nodes = result.columnAs("n");
        while(nodes.hasNext()){
            dfs_stack.push(nodes.next());
            while(!dfs_stack.empty()){
                Node node = (Node) dfs_stack.pop();
                if(visited.containsKey(node.getId())){
                    continue;
                }
                visited.put(node.getId(), false);
                Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING);
                for(Relationship rel : rels){
                    dfs_stack.push(rel.getEndNode());
                }
                scc_stack.push(node);
            }
        }

        // Kosaraju's step 2: Reversed graph DFS
        while(!scc_stack.empty()){
            Node root = scc_stack.pop();
            if(visited.get(root.getId())){
                continue;
            }
            dfs_stack.push(root);
            Node sccNode = indexDb.createNode();

            Set<Node> component = new HashSet<Node>();
            while(!dfs_stack.empty()){
                Node node = (Node) dfs_stack.pop();
                if(visited.get(node.getId())){
                    continue;
                }
                visited.put(node.getId(), true);

                component.add(node);
                mapping.put(node.getId(), sccNode.getId());
                Iterable<Relationship> rels = node.getRelationships(Direction.INCOMING);
                for(Relationship rel : rels){
                    dfs_stack.push(rel.getStartNode());
                }
            }

            // Store SCC node in the index database with "containing" property: a serialized string of node ids.
            Set<Long> containing = new HashSet<Long>();
            for(Node c : component){
                containing.add(c.getId());
            }
            sccNode.setProperty("containing", containing.toString());
            sccNode.setProperty("order", sccNode.getId());
            scc.put(sccNode.getId(), component);
        }

        // Add relations
        result = graphDb.execute( "match ()-[r]->() return r" );
        ResourceIterator<Relationship> relations = result.columnAs("r");
        Relationship rel;
        Long startSccId, endSccId;
        Node startScc, endScc;
        Hashtable<String, Boolean> added_relations = new Hashtable<String, Boolean>();

        while(relations.hasNext()){
            rel = relations.next();
            startSccId = mapping.get(rel.getStartNode().getId());
            endSccId = mapping.get(rel.getEndNode().getId());
            if(!startSccId.equals(endSccId) && !added_relations.containsKey(startSccId+"-"+endSccId)){
                added_relations.put(startSccId+"-"+endSccId, true);

                // Get Start SCC node
                result = indexDb.execute( "match (s) WHERE ID(s) = "+startSccId+" return s" );
                ResourceIterator<Node> startSccResults = result.columnAs("s");
                startScc = startSccResults.next();

                // Get End SCC node
                result = indexDb.execute( "match (s) WHERE ID(s) = "+endSccId+" return s" );
                ResourceIterator<Node> endSccResults = result.columnAs("s");
                endScc = endSccResults.next();

                // Store Relation
                Relationship relationship = startScc.createRelationshipTo(endScc, rel.getType());
                relationship.setProperty("message", "Some Information");
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////   Decide Vertex Level - Part 7.1
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void decideVertixLevel(GraphDatabaseService indexDb){
        Long order_level = (long) 1;

        result = indexDb.execute("MATCH (n) RETURN count(n) as c");
        ResourceIterator<Long> count_query = result.columnAs("c");
        Long nodes_total = (Long) count_query.next();

        while(order_level <= nodes_total){
            result = indexDb.execute("MATCH (n) RETURN n");
            nodes = result.columnAs("n");
            while(nodes.hasNext()){
                Node node = nodes.next();
                if(Vorder.containsKey(node.getId())){
                    S_out.put(node.getId(), -1);
                    S_in.put(node.getId(), -1);
                    continue;
                }
                findOut(node);
                findIn(node);
            }
            Long maxNodeID = findMax();
            result = indexDb.execute("MATCH (n) WHERE ID(n)="+maxNodeID+" SET n.order="+order_level);
            Vorder.put(maxNodeID, true);
            order_level++;
        }
    }

    public static Long findMax(){
        // Loop through nodes and find the maximum f(v,G) =
        // (|Sin(v,G)|.|Sout(v,G)| + |Sin(v,G)| + |Sout(v,G)|) / (|Sin(v,G)| + |Sout(v,G)|)
        Long nodeID = (long) 0;
        float max_cost = -1;
        float cost;
        for(Long k : S_out.keySet()){
            if(S_in.get(k) + S_out.get(k) == 0){
                cost = 0;
            }else{
                cost = (S_in.get(k)*S_out.get(k) + S_in.get(k) + S_out.get(k)) / (S_in.get(k) + S_out.get(k));
            }
            if(cost > max_cost){
                nodeID = k;
                max_cost = cost;
            }
        }
        return nodeID;
    }

    public static int findIn(Node node){
        if(S_in.containsKey(node.getId())){
            return S_in.get(node.getId());
        }

        int in = 0;
        Iterable<Relationship> rels = node.getRelationships(Direction.INCOMING);
        for(Relationship rel : rels){
            if(Vorder.containsKey(rel.getStartNode().getId())){
                continue;
            }
            in += findIn(rel.getStartNode()) + 1;
        }
        S_in.put(node.getId(), in);
        return in;
    }

    public static int findOut(Node node){
        if(S_out.containsKey(node.getId())){
            return S_out.get(node.getId());
        }

        int out = 0;
        Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING);
        for(Relationship rel : rels){
            if(Vorder.containsKey(rel.getEndNode().getId())){
                continue;
            }
            out += findOut(rel.getEndNode()) + 1;
        }
        S_out.put(node.getId(), out);
        return out;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////   Labeling Algorithm - Part 7.2
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void butterfly(GraphDatabaseService indexDb){
        Hashtable<Long, Set<Long>> l_out = new Hashtable<Long, Set<Long>>();
        Hashtable<Long, Set<Long>> l_in = new Hashtable<Long, Set<Long>>();
        Hashtable<Long, Boolean> visited = new Hashtable<Long, Boolean>();

        result = indexDb.execute("MATCH (n) RETURN n ORDER BY n.order");
        nodes = result.columnAs("n");
        while(nodes.hasNext()){
            Queue<Node> bfs_queue = new LinkedList<Node>();
            ArrayList<Node> b_plus = new ArrayList<Node>();
            ArrayList<Node> b_minus = new ArrayList<Node>();

            Node node = nodes.next();
            visited.put(node.getId(), true);

            // Find B Plus
            bfs_queue.add(node);
            while(!bfs_queue.isEmpty()){
                Node bn = bfs_queue.remove();
                Iterable<Relationship> rels = bn.getRelationships(Direction.OUTGOING);
                for(Relationship rel : rels){
                    if(visited.containsKey(rel.getEndNode().getId())){
                        // Consider this node is deleted from the graph
                        continue;
                    }
                    //if((Long) bn.getProperty("order") > (Long) rel.getEndNode().getProperty("order")){
                    bfs_queue.add(rel.getEndNode());
                    b_plus.add(rel.getEndNode());
                    //}
                }
            }

            // Find B Minus
            bfs_queue.add(node);
            while(!bfs_queue.isEmpty()){
                Node bn = bfs_queue.remove();
                Iterable<Relationship> rels = bn.getRelationships(Direction.INCOMING);

                for(Relationship rel : rels){
                    if(visited.containsKey(rel.getStartNode().getId())){
                        // Consider this node is deleted from the graph
                        continue;
                    }

                    //if((Long) bn.getProperty("order") < (Long) rel.getStartNode().getProperty("order")){
                    bfs_queue.add(rel.getStartNode());
                    b_minus.add(rel.getStartNode());
                    //}
                }
            }

            // Update l_in
            for(Node u : b_plus){
                Set<Long> l_out_temp = new HashSet<Long>();
                Set<Long> l_in_temp = new HashSet<Long>();

                if(l_out.containsKey(node.getId())){
                    l_out_temp = l_out.get(node.getId());
                }
                if(l_in.containsKey(u.getId())){
                    l_in_temp = l_in.get(u.getId());
                }

                l_out_temp.retainAll(l_in_temp);
                if(l_out_temp.size() == 0){
                    l_in_temp.add(node.getId());
                    l_in.put(u.getId(), l_in_temp);
                }
            }

            // Update l_out
            for(Node u : b_minus){
                Set<Long> l_out_temp = new HashSet<Long>();
                Set<Long> l_in_temp = new HashSet<Long>();

                if(l_out.containsKey(u.getId())){
                    l_out_temp = l_out.get(u.getId());
                }
                if(l_in.containsKey(node.getId())){
                    l_in_temp = l_in.get(node.getId());
                }

                l_in_temp.retainAll(l_out_temp);
                if(l_in_temp.size() == 0){
                    l_out_temp.add(node.getId());
                    l_out.put(u.getId(), l_out_temp);
                }
            }
        }

        // Store l_out and l_in in Neo4j
        result = indexDb.execute("MATCH (n) RETURN n ORDER BY n.order");
        nodes = result.columnAs("n");
        while(nodes.hasNext()){
            Node node = nodes.next();
            Set<Long> l_out_temp = new HashSet<Long>();
            Set<Long> l_in_temp = new HashSet<Long>();

            if(l_out.containsKey(node.getId())){
                l_out_temp = l_out.get(node.getId());
            }
            if(l_in.containsKey(node.getId())){
                l_in_temp = l_in.get(node.getId());
            }

            node.setProperty("l_out", l_out_temp.toString());
            node.setProperty("l_in", l_in_temp.toString());
        }
    }

}

