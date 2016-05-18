Neo4j Reachability Index
================================

This is an unmanaged extension. 

1. Build it using: 

        mvn clean package

2. Copy target/neo4reach-1.0.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        dbms.unmanaged_extension_classes=org.neo4j.reach.unmanagedextension=/reach

4. Start Neo4j server.

5. Query it over HTTP:

        curl http://localhost:7474/reach/reachability/source/{source node id}/target/{target node id}

You could also compare it with reachability query without using the index using:

        curl http://localhost:7474/reach/reachability/noindex/source/{source node id}/target/{target node id}


This work is based on:

Zhu, Andy Diwen, et al. "Reachability queries on large dynamic graphs: a total order approach." Proceedings of the 2014 ACM SIGMOD international conference on Management of data. ACM, 2014.