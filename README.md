# solrCloud_discovery
Utility service to locate nodes in a solr cloud where a given collection (i.e. index) is deployed and can be requested.

The service use Zookeeper Java API to read solr ZNode of Zookeeper to fetch both clusterstate.json and aliases.json files.

This project includes an example implementation of znode <a href="https://zookeeper.apache.org/doc/r3.3.3/api/org/apache/zookeeper/Watcher.html" target="_blank">Watcher</a> api 

Test class shows an example usage of the solr the service. You only need to provide a connexion string for zookeeper client.

P.S :
You may want to use <a href="https://lucene.apache.org/solr/4_2_0/solr-solrj/org/apache/solr/common/cloud/ClusterState.html" target="_blank">solr4j</a> library which has the same features



