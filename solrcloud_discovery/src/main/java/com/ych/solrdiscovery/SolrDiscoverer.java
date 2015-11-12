package com.ych.solrdiscovery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class SolrDiscoverer extends AbstractZkExplorer implements ISolrDiscoverer {

	protected static final Logger LOG = Logger.getLogger(SolrDiscoverer.class);
	private static final Map<String, List<String>> clusterState = new HashMap<String, List<String>>() ;
	// cache zookeeper solr cluster state file
	private static final String clusterStateZnode = "/solr/clusterstate.json";
	private static final Random random = new Random();
	private static final Object lock = new Object();
	private final SolrStateWatcher solrStateWatcher = new SolrStateWatcher();
	// cache of zookeper solr alias file
	private static final Map<String,String> alias = new HashMap<String,String>();
	private static final String aliasZnode = "/solr/aliases.json";
	private boolean aliasingActive = false;

	/**
	 * 
	 * @param string connection url for zooKeeper
	 */
	public SolrDiscoverer(String connectionString) {
		super(connectionString);
		try {
			fetchLClusterState();
		} catch (SrvLocatorException e) {
			// something wrong goes here ...
			LOG.error(e.getMessage(), e);
		}
	}

	/**
	 * 
	 * @param string connection url for zooKeeper
	 * @param boolean to fetch alias file
	 */
	public SolrDiscoverer(String connectionString, boolean aliasingActive) {
		super(connectionString);
		this.aliasingActive = aliasingActive;
		try {
			fetchLClusterState();
			if(this.aliasingActive){
				fetchAliases();
			}
		} catch (SrvLocatorException e) {
			// something wrong goes here ...
			LOG.error(e.getMessage(), e);
		}
	}
	
	private void fetchAliases() throws SrvLocatorException{
		synchronized (lock) {
			Stat st = null;
			try {
				st = checkSolrPathZk();
			} catch (Exception e1) {
				LOG.error(e1.getMessage(), e1);
			}
			try {
				ZooKeeper zk = getServiceLocator(false);
				if(st != null){
					byte[] d = zk.getData(aliasZnode, solrStateWatcher, st);
					String s_file = new String(d);
					JSONObject jsonObj = (JSONObject) JSONValue.parse(s_file);
					Map<String, String> tmp = parseAliases(jsonObj);
					alias.clear();
					LOG.info("Updating aliases with "+tmp.size()+" aliases definition");
					alias.putAll(tmp);
				}else {
					LOG.error("Solr is not deployed on this cluster");
				}
			} catch (KeeperException e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, String> parseAliases(JSONObject jsonObj) {
		Map<String,String> result = new HashMap<String,String>();
		if(jsonObj != null){
			JSONObject jsonContent = (JSONObject)jsonObj.get("collection");
			Iterator<String> it = jsonContent.keySet().iterator();
			while(it.hasNext()){
				String alias = it.next();
				result.put(alias, (String)jsonContent.get(alias));
			}
		}
		return result;
	}

	private void fetchLClusterState() throws SrvLocatorException{
		synchronized (lock) {
			Stat st = null;
			try {
				st = checkSolrPathZk();
			} catch (Exception e1) {
				LOG.error(e1.getMessage(), e1);
			}
			try {
				ZooKeeper zk = getServiceLocator(false);
				if(st != null){
					byte[] d = zk.getData(clusterStateZnode, solrStateWatcher, st);
					String s_file = new String(d);
					JSONObject jsonObj = (JSONObject) JSONValue.parse(s_file);
					Map<String, List<String>> tmp = parseClusterState(jsonObj);
					clusterState.clear();
					LOG.info("Updating cluster state with "+tmp.size()+" indexes definition");
					clusterState.putAll(tmp);
				}else {
					LOG.error("Solr is not deployed on this cluster");
				}
			} catch (KeeperException e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			} catch (InterruptedException e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				throw new SrvLocatorException(e);
			}
		}
	}
	
	private Stat checkSolrPathZk() throws SrvLocatorException{
		Stat st = null;
		try {
			ZooKeeper zk = getServiceLocator(false);
			st = zk.exists("/solr", false);
		} catch (Exception e) {
			LOG.error("checkSolrPathZk error#1 - "+e.getMessage(), e);
			try {
				ZooKeeper zk = getServiceLocator(true);
				st = zk.exists("/solr", false);
			} catch (Exception e1) {
				LOG.error("checkSolrPathZk error#2 - "+e.getMessage(), e);
				throw new SrvLocatorException(e);
			}
		} 
		return st;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, List<String>> parseClusterState(JSONObject obj) {
		Set<String> cores = obj.keySet();
		Iterator<String> it = cores.iterator();
		Map<String, List<String>> results = new HashMap<String, List<String>>();
		while (it.hasNext()) {
			String coreName = it.next();
			JSONObject coreInfo = (JSONObject) obj.get(coreName);
			List<String> endpoints = new ArrayList<String>();
			results.put(coreName, endpoints);
			if (coreInfo != null) {
				JSONObject shards = (JSONObject) coreInfo.get("shards");
				if (shards != null) {
					// navigate the shards
					Set<String> shards_name = shards.keySet();
					for (String shard : shards_name) {
						JSONObject shard_data = (JSONObject) shards.get(shard);
						if (shard_data != null) {
							JSONObject replicas = (JSONObject) shard_data
									.get("replicas");
							if (replicas != null) {
								Set<String> replicas_name = replicas.keySet();
								for (String replica_name : replicas_name) {
									JSONObject replica_data = (JSONObject) replicas
											.get(replica_name);
									if (replica_data != null
											&& replica_data.get("state") != null
											&& ((String) replica_data
													.get("state"))
													.equals("active")) {
										if (replica_data.get("base_url") != null
												&& replica_data.get("core") != null) {
											endpoints.add(replica_data
													.get("base_url")
													+ "/"
													+ replica_data.get("core"));
										}
									}
								}
							}
						}
					}
				}
			}

		}
		return results;
	}
	

	@Override
	public Set<String> getAvailableCollections() throws SrvLocatorException {
		checkClusterState();
		return clusterState.keySet();
	}
	
	private void checkClusterState() throws SrvLocatorException{
		if(clusterState.isEmpty()){
			// should never go here
			LOG.info("cluster state empty reloading it");
			fetchLClusterState();
		}
	}

	@Override
	public List<String> getActiveNodesByCollectionName(String collectionName)
			throws SrvLocatorException {
		checkClusterState();
		if(aliasingActive){
			collectionName = fetchCollectionbyAlias(collectionName);
		}
		List<String> endpoints = clusterState.get(collectionName);
		if(endpoints!=null && endpoints.size()>0){
			return endpoints;
		}else{
			throw new SrvLocatorException("No active node found for the collection : " + collectionName);
		}
	}

	@Override
	public String getActiveNodeByCollectionName(String collectionName)
			throws SrvLocatorException {
		checkClusterState();
		if(aliasingActive){
			collectionName = fetchCollectionbyAlias(collectionName);
		}
		List<String> endpoints = clusterState.get(collectionName);
		if(endpoints!=null && endpoints.size()>0){
			return endpoints.get(random.nextInt(endpoints.size()));
		}else{
			throw new SrvLocatorException("No active node found for the collection : " + collectionName);
		}
	}

	private String fetchCollectionbyAlias(String collectionName) throws SrvLocatorException{
		if(alias.isEmpty()){
			LOG.info("Aliasing is active but aliases cache is empty, reloading it...");
			fetchAliases();
		}
		return alias.get(collectionName);
	}
	
	// Zookeeper Watcher for /solr znode 
	protected class SolrStateWatcher implements Watcher{

		@Override
		synchronized public void process(WatchedEvent event) {
			if(event!=null){ 
				LOG.info("cluster state event received : "+event.toString());
				if(event.getType()!=null && event.getPath()!=null && event.getType().equals(EventType.NodeDataChanged))
					if(event.getPath().equals(clusterStateZnode)){
						LOG.debug("reloading solr cluster state ");
						try {
							fetchLClusterState();
						} catch (SrvLocatorException e) {
							LOG.error("Unable to read solr cluster state - cause : "+e.getMessage(), e);
						}
					}else if(event.getPath().equals(aliasZnode)){
						LOG.debug("reloading aliases content");
						try {
							fetchAliases();
						} catch (SrvLocatorException e) {
							LOG.error("Unable to read solr aliases - cause : "+e.getMessage(), e);
						}
					}
			}
		}
	}
	
	@Override
	public boolean isAliasingActive() {
		return aliasingActive;
	}

	
	@Override
	public String getConnectString() {
			return super.connectString;
	}

	@Override
	public String getCollectionNameByAlias(String alias) throws SrvLocatorException {
		if(isAliasingActive())
			return SolrDiscoverer.alias.get(alias);
		else
			throw new SrvLocatorException("aliasing not active, restart SolrDiscoverer with alias true");
	}
	
	
	
}
