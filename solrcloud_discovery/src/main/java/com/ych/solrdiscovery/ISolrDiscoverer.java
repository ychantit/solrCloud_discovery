package com.ych.solrdiscovery;
import java.util.List;
import java.util.Set;



public interface ISolrDiscoverer extends IZkServices{
	
	/**
	 * Return a list of urls where the given collection is live
	 * @param collectionName name of the index or the alias to be located
	 * @return List<String> of urls
	 * @throws SrvLocatorException
	 */
	List<String> getActiveNodesByCollectionName(String collectionName) throws SrvLocatorException;
	
	/**
	 * Fetch the list of deployed collection on the server
	 * @return
	 * @throws SrvLocatorException
	 */
	
	Set<String> getAvailableCollections() throws SrvLocatorException;
	
	/**
	 * Fetch an active node where the given index is available
	 * @param collectionName
	 * @return host name
	 * @throws SrvLocatorException
	 */
	
	String getActiveNodeByCollectionName(String collectionName) throws SrvLocatorException;
	
	/**
	 * Get the connectString for zooKeeper for sanity check
	 * @return
	 */
	
	String getConnectString();
	
	/**
	 * Check the runtime status of the srvLocator if it's in alias or index mode
	 * @return
	 */
	
	boolean isAliasingActive();
	
	String getCollectionNameByAlias(String alias) throws SrvLocatorException;
}
