package solrdiscovery;

import java.util.Set;

import com.ych.solrdiscovery.SolrDiscoverer;
import com.ych.solrdiscovery.SrvLocatorException;

/**
 * 
 * Test class for SolrDiscovery service
 *
 */
public class TestSolrDiscovery {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String zkConnString = "host1:2181,host2:2181,host3:2181";
		SolrDiscoverer s = new SolrDiscoverer(zkConnString, false);
		try {
			Set<String> collections = s.getAvailableCollections();
			// loop over all available collections in solr and print a url where the collection can be requested
			for(String col:collections){
				System.out.println("Collection : "+col+" -- node : "+s.getActiveNodeByCollectionName(col));
			}
		} catch (SrvLocatorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
