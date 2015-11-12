package com.ych.solrdiscovery;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractZkExplorer {

	private static int CONNECTION_TIMEOUT = 60000;

	protected static final Logger LOG = LoggerFactory.getLogger(AbstractZkExplorer.class);

	protected String connectString;

	private static ZooKeeper zk;

	private static CountdownWatcher watcher;

	public AbstractZkExplorer(String connectString) {
		super();
		this.connectString = connectString;
		try {
			initZkCli();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	protected ZooKeeper getServiceLocator(boolean reset) throws Exception {
		if (reset) {
			LOG.info("reset connection to ZK ...");
			initZkCli();
		} else {
			if (zk == null) {
				LOG.info("init connection to ZK ...");
				initZkCli();
			} else {
				States st = null;
				try {
					st = zk.getState();
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				}
				if (st == null || st.equals(States.NOT_CONNECTED)) {
					LOG.info("ZK connection closed, reconnecting ... watcher state : "
							+ isConnected());
					reconnect();
				} else if (st != null && !(st.equals(States.CONNECTED) || st.equals(States.CONNECTEDREADONLY))) {
					// in case of conn vanish or in a weired state log it
					// anywhere to get a notice
					LOG.info("ZK connection closed, reconnecting ..."
							+ " States : " + st.toString()
							+ " watcher state : " + isConnected());
					reconnect();
				} else if (st != null) {
					LOG.info("ZK connection state : " + st.toString()
							+ " - watcher state : " + isConnected());
				}
			}
		}
		return zk;
	}

	// protected Watcher getConnWatcher(){
	// return watcher;
	// }

	protected boolean isConnected() {
		if (watcher == null)
			return false;
		return watcher.isConnected();
	}

	private void initZkCli() throws Exception {
		try {
			watcher = new CountdownWatcher();
			zk = new ZooKeeper(connectString, CONNECTION_TIMEOUT, watcher, true);
			watcher.waitForConnected(CONNECTION_TIMEOUT); // ensure zk got
															// connected
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
			throw e;
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			throw new Exception(e);
		} catch (TimeoutException e) {
			LOG.error(e.getMessage(), e);
			throw new Exception(e);
		}

	}

	protected void reconnect() throws Exception {
		try {
			if (zk != null)
				zk.close();
			initZkCli();
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			throw new Exception(e);
		}
	}

	public static class CountdownWatcher implements Watcher {
		// this doesn't need to be volatile! (Should probably be final)
		volatile CountDownLatch clientConnected;
		volatile boolean connected;

		public CountdownWatcher() {
			reset();
		}

		synchronized public void reset() {
			clientConnected = new CountDownLatch(1);
			connected = false;
		}

		@Override
		synchronized public void process(WatchedEvent event) {
			LOG.info("keeper state event " + event.getState().toString()+ " received");
			if (event.getState() == KeeperState.SyncConnected || event.getState() == KeeperState.ConnectedReadOnly) {
				connected = true;
				notifyAll();
				clientConnected.countDown();
			} else {
				connected = false;
				notifyAll();
			}
		}

		synchronized public boolean isConnected() {
			return connected;
		}

		synchronized public void waitForConnected(long timeout)
				throws InterruptedException, TimeoutException {
			long expire = System.currentTimeMillis() + timeout;
			long left = timeout;
			while (!connected && left > 0) {
				wait(left);
				left = expire - System.currentTimeMillis();
			}
			if (!connected) {
				throw new TimeoutException("Did not connect");

			}
		}

		synchronized public void waitForDisconnected(long timeout)
				throws InterruptedException, TimeoutException {
			long expire = System.currentTimeMillis() + timeout;
			long left = timeout;
			while (connected && left > 0) {
				wait(left);
				left = expire - System.currentTimeMillis();
			}
			if (connected) {
				throw new TimeoutException("Did not disconnect");

			}
		}
	}

}
