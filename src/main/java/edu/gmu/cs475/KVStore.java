package edu.gmu.cs475;

import edu.gmu.cs475.internal.IKVStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KVStore extends AbstractKVStore {
    ConcurrentHashMap<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();
    private LeaderLatch leaderLatch;

    private HashMap<String, HashMap<String, String>> cacheMap = new HashMap<>();

    /**
     * This callback is invoked once your client has started up and published an RMI endpoint.
     * <p>
     * In this callback, you will need to set-up your ZooKeeper connections, and then publish your
     * RMI endpoint into ZooKeeper (publishing the hostname and port)
     * <p>
     * You will also need to set up any listeners to track ZooKeeper events
     *
     * @param localClientHostname Your client's hostname, which other clients will use to contact you
     * @param localClientPort     Your client's port number, which other clients will use to contact you
     */
    @Override
    public void initClient(String localClientHostname, int localClientPort) {
        // create a node
        PersistentNode node = new PersistentNode(zk, CreateMode.EPHEMERAL, false, ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
        node.start();

        // create a cache for this client
        HashMap<String, String> cache = new HashMap<>();
        cacheMap.put(getLocalConnectString(), cache);

        // set leader
        if (leaderLatch == null) {
            leaderLatch = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
            try {
                leaderLatch.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // add listeners
    }

    /**
     * Retrieve the value of a key
     *
     * @param key
     * @return The value of the key or null if there is no such key
     * @throws IOException if this client or the leader is disconnected from ZooKeeper
     */
    @Override
    public String getValue(String key) throws IOException {
        ReentrantReadWriteLock lock = getLock(key);
        lock.readLock().lock();
        String value = null;
        try {
            // look if client has the value cached
            HashMap<String, String> cache = cacheMap.get(getLocalConnectString());
            value = cache.get(key);

            // value is not in cache, ask leader
            if (value == null) {
                IKVStore leaderStore = connectToKVStore(getLocalConnectString());
                value = leaderStore.getValue(key, getLocalConnectString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.readLock().unlock();
        }
		return value;
    }

    public synchronized ReentrantReadWriteLock getLock(String key) {
        ReentrantReadWriteLock lock = lockMap.get(key);
        if (lock == null) {
            lock = new ReentrantReadWriteLock();
            lockMap.put(key, lock);
        }
        return lock;
    }


    /**
     * Update the value of a key. After updating the value, this new value will be locally cached.
     *
     * @param key
     * @param value
     * @throws IOException if this client or the leader is disconnected from ZooKeeper
     */
    @Override
    public void setValue(String key, String value) throws IOException {
        ReentrantReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            try {
				IKVStore leaderStore = connectToKVStore(getLocalConnectString());
				leaderStore.setValue(key, value, getLocalConnectString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
     * <p>
     * This command should ONLY be called as a request to the leader.
     *
     * @param key    The key requested
     * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
     * @return The value of the key, or null if there is no value for this key
     * <p>
     * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
     */
    @Override
    public String getValue(String key, String fromID) throws RemoteException {
        String value = null;
        try {
            HashMap<String, String> cache = cacheMap.get(leaderLatch.getLeader().getId());
            value = cache.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    /**
     * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
     * <p>
     * This command should ONLY be called as a request to the leader.
     * <p>
     * This command must wait for any pending writes on the same key to be completed
     *
     * @param key    The key to update
     * @param value  The new value
     * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
     */
    @Override
    public void setValue(String key, String value, String fromID) throws IOException {
		try {
			HashMap<String, String> cache = cacheMap.get(leaderLatch.getLeader().getId());
			cache.put(key, value);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    /**
     * Instruct a node to invalidate any cache of the specified key.
     * <p>
     * This method is called BY the LEADER, targeting each of the clients that has cached this key.
     *
     * @param key key to invalidate
     *            <p>
     *            DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
     */
    @Override
    public void invalidateKey(String key) throws RemoteException {

    }

    /**
     * Called when ZooKeeper detects that your connection status changes
     *
     * @param curatorFramework
     * @param connectionState
     */
    @Override
    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
        System.out.println(getLocalConnectString() + " " + connectionState);
    }

    /**
     * Release any ZooKeeper resources that you setup here
     * (The connection to ZooKeeper itself is automatically cleaned up for you)
     */
    @Override
    protected void _cleanup() {

    }
}

