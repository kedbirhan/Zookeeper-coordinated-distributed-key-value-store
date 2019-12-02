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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KVStore extends AbstractKVStore {
    private LeaderLatch leaderLatch;
    private HashMap<String, String> cache = new HashMap<>();
    private HashMap<String, ReentrantReadWriteLock> locks = new HashMap<>();
    private HashMap<String, List<String>> invalidateMap = new HashMap<>();

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

		// If there is no leader, select one
		leaderLatch = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
		try {
			String leaderId = leaderLatch.getLeader().getId();
			// check if there is a leader
			if(leaderId == null || leaderId.isEmpty()){
				//select a leader
				leaderLatch.start();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
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
           value = cache.get(key);

           // this is the leader
           if(leaderLatch.hasLeadership()){
           		return value;
		   }

            // value is not in cache, ask leader
            if (value == null) {
                IKVStore leaderStore = connectToKVStore(leaderLatch.getLeader().getId());
                value = leaderStore.getValue(key, getLocalConnectString());
                // update follower cache if value is not null
                if(value != null){
                	cache.put(key, value);
				}
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
			lock.readLock().unlock();
		}
        return value;
    }

    public synchronized ReentrantReadWriteLock getLock(String key){
		ReentrantReadWriteLock lock = locks.get(key);
		if(lock == null){
			lock = new ReentrantReadWriteLock();
			locks.put(key, lock);
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

        	if(leaderLatch.hasLeadership()){
        		cache.put(key, value);
        		return;
			}

        	IKVStore leaderStore = connectToKVStore(leaderLatch.getLeader().getId());
        	leaderStore.setValue(key, value, getLocalConnectString());
        	cache.put(key, value);
		}catch (Exception e){
        	throw new IOException();
		}
        finally {
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
		ReentrantReadWriteLock lock = getLock(key);
		lock.readLock().lock();
		try{
			String value = cache.get(key);
			if(value != null){
				saveToInvalidateMap(key, fromID);
			}
			return value;
		}finally {
			lock.readLock().unlock();
		}
    }

    private synchronized void saveToInvalidateMap(String key, String fromID){
		try {
			if(fromID.equals(leaderLatch.getLeader().getId())){
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<String> list = invalidateMap.get(key);
		if(list == null){
			list = new LinkedList<>();
			invalidateMap.put(key, list);
		}
		if(!list.contains(fromID)){
			list.add(fromID);
		}
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
		ReentrantReadWriteLock lock = getLock(key);
		lock.writeLock().lock();

		try{
			// invalidate the cache of all clients
			List<String> list = invalidateMap.get(key);
			if(list != null){
				for(String id : list){
					IKVStore client = connectToKVStore(id);
					client.invalidateKey(key);
				}
				// empty the list
				list.clear();
			}


			//update the value
			cache.put(key, value);

			//save follower who has this key cached
			saveToInvalidateMap(key, fromID);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.writeLock().unlock();
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
		cache.remove(key);
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

