package edu.gmu.cs475.test;

import edu.gmu.cs475.AbstractKVStore;
import edu.gmu.cs475.internal.TestingClient;
import org.apache.curator.utils.ZKPaths;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class P1ZKTests extends Base475Test {

	@Rule
	public Timeout globalTimeout = new Timeout(60000);

	@Test
	public void testClientAddsEphemeralMembership() throws Exception {
		TestingClient client = newClient("Leader");
		Assert.assertTrue("Expected a ZKNode created at " + ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, client.getLocalConnectString()), blockUntilMemberJoins(client));
		client.suspendAccessToZK();
		Assert.assertTrue("Expected no ZKNode anymore at " + ZKPaths.makePath(AbstractKVStore.ZK_MEMBERSHIP_NODE, client.getLocalConnectString()), blockUntilMemberLeaves(client));
	}

	@Test
	public void testFirstClientBecomesLeader() throws Exception {

		TestingClient firstClient = newClient("Leader");
		Assert.assertTrue("Expected the first client we started to assume leadership", blockUntilLeader(firstClient));

		firstClient.suspendAccessToZK();
		Thread.sleep(10000);
		TestingClient second = newClient("Follower");
		Assert.assertTrue("Expected the second client we started to assume leadership", blockUntilLeader(second));
	}

	@Test
	public void testF() throws Exception {

		TestingClient firstClient = newClient("Leader");
		Assert.assertTrue("Expected the first client we started to assume leadership", blockUntilLeader(firstClient));
		TestingClient second = newClient("Follower");
		TestingClient a = newClient("Follower");
		TestingClient b = newClient("Follower");
		TestingClient c = newClient("Follower");

		blockUntilMemberJoins(second);
		blockUntilMemberJoins(a);
		blockUntilMemberJoins(b);
		blockUntilMemberJoins(c);

		firstClient.suspendAccessToZK();
		Thread.sleep(10000);

		Thread t1 = new Thread(() -> {
			try {
				second.getValue("a");
			} catch (Throwable ex) {
				ex.printStackTrace();
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				a.getValue("a");
			} catch (Throwable ex) {
				ex.printStackTrace();
			}
		});

		Thread t3 = new Thread(() -> {
			try {
				b.getValue("a");
			} catch (Throwable ex) {
				ex.printStackTrace();
			}
		});

		Thread t4 = new Thread(() -> {
			try {
				c.getValue("a");
			} catch (Throwable ex) {
				ex.printStackTrace();
			}
		});

		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t1.join();
		t2.join();
		t3.join();
		t4.join();

		Assert.assertTrue("Expected the second client we started to assume leadership", blockUntilLeader(second));
	}
}
