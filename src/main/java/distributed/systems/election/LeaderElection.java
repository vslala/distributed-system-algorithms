package distributed.systems.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;
    private String currentZnodeName;
    private String predecessorZnode;

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        this.zooKeeper.close();
    }

    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = this.zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestZnode = children.get(0);

        if (smallestZnode.equals(this.currentZnodeName)) {
            System.out.println("I'm the leader!");
            return;
        }

        System.out.println("I'm not the leader! " + smallestZnode + " is the leader!");
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePreifx = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = this.zooKeeper.create(znodePreifx, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("ZNode Name: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws InterruptedException, KeeperException {
        List<String> children = this.zooKeeper.getChildren(ELECTION_NAMESPACE, this);
        Collections.sort(children);
        String smallestZnode = children.get(0);

        Stat predecessorStat = null;
        while (predecessorStat == null) {
            if (smallestZnode.equals(this.currentZnodeName)) {
                System.out.println("I'm the leader!");
                return;
            } else {
                System.out.println("I'm not the leader!");
                int predecessorIndex = Collections.binarySearch(children, this.currentZnodeName) - 1;
                predecessorZnode = children.get(predecessorIndex);
                predecessorStat = this.zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnode, this);
            }
        }

        System.out.println("Watching znode: " + predecessorZnode);
        System.out.println();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None -> {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully Connected to zookeeper!");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected event from zookeeper received!");
                        zooKeeper.notifyAll();
                    }
                }
            }
            case NodeDeleted -> {
                try {
                    reelectLeader();
                } catch (InterruptedException | KeeperException ignored) {
                }
            }

        }
    }
}
