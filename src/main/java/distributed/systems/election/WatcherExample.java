package distributed.systems.election;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatcherExample implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target_znode";

    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public void connectToZookeeper() throws IOException, InterruptedException {
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

    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = this.zooKeeper.exists(TARGET_ZNODE, this);
        if (stat == null) return;

        List<String> children = this.zooKeeper.getChildren(TARGET_ZNODE, this);
        byte[] data = this.zooKeeper.getData(TARGET_ZNODE, this, stat);

        System.out.println("Data: " + new String(data) + ", children: " + children);
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
            case NodeDeleted -> System.out.println(TARGET_ZNODE + " deleted!");
            case NodeCreated -> System.out.println(TARGET_ZNODE + " created!");
            case NodeDataChanged -> System.out.println(TARGET_ZNODE + " data changed!");
            case NodeChildrenChanged -> System.out.println(TARGET_ZNODE + " children changed!");
        }

        try {
            watchTargetZnode();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
