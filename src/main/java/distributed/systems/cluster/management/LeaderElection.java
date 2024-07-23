package distributed.systems.cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ELECTION = "/election";
    private final ZooKeeper zookeeper;
    private final OnElectionCallback onElectionCallback;
    private String currentZnodeName;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zookeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION + "/c_";
        String znodeFullPath = zookeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentZnodeName = znodeFullPath.replaceAll(ELECTION + "/", "");

        System.out.println("Znode Name: " + this.currentZnodeName);
    }

    public void reelect() throws InterruptedException, KeeperException {
        List<String> children = zookeeper.getChildren(ELECTION, false);
        Collections.sort(children);
        String smallestNode = children.get(0);

        String predecessorNode = null;
        Stat predecessorStat = null;
        while (predecessorStat == null) {
            if (smallestNode.equals(currentZnodeName)) {
                System.out.println("I'm the leader!");
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                int predecessorNodeIndex = Collections.binarySearch(children, currentZnodeName) - 1;

                predecessorNode = children.get(predecessorNodeIndex);
                predecessorStat = zookeeper.exists(ELECTION + "/" + predecessorNode, this);
            }
        }

        onElectionCallback.onWorker();
        System.out.println("Watching znode: " + predecessorNode + "\n");
    }


    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted -> {
                try {
                    reelect();
                } catch (InterruptedException | KeeperException ignored) {
                }
            }
        }
    }
}
