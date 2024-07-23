package distributed.systems.cluster;

import distributed.systems.cluster.management.LeaderElection;
import distributed.systems.cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static org.apache.zookeeper.Watcher.Event.EventType.None;

public class Orchestrator implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;

    private ZooKeeper zooKeeper;

    public Orchestrator(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        connectToZookeeper();

        var serviceRegistry = new ServiceRegistry(zooKeeper);
        var electionAction = new OnElectionAction(serviceRegistry, currentServerPort);

        var leaderElection = new LeaderElection(zooKeeper, electionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reelect();

        run();
        close();
        System.out.println("Disconnected from zookeeper. Exiting application!");
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None -> {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to the zookeeper!");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from zookeeper!");
                        zooKeeper.notifyAll();
                    }
                }
            }
        }
    }
}
