package com.marco.zkdemo;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by maom3 on 2018/1/24.
 */
public class ZkSimple {
    private static final String connectString = "10.62.228.220:2181,10.62.228.220:2182,10.62.228.220:2183/marco";
    private static final CountDownLatch latch = new CountDownLatch(1);
    private static ZooKeeper zkCli;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = createClient();
        zk.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("get child watcher:"+ watchedEvent);
            }
        });
        Thread.sleep(5000L);
        zk.create("/node_test-", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                System.out.println("Create path result:【" + i + "," + s + "," + ","
                        + o + ", real path name:" + s1);
            }
        }, "jam");
        zk.create("/node_test-", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                System.out.println("Create path result:【" + i + "," + s + "," + ","
                        + o + ", real path name:" + s1);
            }
        }, "jam");
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static synchronized ZooKeeper createClient() throws IOException {
        if (zkCli != null) {
            return zkCli;
        }
        zkCli = new ZooKeeper(connectString, 5000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("recived watch envent:" + watchedEvent);
                if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()){
                    System.out.println("子节点变化");
                }
                if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    latch.countDown();
                }
            }
        });
        System.out.println(zkCli.getState());
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Zookeeper session established");
        return zkCli;
    }

}
