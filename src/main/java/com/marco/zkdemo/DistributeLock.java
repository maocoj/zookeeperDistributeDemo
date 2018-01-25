package com.marco.zkdemo;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by maom3 on 2018/1/24.
 */
public class DistributeLock {
    private static final String connectString = "10.62.228.220:2181,10.62.228.220:2182,10.62.228.220:2183/marco";
    private static final CountDownLatch initClientCout = new CountDownLatch(1);
    private static final CountDownLatch lock = new CountDownLatch(1);
    private static ZooKeeper zkCli;
    private static String lockName;
    private static boolean lockState = true;

    static class ChildWatcher implements Watcher {
        private String clientName;
        public ChildWatcher(String clientName) {
            this.clientName = clientName;
        }

        public void process(WatchedEvent watchedEvent) {
            if (!lockState) {
                return;
            }
            try {
                List<String> children = zkCli.getChildren("/", this);
                String minChild = getMinChild(children);
                if (minChild.equals(lockName)) {
                    System.out.println(clientName +" have got lock:"+lockName);
                    lockState = false;
                    lock.countDown();

                }
            } catch (Exception e) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        initClient();
        String clientName = "name-" + System.currentTimeMillis();
        lock(clientName);
        System.out.println("******run function*****");
        System.out.println("wait...");
        Thread.sleep(20 * 1000);
        System.out.println("******end function*****");
        release(clientName);
    }

    private static void lock(final String clientName) throws Exception {
        zkCli.create("/lock-", clientName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new AsyncCallback.StringCallback() {
            public void processResult(int i, String s, Object o, String s1) {
                System.out.println(clientName + " locked:" + s1);
                lockName = s1;
                List<String> children = null;
                try {
                    children = zkCli.getChildren("/", new ChildWatcher(clientName));
                } catch (Exception e) {
                }
                String minChild = getMinChild(children);
                if (minChild.equals(lockName)) {
                    System.out.println(clientName +" have got lock:"+lockName);
                    lockState = false;
                    lock.countDown();
                }
            }
        }, null);
        try {
            lock.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void release(final String clientName) throws Exception {
        zkCli.close();
        zkCli = null;
    }

    private static synchronized String getMinChild(List<String> children) {
        if (children == null || children.size() == 0) {
            return null;
        }
        String min = children.get(0);
        for (String child : children) {
            if (child.compareTo(min) < 0) {
                min = child;
            }
        }
        return "/"+ min;
    }

    private static synchronized void initClient() throws IOException {
        zkCli = new ZooKeeper(connectString, 5000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("recived watch envent:" + watchedEvent);
                if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    initClientCout.countDown();
                }
            }
        });
        try {
            initClientCout.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Zookeeper session established");
    }
}
