package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class DistributeServer {
    private static String connectString = "cloud-linux01:2181,cloud-linux02:2181,cloud-linux03:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/my_test_servers";

    //创建到zk的客户端连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, event -> {
            //暂时不需要任何操作
        });
    }

    //注册服务器
    public void registServer(String hostname) throws UnsupportedEncodingException, KeeperException, InterruptedException {
        String create = zk.create(
                parentNode + "/" + "child_node",
                hostname.getBytes("UTF-8"),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online" + create);
    }

    //业务功能
    public void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        args = new String[]{"cloud-linux03"};

        DistributeServer server = new DistributeServer();
        server.getConnect();

        server.registServer(args[0]);
        server.business(args[0]);
    }
}
