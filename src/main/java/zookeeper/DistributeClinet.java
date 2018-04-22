package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClinet {
    private static String connectString = "cloud-linux01:2181,cloud-linux02:2181,cloud-linux03:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/my_test_servers";

    //创建到zk的客户端连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, event -> {
            //当我们监听的服务器发生变化，则会回调该方法
            //在这里获取到变化后的所有的服务列表
            try {
                getServerList();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    public void getServerList() throws KeeperException, InterruptedException {
        //1、获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);
        //2、存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();

        //3、遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }
        //4、打印服务器列表信息
        System.out.println(servers);
    }

    //业务功能
    public void business() throws InterruptedException {
        System.out.println("Clinet is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClinet clinet = new DistributeClinet();
        clinet.getConnect();
        clinet.getServerList();
        clinet.business();
    }

}
