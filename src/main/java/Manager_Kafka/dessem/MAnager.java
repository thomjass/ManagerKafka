package Manager_Kafka.dessem;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZooKeeper;

/**
 * Hello world!
 *
 */
public class MAnager 
{
	public static ZooKeeper zk;
	public static String URL_Kafka;
    public static void main( String[] args )
    {
    	URL_Kafka = args[2];
    	ZooConnection connection = new ZooConnection();
        try {
			zk = connection.connect(args[0]);
			try {
				WatcherConn.connectionLatch.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(zk.getState());
			connection.initTree();
			connection.checkEnroll();
			connection.checkQuit();
			while(true) {
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
    }
}
