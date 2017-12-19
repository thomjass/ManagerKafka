package Manager_Kafka.dessem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.apache.zookeeper.ZooKeeper;

public class ZooConnection {
		public static ZooKeeper zoo;
	   final CountDownLatch connectedSignal = new CountDownLatch(1);
	   
	   // Method to connect zookeeper ensemble.
	   public ZooKeeper connect(String host) throws IOException,InterruptedException {	
	      zoo = new ZooKeeper(host,5000,new WatcherConn());
	      return zoo;
	   }
		
	   public static void create(String path, byte[] data) throws 
	      KeeperException,InterruptedException {
	      zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
	      CreateMode.PERSISTENT);
		}
	
	   
	   public void initTree() {
		   String a = "1";
		   zoo.addAuthInfo("digest", "admin:admin".getBytes());
		   try {
			   create("/request",a.getBytes());
		   }catch(Exception e ) {
			   System.out.println(e.getMessage());
		   }
		   try {
			   create("/request/quit",a.getBytes());
		   }catch(Exception e ) {
			   System.out.println(e.getMessage());
		   }
		   try {
			   create("/request/enroll",a.getBytes());
		   }catch(Exception e ) {
			   System.out.println(e.getMessage());
		   }
		   try {
			   create("/registry",a.getBytes());
		   }catch(Exception e ) {
			   System.out.println(e.getMessage());
		   }
		   try {
			   create("/online",a.getBytes());
		   }catch(Exception e ) {
			   System.out.println(e.getMessage());
		   }
		   try {
			zoo.getChildren("/online", new WatcherMaster());
		} catch (Exception e) {
			e.printStackTrace();
		} 
	   }

	   // Method to disconnect from zookeeper server
	   public void close() throws InterruptedException {
	      zoo.close();
	   }
	   public void checkEnroll() {
		   List<String> id_to_enroll=new ArrayList<String>();
		   Stat a = null;
		try {
				id_to_enroll = zoo.getChildren("/request/enroll", false);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				for(int i=0;i<id_to_enroll.size();i++) {
						
						try {
							zoo.create("/registry/"+id_to_enroll.get(i), "1".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
							createATopic(id_to_enroll.get(i));
							zoo.setData("/request/enroll/"+id_to_enroll.get(i), "1".getBytes(), -1);
							System.out.println(id_to_enroll.get(i)+" has been registered successfully");
						} catch (InterruptedException e) {
							try {
								zoo.setData("/request/enroll/"+id_to_enroll.get(i), "0".getBytes(), -1);
								System.out.println(id_to_enroll.get(i)+" error Connection");
							} catch (Exception e1) {
								// TODO Auto-generated catch block
								System.out.println("LINE 49 WATCHER MASTER");
								e1.printStackTrace();
							}
							 
						} catch (KeeperException e) {
							try {
								 zoo.setData("/request/enroll/"+id_to_enroll.get(i), "2".getBytes(), -1);
								 System.out.println(id_to_enroll.get(i)+" is already registered");
								} catch (Exception e2) {
									try {

										zoo.setData("/request/enroll/"+id_to_enroll.get(i), "0".getBytes(), -1);
										System.out.println(id_to_enroll.get(i)+" error Connection");
									} catch (Exception e1) {
										e1.printStackTrace();
									} 
								}
						}
					try{
						a = zoo.exists("/request/enroll/"+id_to_enroll.get(i),false);
					}catch(Exception e){
							e.printStackTrace();
					}
					try {
						if (a == null) {

						} else {
							zoo.delete("/request/enroll/" + id_to_enroll.get(i), -1);
						}
					}catch(Exception e){
							e.printStackTrace();
					}
				}

				try {
					zoo.getChildren("/request/enroll", new WatcherMaster());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	   }
	   
	   public void checkQuit() {
		   List<String> id_to_quit = null;
		try {
			id_to_quit = ZooConnection.zoo.getChildren("/request/quit", false);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		for(int i=0; i<id_to_quit.size(); i++) {  
		   try {
				
				
					Stat version_stat = ZooConnection.zoo.exists("/registry/"+id_to_quit.get(i),false);
					if(version_stat!=null) {
						int version = version_stat.getVersion();
						try {
							ZooConnection.zoo.delete("/registry/"+id_to_quit.get(i), version);
							ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(i), "1".getBytes(), -1);
							System.out.println(id_to_quit.get(i) + " has been deleted successfully");
						} catch (InterruptedException e) {
							ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(i), "0".getBytes(), -1);
						}
					}else {
						ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(i), "2".getBytes(), -1);
						System.out.println(id_to_quit.get(i) + " doesn't exist in registry");
					}
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		   
			try {
				ZooConnection.zoo.getChildren("/request/quit", new WatcherMaster());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	   }
	   
		public void createATopic(String topic){
	        String zookeeperConnect = "localhost:2181";
	        int sessionTimeoutMs = 10 * 1000;
	        int connectionTimeoutMs = 8 * 1000;


	        int partitions = 1;
	        int replication = 1;
	        Properties topicConfig = new Properties(); // add per-topic configurations settings here

	        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
	        // createTopic() will only seem to work (it will return without error).  The topic will exist in
	        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
	        // topic.
	        ZkClient zkClient = new ZkClient(
	                zookeeperConnect,
	                sessionTimeoutMs,
	                connectionTimeoutMs,
	                ZKStringSerializer$.MODULE$);

	        // Security for Kafka was added in Kafka 0.9.0.0
	        boolean isSecureKafkaCluster = false;

	        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
	        AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
	        zkClient.close();

	    }
}
