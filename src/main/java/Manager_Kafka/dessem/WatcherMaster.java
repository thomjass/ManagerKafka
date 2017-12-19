package Manager_Kafka.dessem;

import java.util.*;


import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.Watcher.Event.EventType;



import org.apache.zookeeper.data.Stat;

public class WatcherMaster implements Watcher {
	List<String> id_to_enroll;
	List<String> listChildren;

	
	public void process(WatchedEvent event) {
		
		System.out.println("event receive: " + event.toString());
		
		// TODO Auto-generated method stub
		if (event.getType() == EventType.NodeCreated) {
			System.out.println("Noeud créé: "+event.getPath());
		}
		else if (event.getType() == EventType.NodeDeleted) {
			System.out.println(event.getPath()+" deleted");
			if(event.getPath().contains("/request/enroll")) {
				try {
					ZooConnection.zoo.getChildren("/request/enroll", new WatcherMaster());
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}else if(event.getPath().contains("/request/quit")) {
				try {
					ZooConnection.zoo.getChildren("/request/quit", new WatcherMaster());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else if(event.getPath().contains("/online/")) {
				//When an user go offline
		}
		}
		else if (event.getType() == EventType.NodeChildrenChanged) {
			System.out.println("Master: " + event.getPath() +" NodeChildrenChange triggered");
			if(event.getPath().equals("/request/enroll")) {
				try {
					id_to_enroll = ZooConnection.zoo.getChildren("/request/enroll", false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					for(int i=0;i<id_to_enroll.size();i++) {
							
							try {
								ZooConnection.zoo.create("/registry/"+id_to_enroll.get(i), "1".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
								try {
									createATopic(id_to_enroll.get(i));
								}catch(Exception e) {
									
								}
								ZooConnection.zoo.setData("/request/enroll/"+id_to_enroll.get(i), "1".getBytes(), -1);
								System.out.println(id_to_enroll.get(i)+" has been registered successfully");
							} catch (InterruptedException e) {
								try {
									ZooConnection.zoo.setData("/request/enroll/"+id_to_enroll.get(i), "0".getBytes(), -1);
									System.out.println(id_to_enroll.get(i)+" error Connection");
								} catch (Exception e1) {
									// TODO Auto-generated catch block
									System.out.println("LINE 49 WATCHER MASTER");
									e1.printStackTrace();
								}
								 
							} catch (KeeperException e) {
								try {
									e.printStackTrace();
									 ZooConnection.zoo.setData("/request/enroll/"+id_to_enroll.get(i), "2".getBytes(), -1);
									 System.out.println(id_to_enroll.get(i)+" is already registered");
									} catch (Exception e2) {
										try {

											ZooConnection.zoo.setData("/request/enroll/"+id_to_enroll.get(i), "0".getBytes(), -1);
											System.out.println(id_to_enroll.get(i)+" error Connection");
										} catch (Exception e1) {
											e1.printStackTrace();
										} 
									}
							}
					}
					
					try {
						ZooConnection.zoo.getChildren("/request/enroll", new WatcherMaster());
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}else if(event.getPath().equals("/request/quit")) {
				
				try {
					List<String> id_to_quit = ZooConnection.zoo.getChildren(event.getPath(), false);
					Stat version_stat = ZooConnection.zoo.exists("/registry/"+id_to_quit.get(0),false);
					
					if(version_stat!=null) {
						int version = version_stat.getVersion();
						try {
							ZooConnection.zoo.delete("/registry/"+id_to_quit.get(0), version);
							ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(0), "1".getBytes(), -1);
							System.out.println(id_to_quit.get(0) + " has been deleted successfully");
						} catch (InterruptedException e) {
							ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(0), "0".getBytes(), -1);
						}
					}else {
						ZooConnection.zoo.setData("/request/quit/"+id_to_quit.get(0), "2".getBytes(), -1);
						System.out.println(id_to_quit.get(0) + " doesn't exist in registry");
					}
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				try {
					ZooConnection.zoo.getChildren("/request/quit", new WatcherMaster());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else if(event.getPath().equals("/online")) {
				List<String> listOfUser = null;
                List<String> usersOnline = null;
                try {
                    listOfUser = ZooConnection.zoo.getChildren("/registry", false);
                    usersOnline = ZooConnection.zoo.getChildren("/online", false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                for(int i=0; i<usersOnline.size(); i++){
                    if(listOfUser.contains(usersOnline.get(i))){
                        try {
                            ZooConnection.zoo.setData("/online/"+usersOnline.get(i), "1".getBytes(),-1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }else{
                        try {
                            ZooConnection.zoo.setData("/online/"+usersOnline.get(i), "2".getBytes(),-1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                //NodeChildrenChange on online, new user or node deleted
				try {
					ZooConnection.zoo.getChildren("/online", new WatcherMaster());
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} 
				
			}
		}
		else if (event.getType() == EventType.NodeDataChanged) {
			System.out.println(event.getPath()+"'s data has been changed");
		}
		else {
			System.out.println(event.getPath());
		}
	}
	
	public void createATopic(String topic){
        String zookeeperConnect = MAnager.URL_Kafka;
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

