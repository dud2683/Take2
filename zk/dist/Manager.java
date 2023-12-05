import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.ZooKeeper;

public class Manager {
	private ZooKeeper zk;

	public Manager(ZooKeeper zk){
		this.zk = zk;
		try{
			zk.addWatch("/dist23/workers", new WatchWorkers(zk), AddWatchMode.PERSISTENT_RECURSIVE);
			zk.addWatch("/dist23/tasks", new WatchTasks(zk), AddWatchMode.PERSISTENT_RECURSIVE);
		}
		catch(Exception e){
			Helper.error(e);
		}


	}

}
