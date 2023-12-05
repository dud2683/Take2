import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;

public class Manager {
	private ZooKeeper zk;

	public Manager(ZooKeeper zk){
		this.zk = zk;
		try{
			ArrayList<String> bl = new ArrayList<>();
			zk.addWatch("/dist23/workers", new WatchWorkers(zk, bl), AddWatchMode.PERSISTENT_RECURSIVE);
			zk.addWatch("/dist23/tasks", new WatchTasks(zk, bl), AddWatchMode.PERSISTENT_RECURSIVE);
		}
		catch(Exception e){
			Helper.error(e);
		}


	}

}
