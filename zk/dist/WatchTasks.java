import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class WatchTasks implements Watcher {
	private ZooKeeper zk;
	private List<String> taskBanList;

	public WatchTasks(ZooKeeper zk, List<String> bl){
		this.taskBanList = bl;
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent ev) {
		Helper.print("[Manger Task watcher]");
		Helper.print(ev.toString());
		try{
			zk.addWatch("/dist23/tasks", this, AddWatchMode.PERSISTENT_RECURSIVE);
		} catch(Exception e){Helper.error(e);}

		try{
			if(ev.getType() == Event.EventType.NodeCreated &&
				!ev.getPath().contains("result") &&
				!ev.getPath().contains("working"))
			{
				String next = GetNextWorker();
				if(next == null){
					return;
				}
				WorkerInfo wi  = new WorkerInfo();
				wi.assigned = ev.getPath();
				wi.status = WorkerInfo.Status.Working;
				zk.setData(next, Helper.toBytes(wi), -1);
			}
			if(ev.getType() == Event.EventType.NodeDataChanged){
				Object o = Helper.fromBytes(zk.getData(ev.getPath(), false, null));
				if(o instanceof TaskInfo){
					taskBanList.add(ev.getPath());
				}
			}

		} catch(Exception e){Helper.error(e);}
	}

	public String GetNextWorker(){
		try {
			List<String> children = zk.getChildren("/dist23/workers", false);
			for(String child : children){
				String path = "/dist23/workers/" + child;

				WorkerInfo wi = (WorkerInfo) Helper.fromBytes(zk.getData(path, false, null));
				if(wi.status == WorkerInfo.Status.Idle){
					return path;
				}
			}


		}catch (Exception e){Helper.error(e);}
		return null;
	}
}
