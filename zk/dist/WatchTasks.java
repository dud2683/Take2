import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.nio.file.Path;
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

		try{
			zk.addWatch("/dist23/tasks", this, AddWatchMode.PERSISTENT_RECURSIVE);
		} catch(Exception e){Helper.error(e);}

		try{
			if(ev.getType() == Event.EventType.NodeCreated &&
					(!ev.getPath().contains("result")) &&
					(!ev.getPath().contains("working")))
			{
				Helper.print("[Manger Task] Node created:");
				Helper.print(ev.toString());

				String next = GetNextWorker();
				if(next == null){
					return;
				}
				WorkerInfo wi  = new WorkerInfo();
				String workerID = next.substring(next.indexOf("_w"));
				Helper.print("Assigning task " + ev.getPath() + " to worker " + workerID);
				wi.assigned = ev.getPath();
				wi.status = WorkerInfo.Status.Working;
				zk.setData(next, Helper.toBytes(wi), -1);
			}
			else if(ev.getType() == Event.EventType.NodeDataChanged){
				Helper.print("[Manger Task] NodeDataChanged");
				Helper.print(ev.toString());

				Object o = Helper.fromBytes(zk.getData(ev.getPath(), false, null));
				if(o instanceof TaskInfo){
					taskBanList.add(ev.getPath());
				}
			}

		} catch(Exception e){Helper.error(e);}
	}

	public String GetNextWorker(){
		try {
			Helper.print("Choosing next worker");
			List<String> children = zk.getChildren("/dist23/workers", false);
			for(String child : children){
				String path = "/dist23/workers/" + child;

				WorkerInfo wi = (WorkerInfo) Helper.fromBytes(zk.getData(path, false, null));
				Helper.printWorker(wi);
				
				if(wi.status == WorkerInfo.Status.Idle){
					Helper.print(path);
					return path;
				}
			}


		}catch (Exception e){Helper.error(e);}
		return null;
	}
}
