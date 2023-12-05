import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class WatchWorkers implements Watcher {
	private ZooKeeper zk;
	private List<String> taskBanList;

	public WatchWorkers(ZooKeeper zk, List<String> bl){
		this.zk = zk;
		this.taskBanList = bl;
	}

	@Override
	public void process(WatchedEvent ev) {
		Helper.print("[Manger Worker watcher]");
		Helper.print(ev.toString());


		try{
			zk.addWatch("/dist23/workers", this, AddWatchMode.PERSISTENT_RECURSIVE);
		} catch(Exception e){ Helper.error(e);}
		try{
			if(		(ev.getType() == Event.EventType.NodeCreated ||
					ev.getType() == Event.EventType.NodeDataChanged)
					&& ev.getPath().contains("/_w")
			){
				WorkerInfo wi = (WorkerInfo) Helper.fromBytes(zk.getData(ev.getPath(), false, null));
				String next = GetNextTask();
				if(next == null){
					return;
				}
				String workerID = ev.getPath().substring(ev.getPath().indexOf("_w"));
				Helper.print("Assigning task " + next + " to worker " + workerID);
				wi.status = WorkerInfo.Status.Working;
				wi.assigned = next;
				zk.setData(ev.getPath(), Helper.toBytes(wi), -1);
			}

		} catch(Exception e){ Helper.error(e);}
	}

	public String GetNextTask(){
		try{
			List<String> tasks = zk.getChildren("/dist23/tasks", false);
			if(tasks.size() == 0){
				return null;
			}
			for(String task : tasks){
				List<String> children = zk.getChildren("/dist23/tasks/" + task, false);
				if(children.size() != 0 || taskBanList.contains("/dist23/tasks/" + task))
					continue;
				return "/dist23/tasks/" + task;
			}
		} catch(Exception e){ Helper.error(e);}

		return null;
	}

}


