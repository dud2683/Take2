import org.apache.zookeeper.*;

public class Worker implements Watcher{
	private ZooKeeper zk;

	private String path = null;
	Thread working = null;

	public Worker(ZooKeeper zk){
		this.zk = zk;
		boolean success = false;
		WorkerInfo wi = new WorkerInfo();
		while(!success){
			try{
				path = "/dist23/workers/_w";
				path = zk.create(path, Helper.toBytes(wi), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				zk.addWatch(path, this, AddWatchMode.PERSISTENT);
				success = true;
			}
			catch(Exception e){
				Helper.error(e);
			}
		}
	}


	@Override
	public void process(WatchedEvent ev) {



		try{
			zk.addWatch(path, this, AddWatchMode.PERSISTENT);
		}
		catch (Exception e){Helper.error(e);}
		try{
			if(ev.getType() == Event.EventType.NodeDataChanged){
				Helper.print("[Worker got a message]");
				Helper.print(ev.toString());
				WorkerInfo wi = (WorkerInfo) Helper.fromBytes(zk.getData(path, false, null));
				if(wi == null){
					return;
				}

				if(wi.status == WorkerInfo.Status.Idle){
					return;
				}

				DistTask dt = (DistTask) Helper.fromBytes(zk.getData(wi.assigned, false, null));

				Runnable task = () -> {

					try{
						zk.create(wi.assigned+"/working", null,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
						dt.compute();

						zk.setData(wi.assigned, Helper.toBytes(new TaskInfo()), -1);

						zk.delete(wi.assigned+"/working",-1);
						zk.create(wi.assigned+"/result", Helper.toBytes(dt),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						WorkerInfo newWi = new WorkerInfo();
						zk.setData(path, Helper.toBytes(newWi), -1);
					}
					catch(Exception e){
						Helper.error(e);
						WorkerInfo newWi = new WorkerInfo();
						try{
							zk.setData(path, Helper.toBytes(newWi), -1);
						} catch(Exception ee) {Helper.error(ee);}
					}
				};
				working = new Thread(task);
				working.start();

			}

		}
		catch(Exception e){Helper.error(e);}
	}
}
