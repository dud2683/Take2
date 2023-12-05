import java.io.Serializable;

public class WorkerInfo implements Serializable {
	enum Status{
		Idle,
		Working
	}
	public String assigned = "";
	public Status status = Status.Idle;

}
