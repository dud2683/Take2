import java.io.Serializable;

public class TaskInfo implements Serializable {
	enum State {
		Done;
	}

	public State s = State.Done;
}
