import java.io.*;

public class Helper {
	public static void print(Object a){
		if(a == null){
			print("a is NULL");
		}

		if (((String) a).equals("[Manger Task watcher]")){
			try{
				print("Exact");
				throw new Exception();
			} catch (Exception e){
				error(e);
			}
		}
		else if(((String) a).contains("[Manger Task watcher]")){
			try{
				print("almost");
				System.out.println(a);
				throw new Exception();
			} catch (Exception e){
				error(e);
			}
		}


		System.out.println(a);
	}

	public static void error(Exception e){
		print("------Error-------");
		print(e.getMessage());
		e.printStackTrace();
		print("-------------------");
	}

	public static byte[] toBytes(Serializable obj){
		byte[] byteArray = null;
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outStream);


			objectOutputStream.writeObject(obj);
			objectOutputStream.flush();

			byteArray = outStream.toByteArray();

			objectOutputStream.close();
			outStream.close();
		} catch (IOException e) {
			error(e);
			e.printStackTrace();
		}
		return byteArray;
	}
	public static Object fromBytes(byte[] data){
		if(data == null)
			return null;
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		Object ret = null;
		try {
			ObjectInput in = new ObjectInputStream(bis);
			ret = in.readObject();
		} catch (Exception e) {
			error(e);
		}
		return ret;
	}

	public static void printWorker(WorkerInfo wi){
		print("Printing worker data");
		if(wi == null){
			print("Null");
			return;
		}

		print(wi.assigned);
		print(wi.status);

	}
}
