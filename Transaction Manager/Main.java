public class Main{
	public static void main(String[] args) {
		Manager manager = new Manager();
		manager.bt(0);
		Item x = manager.createItem(0);
		manager.r(0,x);
		manager.bt(1);
		manager.w(1,x);
		Item y = manager.createItem(1);
		manager.r(1,y);
		manager.r(0,y);
		manager.c(0);
		Item z = manager.createItem(2);
		manager.r(1,z);
		manager.c(1);

	}
}