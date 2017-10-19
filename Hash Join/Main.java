import java.util.*;
import java.io.*;
import java.lang.*;

public class Main{
	public static void main(String[] args) throws IOException{
		HashJoin hj = new HashJoin("nation.txt",2,"region.txt",0,10);
		hj.join();
	}

}
