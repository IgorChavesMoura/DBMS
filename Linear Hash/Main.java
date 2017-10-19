import java.util.*;
import java.io.*;
import java.lang.*;

public class Main{
	public static void main(String[] args) throws IOException{
		Linear linear = new Linear("arquivo.txt",4,2);
		boolean finished = false;

		while(finished == false){
			System.out.println("                    Linear Hash");
			System.out.println("1 - Search by key");
			System.out.println("2 - Insert");
			System.out.println("3 - Delete by key");
			System.out.println("4 - Quit");
			System.out.print("Your choice: ");
			InputStream is = System.in;
     		InputStreamReader isr = new InputStreamReader(is);
	    	BufferedReader br = new BufferedReader(isr);
	    	int query;

	    	int choice = Integer.parseInt(br.readLine());

	    	System.out.println();

	    	switch(choice){
	    		case 1:
	    			System.out.print("Search key: ");
	    			query = Integer.parseInt(br.readLine());
	    			System.out.println();

	    			linear.search(query);
	    			break;
	    		case 2:
	    			System.out.print("Insert key: ");
	    			int key = Integer.parseInt(br.readLine());
	    			System.out.println();
	    			System.out.print("Insert value: ");
	    			String value = br.readLine();
	    			System.out.println();

	    			linear.insert(key,value);
	    			break;
	    		case 3:
	    			System.out.print("Delete key: ");
	    			query = Integer.parseInt(br.readLine());
	    			System.out.println();

	    			linear.delete(query);
	    			break;		
	    		case 4:
	    			finished = true;
	    			br.close();
	    			break;


	    	}
	    	

		}

		//System.out.println(linear.search(5));
		//linear.insert(8,"vitoria");
		//linear.delete(8);
	}
}