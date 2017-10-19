import java.util.*;
import java.io.*;
import java.lang.*;

public class Linear{

	private File file;
	private File tempFile;
	private int round;
	private int p;
	private int initSize;
	private int bucketInitSize;
	private int currentPosition;
	private int size;
	private boolean duplicateReady;


	public Linear(String fileName, int initSize, int bucketInitSize){
		this.file = file;
		this.initSize = initSize;
		this.size = initSize;
		this.bucketInitSize = bucketInitSize;
		this.p = 0;
		this.currentPosition = 0;
		this.round = 0;
		this.file = new File(fileName);
		this.tempFile = new File("temp.txt");
		this.duplicateReady = false;
		

	}	

	public String search(int query) throws IOException{
		int line = hA(query);
		if(line < this.p){
			line = hN(query);
		}
		String value = searchFile(line,query);
		return value;
	}
	public void insert(int key, String value) throws IOException{
		int line = hA(key);
		if(line < this.p){
			line = hN(key);
		}
		//System.out.println(line);
		insertInFile(line,key,value);
	}
	public void delete(int query) throws IOException{
		int line = hA(query);
		if(line < this.p){
			line = hN(query);
		}
		String deletedValue = deleteInFile(line,query);

	}
	public String searchFile(int line ,int query) throws IOException{
		FileReader reader = new FileReader(this.file);
		BufferedReader br = new BufferedReader(reader);

		int currentLine = 0;
		String bucket = br.readLine();
		String returnValue = "Not found";

		while(br.ready() && currentLine < line){
			bucket = br.readLine();
			currentLine++;
		}

		StringTokenizer st1 = new StringTokenizer(bucket,"$");
		int bucketSize = Integer.parseInt(st1.nextToken());

		while(st1.hasMoreTokens()){
			String page =  st1.nextToken();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
			StringTokenizer st2 =  new StringTokenizer(page, ",");
			while(st2.hasMoreTokens()){
				int pageKey = Integer.parseInt(st2.nextToken());
				String pageValue = st2.nextToken();
				if(pageKey == query){
					returnValue = pageValue;
				}
			}


		}
		br.close();
		return returnValue;



	}
	public void insertInFile(int line, int key, String value) throws IOException{
		FileReader reader = new FileReader(this.file);
		BufferedReader br = new BufferedReader(reader);

		FileWriter writer = new FileWriter(this.tempFile);
		BufferedWriter bw = new BufferedWriter(writer);

		int currentLine = 0;

		String lineTemp;
		String newLine = "";
		
		while(currentLine < line){
			lineTemp = br.readLine();
			if(lineTemp == null){
				bw.write("0");
			} else {
				bw.write(lineTemp);
			}
			
			bw.newLine();
			currentLine++;
		}
		lineTemp = br.readLine();

		
		if(lineTemp == null || lineTemp.equals("0")){
			bw.write("1$"+key+","+value);
			bw.newLine();
		} else {
			StringTokenizer st3 = new StringTokenizer(lineTemp,"$");
			int bucketSize = Integer.parseInt(st3.nextToken());
			bucketSize++;
			newLine += bucketSize;
			while(st3.hasMoreTokens()){
				newLine+="$";
				newLine+=st3.nextToken();
			}
			newLine+= "$"+key+","+value;
			bw.write(newLine);
			bw.newLine();
			if(bucketSize > this.bucketInitSize){
				duplicateReady = true;
			}
		}
		while(br.ready()){
			lineTemp = br.readLine();
			bw.write(lineTemp);
			bw.newLine();
		}
		br.close();
		bw.close();


		
		FileReader reader2 = new FileReader("temp.txt");
		BufferedReader br2 = new BufferedReader(reader2);

		FileWriter writer2 = new FileWriter(this.file);
		BufferedWriter bw2 = new BufferedWriter(writer2);

		String temp2;

		while(br2.ready()){
			temp2 = br2.readLine();
			bw2.write(temp2);
			bw2.newLine();
		}

		br2.close();
		bw2.close();

		if(duplicateReady){
			duplicate();
			duplicateReady = false;
		}

	}
	public String deleteInFile(int line, int query) throws IOException{
		FileReader reader5 = new FileReader(this.file);
		BufferedReader br5 = new BufferedReader(reader5);

		FileWriter writer5 = new FileWriter("temp.txt");
		BufferedWriter bw5 = new BufferedWriter(writer5);

		int currentLine = 0, pageKey;
		String lineTemp, newBucket = "", deletedValue = "", page;

		while(currentLine < line){
			lineTemp = br5.readLine();
			bw5.write(lineTemp);
			bw5.newLine();
		}
		lineTemp = br5.readLine();
		StringTokenizer st6 = new StringTokenizer(lineTemp,"$");
		int bucketSize = Integer.parseInt(st6.nextToken());
		bucketSize--;
		newBucket += String.valueOf(bucketSize);

		while(st6.hasMoreTokens()){
			page = st6.nextToken();
			StringTokenizer st7 = new StringTokenizer(page,",");
			pageKey = Integer.parseInt(st7.nextToken());
			if(pageKey != query){
				newBucket += "$" + String.valueOf(pageKey) + "," + st7.nextToken();
			} else {
				deletedValue = st7.nextToken();
			}
		}

		bw5.write(newBucket);
		bw5.newLine();

		while(br5.ready()){
			lineTemp = br5.readLine();
			bw5.write(lineTemp);
			bw5.newLine();

		}
		br5.close();
		bw5.close();

		FileReader reader6 = new FileReader("temp.txt");
		BufferedReader br6 = new BufferedReader(reader6);

		FileWriter writer6 = new FileWriter(this.file);
		BufferedWriter bw6 = new BufferedWriter(writer6);

		while(br6.ready()){
			lineTemp = br6.readLine();
			bw6.write(lineTemp);
			bw6.newLine();
		}
		br6.close();
		bw6.close();

		return deletedValue;


	}
	public void duplicate() throws IOException{
		FileReader reader3 = new FileReader(this.file);
		BufferedReader br3 = new BufferedReader(reader3);
		
		FileWriter writer3 = new FileWriter("temp.txt");
		BufferedWriter bw3 = new BufferedWriter(writer3);

		int currentLine = 0, oldBucketSize = 0, newBucketSize = 0;

		String lineTemp, oldBucket = "",oldBucketTemp = "", newBucket = "", newBucketTemp = "";
		System.out.println("p: " + this.p);
		while(currentLine < this.p && br3.ready()){
			System.out.println("Entrou");
			lineTemp = br3.readLine();
			System.out.println(lineTemp);
			bw3.write(lineTemp);
			bw3.newLine();
			currentLine++;
		}

		lineTemp = br3.readLine();

		StringTokenizer st4 = new StringTokenizer(lineTemp, "$");
		int bucketSize = Integer.parseInt(st4.nextToken());

		while(st4.hasMoreTokens()){
			String page = st4.nextToken();
			StringTokenizer st5 = new StringTokenizer(page, ",");
			int pageKey = Integer.parseInt(st5.nextToken());
			if(this.hN(pageKey) != p){
				//System.out.println("Vai pro novo " + pageKey);
				newBucketTemp+="$" + page;
				newBucketSize++;

			} else {
				//System.out.println("Vai pro velho " + pageKey);
				oldBucketTemp+="$" + page;
				oldBucketSize++;
			}


		}
		
		oldBucket = String.valueOf(oldBucketSize) + oldBucketTemp;
		//System.out.println(oldBucket);
		newBucket = String.valueOf(newBucketSize) + newBucketTemp;
		//System.out.println(newBucket);
		bw3.write(oldBucket);
		bw3.newLine();

		while(br3.ready()){
			lineTemp = br3.readLine();
			bw3.write(lineTemp);
			bw3.newLine();
		}
		br3.close();
		bw3.write(newBucket);
		bw3.close();
		if(this.p == this.size-1){
			this.p = 0;
			this.round++;
			this.size = this.size*2;
		} else {
			System.out.println(this.size);
			this.p++;	
		}
		

		FileReader reader4 = new FileReader("temp.txt");
		BufferedReader br4 = new BufferedReader(reader4);

		FileWriter writer4 = new FileWriter(this.file);
		BufferedWriter bw4 = new BufferedWriter(writer4);

		while(br4.ready()){
			lineTemp = br4.readLine();
			bw4.write(lineTemp);
			bw4.newLine();
		}
		br4.close();
		bw4.close();



	}
	public int hA(int key){
		//System.out.println(key%((Math.pow(2,0))*initSize));
		return (int)(key%((Math.pow(2,round))*initSize));
	}
	public int hN(int key){
		return (int)(key%((Math.pow(2,round))*initSize*2));
	}

}