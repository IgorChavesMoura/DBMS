import java.util.*;
import java.io.*;
import java.lang.*;

//Iremos supor que cada página do arquivo tem 5 tuplas, pois, nos arquivos de teste, o menor possui 5 tuplas,
//logo ele tera uma página.

public class HashJoin {
	private int atr1, atr2; 
	private File table1;
	private File table2;
	private File partitionsTable1;
	private File partitionsTable2;
	private int bufferSize;
	//private LinkedList<File> partitions;
	private LinkedList<String> joinResult;

	public HashJoin(String tableFileName1,int atr1, String tableFileName2,int atr2, int bufferSize) throws IOException{
		this.table1 = new File(tableFileName1);
		this.table2 = new File(tableFileName2);
		this.atr1 = atr1;
		this.atr2 = atr2;
		this.bufferSize = bufferSize;
		partitionsTable1 = new File("partitionsTable1.txt");
		partitionsTable2 = new File("partitionsTable2.txt");

		if(!partitionsTable1.exists()){
			partitionsTable1.createNewFile();
		}
		if(!partitionsTable2.exists()){
			partitionsTable2.createNewFile();
		}
	}
	public void join() throws IOException{
		this.buildPartitions();
		FileReader reader1 = new FileReader(this.partitionsTable1);
		BufferedReader br1 = new BufferedReader(reader1);

		FileReader reader2 = new FileReader(this.partitionsTable2);
		BufferedReader br2 = new BufferedReader(reader2);

		String partition1, partition2;
		System.out.println("                                        Junções");
		while(br1.ready() && br2.ready()){
			partition1 = br1.readLine();
			partition2 = br2.readLine();

			comparePartitions(partition1, partition2);
		}
		br1.close();
		br2.close();


		System.out.println("Custo 3(M+N): " + this.calculateJoinCost());
	}
	private void comparePartitions(String partition1, String partition2){
		StringTokenizer st1 = new StringTokenizer(partition1, "$");
		StringTokenizer st2 = new StringTokenizer(partition2, "$");
		Hashtable<Integer,String> table2HashTable = new Hashtable();
		String tuplePartition;
		int offset = 0, hashTableKey;
		while(st2.hasMoreTokens()){
			tuplePartition = st2.nextToken();
			table2HashTable.put(h2(tuplePartition,2,offset),tuplePartition);
			offset++;
		}
		//System.out.println(table2HashTable);
		String currentTupleTable1, currentTupleTable2;
		offset = 0;
		
		while(st1.hasMoreTokens()){
			currentTupleTable1 = st1.nextToken();
			StringTokenizer st3 = new StringTokenizer(currentTupleTable1,"|");
			int currentAtr = 0;

			Iterator<Integer> itr = table2HashTable.keySet().iterator();
			String tupleAtr = st3.nextToken();
			while(currentAtr < this.atr1){
				tupleAtr = st3.nextToken();
				currentAtr++;
				
			}
			int joinAtrTuple1 = Integer.parseInt(tupleAtr);
			while(itr.hasNext()){
				currentAtr = 0;
				currentTupleTable2 = table2HashTable.get(itr.next());
				StringTokenizer st4 = new StringTokenizer(currentTupleTable2, "|");
				tupleAtr = st4.nextToken();
				while(currentAtr < this.atr2){
					tupleAtr = st4.nextToken();
					currentAtr++;
				}
				int joinAtrTuple2 = Integer.parseInt(tupleAtr);
				if(joinAtrTuple1 == joinAtrTuple2){
					System.out.println(currentTupleTable1 + currentTupleTable2);
				}
			}

		}



	}
	private int h(String tuple, int table){
		System.out.println("Tupla: " + tuple);
		int currentAtr = 0;
		String partitionAtr;
		StringTokenizer st1 = new StringTokenizer(tuple, "|");
		partitionAtr = st1.nextToken();
		if(table == 1){
			while(currentAtr < this.atr1){
				partitionAtr = st1.nextToken();
				currentAtr++;
			}
			//System.out.println(partitionAtr);
		} else if(table == 2){
			while(currentAtr < this.atr2){
				partitionAtr = st1.nextToken();
				currentAtr++;
			}
			//System.out.println(partitionAtr);
		}
		int partitionNumber = Integer.parseInt(partitionAtr);
		partitionNumber = partitionNumber%(this.bufferSize);
		System.out.println("Resultado do hash: " + partitionNumber);
		
		return partitionNumber;


	}
	private int h2(String tuple, int table, int offset){
		int currentAtr = 0;
		String partitionAtr;
		StringTokenizer st1 = new StringTokenizer(tuple, "|");
		partitionAtr = st1.nextToken();
		if(table == 1){
			while(currentAtr < this.atr1){
				partitionAtr = st1.nextToken();
				currentAtr++;
			}
			//System.out.println(partitionAtr);
		} else if(table == 2){
			while(currentAtr < this.atr2){
				partitionAtr = st1.nextToken();
				currentAtr++;
			}
			//System.out.println(partitionAtr);
		}
		int tupleNumber = Integer.parseInt(partitionAtr);
		tupleNumber = ((tupleNumber-(tupleNumber%this.bufferSize))/this.bufferSize)+offset;
		return tupleNumber;
	}

	private void buildPartitions() throws IOException{
		FileReader reader = new FileReader(this.table1);
		BufferedReader br = new BufferedReader(reader);

		FileReader reader2 = new FileReader(this.table2);
		BufferedReader br2 = new BufferedReader(reader2);
		LinkedList<String> table1PartitionsBuffer = new LinkedList();
		LinkedList<String> table2PartitionsBuffer = new LinkedList();
		
		

		String partitionTuple = "";
		int partitionNumber;
		while(br.ready()){
			partitionTuple = br.readLine();
			table1PartitionsBuffer.add(partitionTuple);
			//partitionNumber = h(partitionTuple,1);
			//insertInPartitionFile(1,partitionTuple,partitionNumber);

		}
		br.close();
		while(br2.ready()){
			partitionTuple = br2.readLine();
			table2PartitionsBuffer.add(partitionTuple);
			//partitionNumber = h(partitionTuple,2);
			//insertInPartitionFile(2,partitionTuple,partitionNumber);
		}
		br2.close();

		ListIterator<String> li1 = table1PartitionsBuffer.listIterator();
		ListIterator<String> li2 = table2PartitionsBuffer.listIterator();

		while(li1.hasNext()){
			partitionTuple = li1.next();
			partitionNumber = h(partitionTuple,1);
			insertInPartitionFile(1,partitionTuple,partitionNumber);
		}
		while(li2.hasNext()){
			partitionTuple = li2.next();
			partitionNumber = h(partitionTuple,2);
			insertInPartitionFile(2,partitionTuple,partitionNumber);
		}

	}
	private void insertInPartitionFile(int partitionFile,String tuple,int partitionNumber) throws IOException{
		File tempFile = new File("temp.txt");

		if(!tempFile.exists()){
			tempFile.createNewFile();
		}
		FileReader reader = null;
		if(partitionFile == 1){
			reader = new FileReader(this.partitionsTable1);
			System.out.println("Tabela 1 escolhida");
		} else if(partitionFile == 2) {
			reader = new FileReader(this.partitionsTable2);
			System.out.println("Tabela 2 escolhida");
		}
		BufferedReader br = new BufferedReader(reader);

		FileWriter writer = new FileWriter(tempFile);
		BufferedWriter bw = new BufferedWriter(writer);
		String partition = "";
		
		partition = br.readLine();
		if(partition == null){
			partition = "empty";
		}
		int currentPartition = 0; 
		System.out.println("Partição " + currentPartition + " :" + partition);

		
		while(currentPartition < partitionNumber){
			bw.write(partition);
			bw.newLine();
			partition = br.readLine();
			if(partition == null){
				partition = "empty";
			}
			
			
			System.out.println("Partição " + currentPartition + " :" + partition);
			currentPartition++;
		}
		if(partition.equals("empty")){
			partition = tuple;
		}
		if(partition != tuple){
			partition += tuple;
		}	
		
		partition += "$";
		System.out.println("Tupla a ser inserida na partição " + currentPartition + ": " + partition);
		bw.write(partition);
		bw.newLine();
		while(br.ready()){
			partition = br.readLine();
			bw.write(partition);
			bw.newLine();
		}
		br.close();
		bw.close();
		System.out.println();
		
		FileReader reader2 = new FileReader(tempFile);
		BufferedReader br2 = new BufferedReader(reader2);

		FileWriter writer2 = null;
		if(partitionFile == 1){
			writer2 = new FileWriter(this.partitionsTable1);
		} else if(partitionFile == 2){
			writer2 = new FileWriter(this.partitionsTable2);
		}

		BufferedWriter bw2 = new BufferedWriter(writer2);
		if(writer2 != null){
			while(br2.ready()){
				partition = br2.readLine();
				bw2.write(partition);
				bw2.newLine();
			}
			
		}
		tempFile.deleteOnExit();
		br2.close();
		bw2.close();

	}
	public int calculateJoinCost() throws IOException{
		FileReader reader = new FileReader(this.table1);
		BufferedReader br = new BufferedReader(reader);
		int table1Size = 0,table2Size = 0;
		while(br.ready()){
			br.readLine();
			table1Size++;
		}
		br.close();
		FileReader reader2 = new FileReader(this.table2);
		BufferedReader br2 = new BufferedReader(reader2);
		while(br2.ready()){
			br2.readLine();
			table2Size++;
		}
		int table1Pages, table2Pages;

		table1Pages = (int)Math.ceil(table1Size/5);
		table2Pages = (int)Math.ceil(table2Size/5);

		return 3*(table1Pages+table2Pages);

	}



}