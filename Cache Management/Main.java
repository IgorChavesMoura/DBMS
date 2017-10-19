import java.io.*;
import java.util.*;

interface Cache<K, V>{
	V getV(K key);
	void add(K key, V value);
	void displayCache();
	void evict();
}

class LRUCacheShort<K, V> extends LinkedHashMap<K, V> implements Cache<K, V>{
	private int capacity;
	public LRUCacheShort(int capacity){
		super(16,0.75f,true);
		this.capacity = capacity;
	}
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest){
		return size() > capacity;
	}
	public V getV(K key){
		return this.get(key);
	}
	public void add(K key, V value){
		this.put(key,value);
	}
	public void displayCache(){
		for(K key : this.keySet()){
			System.out.print(key + " ");
		}
		System.out.println();
	}
	public String toString(){
		return "LRU";
	}
	public void evict(){
		this.remove(Collections.max(this.keySet(),null));
	}
	
}
class RandomCache<K, V> implements Cache<K, V>{
	private LinkedHashMap<K, V> map;
	private LinkedList<K> randomlist;
	private int capacity;
	private Random random;


	public RandomCache(int capacity){
		this.capacity = capacity;	
		this.map = new LinkedHashMap();
		this.random = new Random();

	}
	public void add(K key, V value){
		if(this.map.size() == capacity){
			/*int selected  = this.random.nextInt()%capacity;
			selected++;
			K selectedKey;
			Iterator iter = map.keySet().iterator();
			int i = 1;
			while(iter.hasNext() && i < selected){
				selectedKey = (K)iter.next();
				if(i == selected){
					this.map.remove(selectedKey);
				}
				i++;

			}*/
			this.evict();
			
			

		}
		this.map.put(key,value);
	}
	public V getV(K key){
		if(this.map.containsKey(key)){
			return this.map.get(key);
		} else {
			return null;
		}
	}
	public void displayCache(){
		for(K key : this.map.keySet()){
			System.out.print(key + " ");
		}
		System.out.println();
	}
	public String toString(){
		return "Random";
	}
	public void evict(){
		int selected  = this.random.nextInt()%capacity;
		selected++;
		K selectedKey;
		Iterator iter = map.keySet().iterator();
		int i = 1;
		while(iter.hasNext() && i < selected){
			selectedKey = (K)iter.next();
			if(i == selected){
				this.map.remove(selectedKey);
			}
			i++;
		}
	}


}
class ClockCache<K, V> implements Cache<K, V>{
	private LinkedHashMap<K, V> clockmap;
	private LinkedList<K> clocklist;
	private int capacity;
	private int clockPointer;
	private int evictIndex;
	public ClockCache(int capacity){
		this.capacity = capacity;
		this.clockmap = new LinkedHashMap();
		this.clocklist = new LinkedList();
		this.clockPointer = 0;
		this.evictIndex = -1;

	}
	public V getV(K key){
		if(this.clockmap.containsKey(key)){
			return this.clockmap.get(key);
		} else {
			return null;
		}
	}
	public void add(K key, V value){
		if(this.clockmap.size() >= this.capacity){
			this.evict();
		}
		this.clockmap.put(key,value);
		if(this.evictIndex > -1){
			this.clocklist.add(this.evictIndex,key);
		} else {
			this.clocklist.add(key);
		}
				
	}
	public void evict(){
		//System.out.println("Clock Pointer atual: "  + clockPointer);
		//System.out.println("Proxima chave a ser removida: " + this.clocklist.get(clockPointer));
		//System.out.println(this.clocklist);
		this.clockmap.remove(this.clocklist.get(clockPointer));
		this.clocklist.remove(this.clocklist.get(clockPointer));
		this.evictIndex = this.clockPointer;
		clockPointer++;
		if(clockPointer == this.capacity-1){
			clockPointer = 0;
		}
	}
	public void displayCache(){
		for(int i = 0; i < this.capacity; i++){
			System.out.print(clocklist.get(i) + " ");
		}
		System.out.println();
	}
	public String toString(){
		return "Clock";
	}


}
class MRUCache<K, V> implements Cache<K, V>{
	private LinkedHashMap<K, V> mrumap;
	private Stack<K> mrustack;
	private int capacity;

	public MRUCache(int capacity){
		this.capacity = capacity;
		this.mrumap = new LinkedHashMap();
		this.mrustack = new Stack();

	}	

	public void add(K key, V value){
		if(this.mrumap.size() >= this.capacity){
			//this.mrumap.remove(this.recent);
			/*K mRecent = this.mrustack.pop();
			this.mrumap.remove(mRecent);*/
			this.evict();

		}
		this.mrumap.put(key,value);
		this.mrustack.push(key);
			

	}
	public V getV(K key){
		if(this.mrumap.containsKey(key)){
			this.mrustack.push(key);
			return this.mrumap.get(key);

		}else {
			return null;
		}

	}
	public void displayCache(){
		for(K key : this.mrumap.keySet()){
			System.out.print(key + " ");
		}
		System.out.println();
	}
	public String toString(){
		return "MRU";
	}
	public void evict(){
		K mRecent = this.mrustack.pop();
		this.mrumap.remove(mRecent);
	}
	
}
class LFUCache<K, V> implements Cache<K, V>{
	private V min;
	private LinkedHashMap<K, V> cache;
	private LinkedHashMap<K, Integer> frequencies;
	private int capacity;

	public LFUCache(int capacity){
		this.cache = new LinkedHashMap();
		this.frequencies = new LinkedHashMap();
		this.capacity = capacity;
	}
	public void add(K key, V value){
		if(this.cache.size() == capacity){
			/*K min = this.getMinFreqKey();
		    this.cache.remove(min);
		   	this.frequencies.remove(min);*/
		   	this.evict();

		}
		this.cache.put(key,value);
		this.frequencies.put(key,Integer.valueOf(0));
	}
	private K getMinFreqKey() {
    	Iterator iter = this.frequencies.keySet().iterator();
    	K currentKey = (K)iter.next();
    	Integer minFreq = this.frequencies.get(currentKey);
    	K minFreqKey = currentKey;
    	while(iter.hasNext()){
    		currentKey = (K)iter.next();
    		if(this.frequencies.get(currentKey) < minFreq){
    			minFreq = this.frequencies.get(currentKey);
    			minFreqKey = currentKey;
    		}
    	}
    	return minFreqKey;
  	}
	public V getV(K key){
		if(this.cache.containsKey(key)){
			Integer oldFrequency = this.frequencies.get(key);
			int temp = oldFrequency.intValue();
			temp++;
			Integer newFrequency = Integer.valueOf(temp);
			this.frequencies.put(key,newFrequency);
			//System.out.println("Frequência da chave " + key + " mudada de " + oldFrequency + " para " + newFrequency);
			
			return this.cache.get(key);
		} else {
			return null;
		}
	}
	public void displayCache(){
		for(K key : this.cache.keySet()){
			System.out.print(key + " ");
		}
		System.out.println();
		/*for(K key: this.frequencies.keySet()){
			System.out.print(this.frequencies.get(key) + " ");
		}
		System.out.println();*/
	}

	public String toString(){
		return "LFU";
	}
	public void evict(){
		K min = this.getMinFreqKey();
		this.cache.remove(min);
		this.frequencies.remove(min);
	}
}
class FIFOCache<K, V> implements Cache<K, V>{
	private Queue<K> fifoQ;
	private LinkedHashMap<K, V> fifoHm;
	private int capacity;
	public FIFOCache(int capacity){
		this.fifoQ = new LinkedList<K>();
		this.fifoHm = new LinkedHashMap<K, V>();
		this.capacity = capacity;
	}
	public void add(K key, V value){
		if(this.fifoQ.size() == capacity){
			/*K removedKey = this.fifoQ.poll();
			this.fifoHm.remove(removedKey);*/
			this.evict();

		}
		this.fifoQ.add(key);
		this.fifoHm.put(key,value);
	}
	public V getV(K key){
		if(fifoHm.containsKey(key)){
			return this.fifoHm.get(key);
		} else {
			return null;
		}
	}
	public String toString(){
		return "FIFO";
	}
	public void displayCache(){
		for(K key : this.fifoHm.keySet()){
			System.out.print(key + " ");
		}
		System.out.println();
	}
	public void evict(){
		K removedKey = this.fifoQ.poll();
		this.fifoHm.remove(removedKey);
	}

}
class DBMS{
		
	public Cache<Integer, String> cache;
	private int cacheHit;
	private int cacheMiss;


	public void initializeCache(){
		try{
			InputStream is = System.in;
     		InputStreamReader isr = new InputStreamReader(is);
	    	BufferedReader br = new BufferedReader(isr);
			
			Cache<Integer, String> cache;
			
			this.cacheHit = 0;
			this.cacheMiss = 0;
			
			System.out.println("Escolha o tipo de cache:");
			System.out.println("	1 - LRU");
			System.out.println("	2 - FIFO");
			System.out.println("	3 - Clock");
			System.out.println("	4 - MRU");
			System.out.println("	5 - Random");
			System.out.println("	6 - LFU");
			System.out.println("Digite sua escolha: ");	
			
			Integer choice = Integer.parseInt(br.readLine());
			
			switch(choice){
			case 1: cache = new LRUCacheShort(8);
					this.cache = cache;
					break;
			case 2: cache = new FIFOCache(8);
					this.cache = cache;
					break;
			case 3: cache = new ClockCache(8);
					this.cache = cache;
					break;		
			case 4: cache = new MRUCache(8);
					this.cache = cache;
					break;
			case 5: cache = new RandomCache(8);
					this.cache = cache;
					break;
			case 6: cache = new LFUCache(8);
					this.cache = cache;
					break;

			}
			
		} catch (IOException ioe){

		}
	}

	public void fetch(Integer key) throws IOException{
		if(this.cache.getV(key) == null){
			InputStream is = new FileInputStream("arquivo.txt");
     		InputStreamReader isr = new InputStreamReader(is);
     		BufferedReader br = new BufferedReader(isr);
     		int i = 0;
     		String s = br.readLine();
     		i++;
     		while(s != null && i < key){
     			i++;
     			s = br.readLine();
     		}
     		this.cache.add(key,s);
     		br.close();
     		this.cacheMiss++;

		}else{
			this.cacheHit++;
		}
		System.out.println(this.cache.getV(key));
	}
	public void evict(){
		this.cache.evict();
	}
	public void displayCache(){
		this.cache.displayCache();
	}
	public void displayStats(){
		System.out.println("Cache Hit: " + this.cacheHit + "    Cache Miss: " + this.cacheMiss);
	}	



}
public class Main {
	public static void main(String[] args) throws IOException{
		DBMS dbms = new DBMS();
		dbms.initializeCache();
		System.out.println("Seu tipo de cache é: " + dbms.cache);
		boolean continua = true;
		while(continua == true){
			System.out.println("Escolha uma das opções abaixo: ");
			System.out.println("	1 - Requisitar (fetch)");
			System.out.println(" 	2 - Remover    (evict)");
			System.out.println("	3 - Mostrar cache");
			System.out.println(" 	4 - Mostrar status");
			System.out.println("	5 - Sair");
			System.out.println("Digite sua escolha: ");
		
			InputStream is = System.in;
     		InputStreamReader isr = new InputStreamReader(is);
	   		BufferedReader br = new BufferedReader(isr);

	    	Integer choice = Integer.parseInt(br.readLine());
	    	switch(choice){
	    		case 1:
	    			System.out.println("Digite a chave para requisição: ");
	    			int choice2 = Integer.parseInt(br.readLine());
	    			dbms.fetch(choice2);
	    			break;
	    		case 2:
	    			dbms.evict();
	    			break;
	    		case 3:
	    			dbms.displayCache();
	    			break;
	    		case 4:
	    			dbms.displayStats();
	    			break;
	    		case 5:
	    			
	    			continua = false;
	    			break;				
	    	}
		}



		
			

     	
     	//dbms.fetch(1);
     	//dbms.fetch(5);
     	//dbms.fetch(6);
     	//dbms.fetch(13);
     	//dbms.fetch(1);
     	//dbms.fetch(4);
     	//dbms.fetch(8);
     	//dbms.fetch(13);
     	//dbms.fetch(9);
     	//dbms.fetch(10);
     	//dbms.fetch(15);
     	//dbms.fetch(5);
     	//dbms.fetch(4);
     	//dbms.fetch(4);
     	//dbms.fetch(4);
     	//dbms.fetch(6);
     	//dbms.fetch(16);

		System.out.println("Cache final: ");
     	dbms.displayCache();
     	System.out.println();
     	System.out.println("Status final: ");
     	dbms.displayStats();
     	
	}
}