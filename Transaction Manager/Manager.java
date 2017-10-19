import java.lang.*;
import java.io.*;
import java.util.*;
import java.sql.*;

enum State{
	ACTIVE(1),COMMITED(2),WAITING(0),ABORTED(-1);

	public int value;

	 State (int value){
		this.value = value;
	}
}

public class Manager{
	private Lock_Manager lockManager;
	private Tr_Manager trManager;

	private Vector<Item> itemList;

	public Manager(){
		this.lockManager = new Lock_Manager();
		this.trManager = new Tr_Manager();
		this.itemList = new Vector();
	}
	public Item createItem(int id){
		Item item = new Item(id);
		this.itemList.add(item);
		return item;
	}
	public void bt(int trId){
		System.out.println("BT(" + trId + ")");
		Timestamp currentTime = new Timestamp(System.currentTimeMillis());
		Tr newTr = new Tr(trId,currentTime,State.ACTIVE);
		this.trManager.insertTransactionInList(newTr);
		this.trManager.showStatus();
	}
	public void r(int trId, Item item){
		System.out.println("r" + trId + "(" + item.getId() + ")");
		this.lockManager.ls(trId,item, this.trManager);
		this.trManager.showStatus();
	}
	public void w(int trId, Item item){
		System.out.println("w" + trId + "(" + item.getId() + ")");
		this.lockManager.lx(trId,item, this.trManager);
		this.trManager.showStatus();
	}
	public void c(int trId){
		System.out.println("c(" + trId + ")");
		for(int i = 0; i < itemList.size(); i++){
			this.lockManager.u(trId,this.itemList.elementAt(i),this.trManager);
		}
		
		this.trManager.commitTransaction(trId);
		this.trManager.showStatus();

	}
		
}
class Tr_Manager{
	private int transactionsNumber;
	private Vector<Tr> trList; 

	public Tr_Manager(){
		this.transactionsNumber = 0;
		this.trList = new Vector();
		
	}
	public Tr getTransaction(int trId){
		return this.trList.elementAt(trId);
	}
	public void insertTransactionInList(Tr tr){
		this.trList.add(tr);
		this.transactionsNumber++;
	}
	public void stopTransaction(int trId){
		this.trList.elementAt(trId).setState(State.WAITING);
		System.out.println("Transacao " + trId + " pausada");
	}
	public void abortTransaction(int trId){
		this.trList.elementAt(trId).setState(State.ABORTED);
		System.out.println("Transacao " + trId + " abortada");
	}
	public void commitTransaction(int trId){
		this.trList.elementAt(trId).setState(State.COMMITED);
		System.out.println("Transacao " + trId + " commitada com sucesso");
	}
	public void showStatus(){
		Vector<Tr> actives = new Vector();
		Vector<Tr> waiting = new Vector();
		Vector<Tr> stopped = new Vector();
		Vector<Tr> commited = new Vector();

		for(int i = 0; i < transactionsNumber; i++){
			Tr current = this.trList.elementAt(i);
			if(current.getState().equals(State.ACTIVE)){
				actives.add(current);
			} 
			if(current.getState().equals(State.WAITING)){
				waiting.add(current);
			}
			if(current.getState().equals(State.COMMITED)){
				commited.add(current);
			}
			
			if(current.getState().equals(State.ABORTED)){
				stopped.add(current);
			}
		}


		System.out.println("Ativas:");
		System.out.println(actives);
		System.out.println("Em espera");
		System.out.println(waiting);
		System.out.println("Abortadas:");
		System.out.println(stopped);
		System.out.println("Commitadas");
		System.out.println(commited);
		System.out.println();
		System.out.println();
		System.out.println();
	}
	

}
class Lock_Manager{
	private HashMap<Item,LinkedList<QueueElement>> wait_Q;
	/*
		Linha i eh o id do item i, coluna j id da tr j, posicao ij:
		0 => Sem bloqueio 
		1 => Bloqueio compartilhado 'S'
		2 => Bloqueio exclusivo 'X'
	*/
	 
	private int[][] lock_table; 

	public Lock_Manager(){
		this.wait_Q = new HashMap();
		this.lock_table = new int[100][100];
		for(int i = 0; i < 100; i++){
			for(int j = 0; j < 100; j++){
				lock_table[i][j] = 0;
			}
		}
		
	}
	private int verifyItemXBlock(Item item){
		for(int i = 0; i < 100; i++){
			if(this.lock_table[item.getId()][i] == 2){
				return i;
			} 
		}
		return -1;
	}
	public void insertItemInWaitQ(Item item){
		this.wait_Q.put(item,new LinkedList());
	}
	public void insertTransaction(Item item, int trId, int lock_mode){
		this.wait_Q.get(item).add(new QueueElement(trId,lock_mode));
	}
	public boolean ls(int trId, Item item, Tr_Manager trManager){
		for(int i = 0; i < 100; i++){
			if(this.lock_table[item.getId()][i] == 2){
				System.out.println(item.getId());
				System.out.println(i);
				//int itemTrXBlockId = this.verifyItemXBlock(item);
			
					Tr trXBlock = trManager.getTransaction(i);
					if(trXBlock != null){
						Tr currentTr = trManager.getTransaction(trId);
						
						if(currentTr.getTimestamp().after(trXBlock.getTimestamp())){
							trManager.abortTransaction(trId);
							return false;
						}
					}
					
				
				if(!wait_Q.containsKey(item)){
					this.insertItemInWaitQ(item);
				}
				this.insertTransaction(item,trId,1);
				trManager.stopTransaction(trId);
				System.out.println("Transacao " + trId + " entrou na fila de espera");
				return false;
			}
		}
		this.lock_table[item.getId()][trId] = 1;
		System.out.println("Bloqueio compartilhado inserido no item " + item.getId() + " para a transação " + trId);
		return true;
	}
	public boolean lx(int trId, Item item, Tr_Manager trManager){
		for(int i = 0; i < 100; i++){
			if(this.lock_table[item.getId()][i] == 2){
				//int itemTrXBlockId = this.verifyItemXBlock(item);
			
					Tr trXBlock = trManager.getTransaction(i);
					if(trXBlock != null){
						Tr currentTr = trManager.getTransaction(trId);
						if(currentTr.getTimestamp().after(trXBlock.getTimestamp())){
							trManager.abortTransaction(trId);
							return false;
						}
					}
					

				if(!wait_Q.containsKey(item)){
					this.insertItemInWaitQ(item);
				}
				this.insertTransaction(item,trId,2);
				trManager.stopTransaction(trId);
				System.out.println("Transacao " + trId + " entrou na fila de espera");
				return false;
			}
		}
		this.lock_table[item.getId()][trId] = 2;
		System.out.println("Bloqueio exclusivo inserido no item " + item.getId() + " para a transação " + trId);
		return true;
	}
	public void u(int trId, Item item, Tr_Manager trManager){
		this.lock_table[item.getId()][trId] = 0;
		System.out.println("Analisando fila de espera do item " + item.getId());
		LinkedList<QueueElement> itemWaitQ = this.wait_Q.get(item);
		if(itemWaitQ != null){
			QueueElement nextBlock = itemWaitQ.poll();
			if(nextBlock != null){
				if(nextBlock.lock_mode  ==  1){
					this.ls(trId,item,trManager);
				} else if(nextBlock.lock_mode == 2){
					this.lx(trId,item,trManager);
				}
			}
		}

	}
} 
class QueueElement{
	public int trId;
	public int lock_mode;

	public QueueElement(int trId, int lock_mode){
		this.trId = trId;
		this.lock_mode = lock_mode;
	}
}
class Tr{
	private int id;
	private Timestamp ts;
	private State state;

	public Tr(int id, Timestamp timestamp, State state){
		this.id = id;
		this.ts = timestamp;
		this.state = state;
	}

	public int getId(){
		return this.id;
	}
	public Timestamp getTimestamp(){
		return this.ts;
	}
	public State getState(){
		return this.state;
	}
	public void setState(State state){
		this.state = state;
	}
	public String toString(){
		return "T" + this.id;
	}
}
class Item{
	private int id;

	public Item(int id){
		this.id = id;
	}

	public int getId(){
		return this.id;
	}

}