package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	//HashMap<TransactionId, HashSet<PageId>> shareLocksTidToPageidMap;
	//HashMap<TransactionId, HashSet<PageId>> exclusiveLocksTidToPageidMap;
	Map<PageId, HashSet<TransactionId>> shareLocksPageidToTidMap;
	Map<PageId, TransactionId> exclusiveLocksPageidToTidMap;
	public LockManager(){
		//shareLocksTidToPageidMap = new HashMap<TransactionId, HashSet<PageId>>();
		//exclusiveLocksTidToPageidMap = new HashMap<TransactionId, HashSet<PageId>>();
		shareLocksPageidToTidMap = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		exclusiveLocksPageidToTidMap = new ConcurrentHashMap<PageId, TransactionId>();
	}
	public boolean holdsLock(TransactionId tid, PageId pid) {
		return holdsShareLock(tid,pid) || holdsExclusiveLock (tid,pid);
	}
	
	public synchronized void releasePage(TransactionId tid, PageId pid){
		if(holdsShareLock(tid, pid)){
			deleteShareLock(tid, pid);
		}
		if(holdsExclusiveLock(tid, pid)){
			exclusiveLocksPageidToTidMap.put(pid, tid);
		}
	}
	
	public synchronized boolean checkAndAquireLock(TransactionId tid, PageId pid, Permissions perm){
		if(perm.equals(Permissions.READ_ONLY)){
			if(!holdsLock(tid, pid)){ //a page doesn't have any lock in that transaction
				HashSet<TransactionId> tidset = new HashSet<TransactionId>();
				if(shareLocksPageidToTidMap.containsKey(pid)){
					tidset = shareLocksPageidToTidMap.get(pid);
				}
				tidset.add(tid);
				shareLocksPageidToTidMap.put(pid, tidset);
				return true;
			}
			return false;
		}else if(perm.equals(Permissions.READ_WRITE)){ //idea: check share list, share lock is found, delete it. check ex lock, ex lock not found, add it. 
			if(holdsShareLock(tid, pid) && shareLocksPageidToTidMap.get(pid).size()==1){//has share lock, and there is only 1 transaction
				deleteShareLock(tid, pid);
			}
			if(!holdsLock(tid, pid)){
				exclusiveLocksPageidToTidMap.put(pid, tid);
				return true;
			}
			return false;
		}
		System.out.println("LockManager.java perm is not read only and read write");
		return false;
	}
	
	private synchronized void deleteShareLock(TransactionId tid, PageId pid){ //assume tid already in the list
		HashSet<TransactionId>tidset = shareLocksPageidToTidMap.get(pid);
		tidset.remove(tid);
		if(tidset.size()==0){
			shareLocksPageidToTidMap.remove(pid);
		}else{
			shareLocksPageidToTidMap.put(pid, tidset);
		}
	}
	
	private boolean holdsShareLock(TransactionId tid, PageId pid){
		if(!shareLocksPageidToTidMap.containsKey(pid)){
			return false;
		}else if (shareLocksPageidToTidMap.get(pid).contains(tid)){
			return true;
		}
		return false;
	}
	private boolean holdsExclusiveLock(TransactionId tid, PageId pid){
		if(!exclusiveLocksPageidToTidMap.containsKey(pid)){
			return false;
		}else if(exclusiveLocksPageidToTidMap.get(pid).equals(tid)){
			return true;
		}
		return false;
	}
}






