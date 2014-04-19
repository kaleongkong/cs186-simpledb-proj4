package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	volatile Map<PageId, HashSet<TransactionId>> shareLocksPageidToTidMap;
	volatile Map<PageId, TransactionId> exclusiveLocksPageidToTidMap;
	public LockManager(){
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
			exclusiveLocksPageidToTidMap.remove(pid);
		}
	}
	public synchronized void releaseAllLockOfATransaction(TransactionId tid){
		for(PageId k: shareLocksPageidToTidMap.keySet()){
			if(shareLocksPageidToTidMap.get(k).contains(tid)){
				deleteShareLock(tid,k);
			}
		}
		for(PageId k: exclusiveLocksPageidToTidMap.keySet()){
			if(exclusiveLocksPageidToTidMap.get(k).equals(tid)){
				exclusiveLocksPageidToTidMap.remove(k);
			}
		}
	}
	
	public synchronized boolean checkAndAquireLock(TransactionId tid, PageId pid, Permissions perm){
		if(perm.equals(Permissions.READ_ONLY)){
			//System.out.println("pid: "+pid);
			if(holdsLock(tid,pid)){
				return true;
			}else if(exclusiveLocksPageidToTidMap.containsKey(pid)){ // this page has a exclusive lock in another transaction
				return false;
			}else if(shareLocksPageidToTidMap.containsKey(pid)){ // this page has share locks in other transaction(s)
				HashSet<TransactionId> s = shareLocksPageidToTidMap.get(pid);
				s.add(tid);
				shareLocksPageidToTidMap.put(pid, s);
				return true;
			}else if(!shareLocksPageidToTidMap.containsKey(pid)){ // this page has no share locks at all
				HashSet<TransactionId> s = new HashSet<TransactionId>();
				s.add(tid);
				shareLocksPageidToTidMap.put(pid, s);
				return true;
			}
			System.out.println("checkandaquirelock read only no way to be here");
			return false;
		}else if(perm.equals(Permissions.READ_WRITE)){ //idea: check share list, share lock is found, delete it. check ex lock, ex lock not found, add it. 
			if(holdsExclusiveLock(tid, pid)){
				return true;
			}else if(holdsShareLock(tid, pid)){
				if(shareLocksPageidToTidMap.get(pid).size()==1){
					deleteShareLock(tid,pid);
					exclusiveLocksPageidToTidMap.put(pid,tid);
					return true;
				}else{
					return false;
				}
			}else if(!exclusiveLocksPageidToTidMap.containsKey(pid) && !shareLocksPageidToTidMap.containsKey(pid)){
				exclusiveLocksPageidToTidMap.put(pid, tid);
				return true;
			}else if(exclusiveLocksPageidToTidMap.containsKey(pid) ||shareLocksPageidToTidMap.containsKey(pid) ){
				return false;
			}
			System.out.println("checkandaquirelock read write no way to be here");
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
		}else if (!shareLocksPageidToTidMap.get(pid).contains(tid)){//page has share lock in another transaction but not this one
			return false;
		}else if(shareLocksPageidToTidMap.get(pid).contains(tid)){
			return true;
		}
		System.out.println("holdsShareLock: no way to be here");
		return false;
	}
	private boolean holdsExclusiveLock(TransactionId tid, PageId pid){
		if(!exclusiveLocksPageidToTidMap.containsKey(pid)){
			return false;
		}else if(!exclusiveLocksPageidToTidMap.get(pid).equals(tid)){
			return false;
		}else if(exclusiveLocksPageidToTidMap.get(pid).equals(tid)){
			return true;
		}
		System.out.println("holdsExclusiveLock: no way to be here");
		return false;
	}
}






