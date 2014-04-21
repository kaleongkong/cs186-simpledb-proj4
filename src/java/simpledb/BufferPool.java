package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    protected volatile int numPages;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public Map<PageId, Page> pages;
    protected Map<PageId, Integer> pageid_to_ru; 
    protected int ru;
    protected LockManager lock_manager;
    int cachecount;
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	pages = new ConcurrentHashMap<PageId, Page>();
    	pageid_to_ru = new ConcurrentHashMap<PageId, Integer>(numPages); // least recent used is the page has least recent used count
    	lock_manager = new LockManager();
    	ru =0;
    	cachecount=0;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	boolean aquire = false;
    	
    	double current = System.currentTimeMillis();
    	while(!aquire){
    		if(System.currentTimeMillis()-current>100){
    			throw new TransactionAbortedException();
    		}
    		aquire = lock_manager.checkAndAquireLock(tid,pid,perm);
    	}
    	
    	Page p;
    	//System.out.println("page id in buffer pool getPage: "+ pid);
    	//System.out.println("numPages: "+numPages);
    	//System.out.println("page: "+pages.get(pid));
    	//System.out.println("num pages: "+pages.size());
		//System.out.println("ru: "+pageid_to_ru.size());
    	if (!pages.containsKey(pid)){
    		
    		DbFile dbfile = Database.getCatalog().getDbFile(pid.getTableId());
        	p = dbfile.readPage(pid);
        	if(pages.size()>=numPages){
        		evictPage();
        	}
    		pages.put(pid, p);
    	}else{
        	p = pages.get(pid);
    	}
    	pageid_to_ru.put(pid, new Integer(ru));
    	ru++;
        return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    	lock_manager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lock_manager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    	if(commit){
    		for(PageId k:pages.keySet()){
    			HeapPage page = (HeapPage)pages.get(k);
    			if(page.isDirty()!=null && page.isDirty().equals(tid)){
    				flushPage(k);
    			}
    			page.setBeforeImage();
    		}
    	}else{
    		for(PageId k:pages.keySet()){
    			HeapPage page = (HeapPage)pages.get(k);
    			if(page.isDirty()!=null && page.isDirty().equals(tid)){ //page that is dirty and belongs to this transaction
    				pages.put(k,page.getBeforeImage());
    			}
    		}
    	}
    	lock_manager.releaseAllLockOfATransaction(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	try{
    		//System.out.println("____________insert begin_____________");
    		DbFile dbfile = Database.getCatalog().getDbFile(tableId);
    		//System.out.println("____________got db file_____________");
    		//System.out.println("____________pages size before dbfile insert Tuple _____________: "+pages.size());
    		ArrayList<Page> pages= dbfile.insertTuple(tid, t);
    		//System.out.println("____________pages size after dbfile insert Tuple _____________: "+pages.size());
    		Iterator<Page> pagesitr = pages.iterator();
    		
    		while(pagesitr.hasNext()){
    			HeapPage hpage = (HeapPage)pagesitr.next();
    			//System.out.println("mark Dirty??");
    			//System.out.println("numPages: "+numPages);
            	//System.out.println("num of pages: "+pages.size());
    			hpage.markDirty(true, tid);
    			this.pages.put(hpage.getId(), hpage);
    			//System.out.println(((HeapPage)this.pages.get(hpage.getId())).dirty);
    		}
    	}catch (Exception e){
    		e.printStackTrace();
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
    	//try{
    		DbFile dbfile = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
    		//System.out.println(tid);
    		dbfile.deleteTuple(tid, t);
    	//}catch(Exception e){
    		//e.printStackTrace();
    	//}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
    	Set<PageId> pageids = pages.keySet();
    	Iterator<PageId> pagesitr = pageids.iterator();
    	while(pagesitr.hasNext()){
    		try {
				flushPage(pagesitr.next());
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    	
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	HeapFile hfile = (HeapFile)Database.getCatalog().getDbFile(pid.getTableId());
    	HeapPage hpage = (HeapPage)pages.get(pid);
    	try{
    		hfile.writePage(hpage);
    		hpage.markDirty(false, null);
    	}catch(IOException e){
    		throw new IOException();
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1s
    	for(PageId k: pages.keySet()){
    		if(pages.get(k).isDirty()!=null &&pages.get(k).isDirty().equals(tid)){
    			flushPage(k);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    	Set<PageId> pageids = pageid_to_ru.keySet();
    	Iterator<PageId> pageiditr = pageids.iterator();
    	PageId lrupageid = null;
    	int lru = Integer.MAX_VALUE;
    	while(pageiditr.hasNext()){
    		PageId next = pageiditr.next();
    		int next_ru = pageid_to_ru.get(next);
    		if(next_ru < lru && !((HeapPage)pages.get(next)).dirty){ //page selected cannot be dirty
    			lru = next_ru;
    			lrupageid = next;
    		}
    	}
    	/*System.out.println("expected to be here, lrupageid: "+lrupageid);*/
    	
    	if(lrupageid == null){ // this happens only when all pages are dirty
    		throw new DbException("All pages are dirty");
    	}
    	if(lru>(Math.pow(2,30))){
    		reducecount();
    	}
    	try {
			flushPage(lrupageid);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	pages.remove(lrupageid);
    	pageid_to_ru.remove(lrupageid);
    	
    }
    
    private void reducecount(){
    	Set<PageId> pageids = pageid_to_ru.keySet();
    	Iterator<PageId> pageiditr = pageids.iterator();
    	while(pageiditr.hasNext()){
    		PageId next = pageiditr.next();
    		int next_ru = pageid_to_ru.get(next);
    		next_ru = next_ru-(int)Math.pow(2,30);
    		pageid_to_ru.put(next, ru);
    	}
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    

}
