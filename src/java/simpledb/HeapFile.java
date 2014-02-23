package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	protected File file;
	protected TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	file = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
    	int page_size = BufferPool.PAGE_SIZE;
    	byte[] buffer = new byte[page_size];
    	HeapPageId hpid = new HeapPageId(this.getId(), offset);
    	Page p;
    	try{
    		RandomAccessFile raf = new RandomAccessFile(file, "r");
    		raf.seek(offset);
    		raf.read(buffer, 0, page_size);
    		p = new HeapPage(hpid, buffer);
    		raf.close();
    		return p;
    	} catch (Exception e){
    		e.printStackTrace();
    	}
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    	int offset = page.getId().pageNumber()*BufferPool.PAGE_SIZE;
    	int page_size = BufferPool.PAGE_SIZE;
    	byte[] buffer = new byte[page_size];
    	try{
    		RandomAccessFile raf = new RandomAccessFile(file, "rw");
    		raf.seek(offset);
    		buffer = page.getPageData();
    		raf.write(buffer, 0, page_size);
    		raf.close();
    	} catch (Exception e){
    		e.printStackTrace();
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	HeapPage page = null;
    	ArrayList<Page> pages = new ArrayList<Page>();
    	try{
    		int tableid = getId();
    		int pageno = 0;
    		
    		while(pageno<numPages()){
    			HeapPageId pidtobeinserted = new HeapPageId(tableid, pageno);
    			page = (HeapPage)Database.getBufferPool().getPage(tid, pidtobeinserted, Permissions.READ_WRITE);
    			pages.add(page);
    			if(page.getNumEmptySlots()>0){
    				RecordId trid = new RecordId(pidtobeinserted, pageno);
    				t.setRecordId(trid);
    				page.insertTuple(t);
    				break;
    			} else{
    				pageno++;
    				pidtobeinserted=new HeapPageId(tableid, pageno);
    				if(pageno>=numPages()){
    					HeapPage newpage = new HeapPage(pidtobeinserted, HeapPage.createEmptyPageData());
    					writePage(newpage);
    				}
    			}
    		}
    	}catch(Exception e){
    		e.printStackTrace();
    		throw new DbException("Exception at HeapFile addTuple");
    	}
    	return pages;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	HeapPage page = null;
    	try{
    		page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().pid, Permissions.READ_WRITE);
    		page.deleteTuple(t);
    	}catch(Exception e){
    		throw new DbException("Exception at HeapFile deleteTuple");
    	}
    	return page;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
		return new HeapFileIterator(this, tid);		
    }
    

    

}

