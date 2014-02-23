package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Iterator<Tuple> tuples;
	TransactionId tid;
	HeapPage hp;
	int page_num;
	HeapFile f;
	public HeapFileIterator(HeapFile f, TransactionId tid){
		this.f = f;
		this.tid = tid;
		page_num = 0;
	}
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		hp = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(f.getId(), page_num), Permissions.READ_ONLY);
		if(hp!=null){
			tuples = hp.iterator();
		}
	}

	@Override
	public boolean hasNext() throws DbException,TransactionAbortedException {
		// TODO Auto-generated method stub
		if(hp == null){
			return false;
		}
		
		if(tuples.hasNext()){
			return true;
		}
		if(page_num<f.numPages()-1){
			return true;
		}
		return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		// TODO Auto-generated method stub
		if(!hasNext()){
			throw new NoSuchElementException();
		}
		if(!tuples.hasNext()){
			page_num++;
			open();
		}
		
		return tuples.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		close();
		open();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		this.tid = null;
		hp=null;
		this.page_num=0;
	}
	
}
