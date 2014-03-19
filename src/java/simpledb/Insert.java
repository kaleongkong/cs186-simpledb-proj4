package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public TransactionId t;
    public TupleDesc td;
    public DbIterator child;
    public int tableid;
    public HeapFile hpfile;
    public int count;
    public boolean fetched;
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	HeapFile hpfile = (HeapFile)Database.getCatalog().getDbFile(tableid);
    	this.child = child;
    	try {
			this.child.open();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	TupleDesc childtd = this.child.getTupleDesc();
    	TupleDesc td = hpfile.getTupleDesc();
    	if(!td.equals(childtd)){
    		throw new DbException("Insert Operator: child tupledesc doesn't match table tupledesc");
    	}
    	this.t=t;
    	this.fetched = false;
    	this.tableid = tableid;
    	this.hpfile = hpfile;
    	Type[] types = new Type[1];
		types[0] = Type.INT_TYPE;
		this.td = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    	count = 0;
    }

    public void close() {
        // some code goes here
    	super.close();
    	child.close();
    	count=0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(fetched){
    		return null;
    	}
    	while(child.hasNext()){
    		try {
				Database.getBufferPool().insertTuple(t, tableid, child.next());
			} catch (NoSuchElementException e){
				e.printStackTrace();
    		}catch (IOException e) {
				e.printStackTrace();
			}
    		count++;
    	}
    	fetched = true;
    	Tuple ct = new Tuple(td);
		ct.setField(0, new IntField(count));
		return ct;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] r = new DbIterator[1];
    	r[0]=child;
        return r;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child= children[0];
    }
}
