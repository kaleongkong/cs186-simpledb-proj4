package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    protected TransactionId t;
    protected DbIterator child;
    protected TupleDesc td;
    protected boolean fetched;
    protected int count;
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	Type[] types = new Type[1];
		types[0] = Type.INT_TYPE;
		this.child = child;
    	try {
			child.open();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
		this.td = new TupleDesc(types);
		this.fetched = false;
		this.t = t;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	count=0;
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
    	child.rewind();
    	open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(fetched){
    		return null;
    	}
    	while(child.hasNext()){
    		//try {
				Database.getBufferPool().deleteTuple(t, child.next());
			//} catch (Exception e) {
				//e.printStackTrace();
			//}
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
