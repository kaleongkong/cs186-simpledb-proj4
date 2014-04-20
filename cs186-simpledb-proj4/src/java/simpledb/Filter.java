package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    protected Predicate p;
    protected DbIterator current;
    protected TupleDesc td;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	current = child;
    	try {
			current.open();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	td = current.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	
    	//restDbIterators.add(current);
    	super.open();
    }

    public void close() {
        // some code goes here
    	current.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    	current.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while(current.hasNext()){
    		Tuple t = current.next();
    		if(p.filter(t)){
    			return t;
    		}
    	}
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] darray = new DbIterator[1];
    	darray[0] = current;
        return darray;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if(children !=null && children.length>0){
    		current = children[0];
    	}
    }

}
