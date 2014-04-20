package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    protected DbIterator child;
    protected int afield;
    protected int gfield;
    protected Aggregator.Op aop;
    protected Type gbfieldtype;
    protected TupleDesc td;
    protected Aggregator aggr;
    protected DbIterator aggritr;
    protected Type gtype;
    protected Type atype;
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	Type[]types = null;
    	String[] names = null;
    	gtype = null;
    	gbfieldtype = null;
    	//System.out.println(child.getTupleDesc());
    	try {
			child.open();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	atype = child.getTupleDesc().getFieldType(afield);
    	if(gfield!= Aggregator.NO_GROUPING){
    		gtype = child.getTupleDesc().getFieldType(gfield);
        	types = new Type[2];
        	types[0] = gtype;
        	types[1] = atype;
        	names = new String[2];
        	names[0] = this.groupFieldName();
        	names[1] =this.aggregateFieldName();
        	gbfieldtype = atype;
    	}else{
    		types = new Type[1];
    		types[0] = atype;
    		names = new String[1];
    		names[0] = aggregateFieldName();
    	}
    	td = new TupleDesc(types, names);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if(gfield==Aggregator.NO_GROUPING){
    		return null;
    	}
    	return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	super.open();
    	child.open();
    	if( atype == Type.INT_TYPE){
    		aggr = new IntegerAggregator(gfield, gtype,afield,aop);
    	}else if (atype ==Type.STRING_TYPE){
    		aggr = new StringAggregator(gfield, gtype,afield,aop);
    	}
    	
    	while(child.hasNext()){
			aggr.mergeTupleIntoGroup(child.next());
		}
    	aggritr = aggr.iterator();
    	aggritr.open();
    	
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if(aggritr.hasNext()){
    		return aggritr.next();
    	}
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return td;
    }

    public void close() {
	// some code goes here
    	super.close();
    	child.close();
    	td = null;
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
    	DbIterator[] r = new DbIterator[1];
    	r[0] = child;
	return r;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
    	child = children[0];
    }
    
}
