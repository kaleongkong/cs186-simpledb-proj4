package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import simpledb.Aggregator.Op;
import simpledb.IntegerAggregator.Container;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    protected int gbfield;
    protected Type gbfieldtype;
    protected int afield;
    protected Op op;
    protected TupleDesc td;
    HashMap <Field, Integer> map = new HashMap<Field, Integer>();
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	op = what;
    	this.map = new HashMap<Field, Integer>();
    	Type[] types = null;
    	if (gbfield == Aggregator.NO_GROUPING){
    		types = new Type[1];
    		types[0] = Type.INT_TYPE;
    	}else{
    		types = new Type[2];
        	types[0] = gbfieldtype;
        	types[1] = Type.INT_TYPE;
    	}
    	td = new TupleDesc(types);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field groupfield =null;
    	if (gbfield != NO_GROUPING){
    		groupfield = tup.getField(gbfield);
    	}
    	int c =0;
    	Integer count = null;
    	if(map.containsKey(groupfield)){
    		c = map.get(groupfield).intValue();
    		c++;
    		count = new Integer(c);
    	}else{
    		count = new Integer(1);
    	}
    	map.put(groupfield, count);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Set<Field> keys = map.keySet();
    	Iterator<Field> keyItr = keys.iterator();
    	ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
    	while(keyItr.hasNext()){
    		Tuple t = new Tuple(td);
    		Field key = (Field)keyItr.next();
    		if(gbfield == Aggregator.NO_GROUPING){
    			t.setField(0, new IntField(map.get(key)));
    		}else{
    			t.setField(0, key);
    			t.setField(1, new IntField(map.get(key)));
    		}
    		tuplelist.add(t);
    	}
    	return new TupleIterator(td, tuplelist);
    }

}
