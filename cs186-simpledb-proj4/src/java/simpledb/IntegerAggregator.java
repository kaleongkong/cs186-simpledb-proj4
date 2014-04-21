package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     *            
     *            
     */
    public class Container{
    	public IntField field;
    	public int count;
    	public int sum;
    	public Container(){
    		
    	}
    	public Container(IntField field, int count, int sum){
    		this.field= field;
    		this.count = count;
    		this.sum = sum;
    	}
    }
    protected int gbfield;
    protected Type gbfieldtype;
    protected int afield;
    protected Op op;
    protected TupleDesc td;
    protected HashMap<Field, Container> amap; //key: gbfield, value: field, count, sum
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.op = what;
    	this.amap = new HashMap<Field, Container>();
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field groupfield =null;
    	if (gbfield != NO_GROUPING){
    		groupfield = tup.getField(gbfield);
    	}
    	IntField aggrefield_toadd = (IntField)tup.getField(afield);
    	Container values = new Container();
    	IntField aggrefield = null;
    	
    	if(amap.containsKey(groupfield)){
    		values = amap.get(groupfield);
    		aggrefield = values.field;
    		int count = values.count;
    		int sum = values.sum;
    		values = mergeFields(aggrefield, aggrefield_toadd, count, sum);
    	}else{
    		values.count = 1;
    		if(op != Op.COUNT){
    			values.field = aggrefield_toadd;
    		} else {
    			values.field = new IntField(values.count);
    		}
    		values.sum = new Integer(aggrefield_toadd.toString()).intValue();
    	}
		amap.put(groupfield, values);
    }
    
    public Container mergeFields(IntField existed, IntField toadd, int count, int sum){
    	int aggreVal_int = 0;
    	int existedint = new Integer(existed.toString()).intValue();
    	int toaddint = new Integer(toadd.toString()).intValue();
    	sum = sum+toaddint;
    	count++;
    	switch(op){
    		case AVG: 
    			int avg = sum/count;
    			aggreVal_int = avg;	  
    			break;
    		case COUNT: 
    			aggreVal_int = count;
    			break;
    		case MAX: 
    			aggreVal_int = existedint;
    			if(toaddint>existedint){
    				aggreVal_int = toaddint;
    			}
    			break;
    		case MIN: 
    			aggreVal_int = existedint;
    			if(toaddint<existedint){
    				aggreVal_int = toaddint;
    			}
    			break;
    		case SUM: 
    			aggreVal_int = sum;
    			break;
    		default:
    			System.out.println("It's not possible to get here");
    	}
    	return new Container(new IntField(aggreVal_int), count, sum);
    }



    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	Set<Field> keys = amap.keySet();
    	Iterator<Field> keyItr = keys.iterator();
    	ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
    	while(keyItr.hasNext()){
    		Tuple t = new Tuple(td);
    		Field key = (Field)keyItr.next();
    		if(gbfield == Aggregator.NO_GROUPING){
    			t.setField(0, amap.get(key).field);
    		}else{
    			t.setField(0, key);
    			t.setField(1, amap.get(key).field);
    		}
    		tuplelist.add(t);
    	}
    	return new TupleIterator(td, tuplelist);
        
    }

}
