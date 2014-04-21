package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.Predicate.Op;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public class MinMaxContainer {
    	public int min;
    	public int max;
    	public MinMaxContainer(){
    		this.min = Integer.MAX_VALUE;
    		this.max = Integer.MIN_VALUE;
    	}
    	public MinMaxContainer(int min, int max){
    		this.min = min;
    		this.max = max;
    	}
    }
    protected int tableid;
    protected int ioCostPerPage;
    protected HashMap<String, MinMaxContainer> min_max_hash = new HashMap<String, MinMaxContainer>();
    protected HashMap<String, Object> histograms = new HashMap<String, Object>();
    protected HeapFile file;
    protected int numfields;
    protected TupleDesc td;
    protected int numtuples;
    public TableStats(int tableid, int ioCostPerPage) {
    	
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	file = (HeapFile)Database.getCatalog().getDbFile(tableid);
    	Transaction t = new Transaction();
    	DbFileIterator iterator = file.iterator(t.getId());
    	td = file.getTupleDesc();
    	this.numfields = td.numFields();
    	numtuples = 0;
    	try {
			iterator.open();
			while(iterator.hasNext()){
				Tuple tuple = iterator.next();
				updateMinMaxHash(tuple, td);
				numtuples++;
			}
			iterator.rewind();
			iterator.open();
			while(iterator.hasNext()){
				Tuple tuple = iterator.next();
				updateHistograms(tuple, td);
			}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	
    	
    }
    private void updateHistograms(Tuple tuple, TupleDesc td){
		for(int i =0; i<numfields; i++){
			Field f = tuple.getField(i);
			String fieldname = td.getFieldName(i);
			if(f.getType().equals(Type.INT_TYPE)){
				int val = ((IntField)f).getValue();
				if(histograms.containsKey(fieldname)){
					((IntHistogram)histograms.get(fieldname)).addValue(val);
				}else{
					MinMaxContainer m = min_max_hash.get(fieldname);
					IntHistogram ih = new IntHistogram(NUM_HIST_BINS, m.min, m.max);
					ih.addValue(val);
					histograms.put(fieldname, ih);
				}
			}else{
				String val = ((StringField)f).getValue();
				if(histograms.containsKey(fieldname)){
					((StringHistogram)histograms.get(fieldname)).addValue(val);
				}else{
					StringHistogram sh = new StringHistogram(NUM_HIST_BINS);
					sh.addValue(val);
					histograms.put(fieldname, sh);
				}
			}
		}
    }
    private void updateMinMaxHash(Tuple tuple, TupleDesc td){
		for(int i =0; i<numfields; i++){
			Field f = tuple.getField(i);
			if(f.getType().equals(Type.INT_TYPE)){
				String fieldname = td.getFieldName(i);
				int val = ((IntField)f).getValue();
				MinMaxContainer m = null;
				if(min_max_hash.containsKey(fieldname)){
					m = min_max_hash.get(fieldname);
					if(val<m.min){
						m.min=val;
					}
					if(val>m.max){
						m.max=val;
					}
					min_max_hash.put(fieldname, m);
				}else{
					m = new MinMaxContainer(val, val);
				}
				min_max_hash.put(fieldname, m);
			}
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return file.numPages()*this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(numtuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	String fieldname = td.getFieldName(field);
    	Object histogram = histograms.get(fieldname);
    	if(constant.getType().equals(Type.INT_TYPE)){
    		return ((IntHistogram)histogram).estimateSelectivity(op, ((IntField)constant).getValue());
    	}else{
    		return ((StringHistogram)histogram).estimateSelectivity(op, ((StringField)constant).getValue());
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
