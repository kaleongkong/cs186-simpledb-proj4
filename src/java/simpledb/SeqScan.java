package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    protected TransactionId tid;
    protected int tableid;
    protected String tableAlias;
    protected DbFileIterator tuples;
    protected DbFile dbf;
    
    protected TupleDesc td;
    
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	//System.out.println("SeqScan open");
    	dbf = Database.getCatalog().getDbFile(tableid);
    	//******
    	/*int count = 0;
    	DbFileIterator test = dbf.iterator(tid);
    	test.open();
    	while(test.hasNext()){
    		test.next();
    		count++;
    	}
    	System.out.println("SeqScan open iterator count: "+count);*/
    	//******
    	//if(dbf==null){
    	//	System.out.println("SeqScan dbf: "+dbf);
    	//}
    	tuples = dbf.iterator(tid);
    	//if(tuples==null){
    	//	System.out.println("SeqScan tuples: "+tuples);
    	//}
    	tuples.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	TupleDesc temptd = dbf.getTupleDesc();
    	Iterator<TDItem> tditems = temptd.iterator();
    	Type[] typeAr = new Type[temptd.numFields()];
    	String[] fieldAr = new String[temptd.numFields()];
    	int count = 0;
    	while(tditems.hasNext()){
    		TDItem tditem = tditems.next();
    		Type type = tditem.fieldType;
    		String name = tditem.fieldName;
    		String newname ="";
    		if(tableAlias == null && name == null){
    			newname ="null.null";
    		}else if (tableAlias == null){
    			newname = "null."+name;
    		}else{
    			newname = tableAlias+"."+name;
    		}
    		typeAr[count] = type;
    		fieldAr[count] = newname;
    		count++;
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	return tuples.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	
    	if( hasNext())
    		return tuples.next();
    	else
    		throw new NoSuchElementException();
    }

    public void close() {
        // some code goes here
    	if(tuples!=null){
    		tuples.close();
    	}
    	tuples = null;
    	dbf= null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }
}
