package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
	protected int table_id;
	protected int pg_no;
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
    	table_id = tableId;
    	pg_no = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return table_id;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        // some code goes here
        return pg_no;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
    	return (int) (table_id*(Math.log10((double)pg_no)+1)+pg_no);
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o instanceof PageId){
	        HeapPageId pid = (HeapPageId)o;
	        if(pid.getTableId()==this.getTableId() && pid.pageNumber()==this.pageNumber()){
	        	return true;
	        }
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
