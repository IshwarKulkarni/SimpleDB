package simpledb;


public class IndexPageId implements PageId {

	//Defining possible categories of the pages
	//NOTE You might need other categories, or do not need any category at all based on your design
	public final static int BUCKET = 0;
	public final static int OVERFLOW = 1;

	private int tableId;
	private int pgNo;
	private int pgCateg;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     * @param pgcateg which kind of page it is (based on your chose index structure to implement)
     */
    public IndexPageId(int tableId, int pgNo, int pgcateg) {
    	this.tableId = tableId;
    	this.pgNo = pgNo;
    	this.pgCateg = pgcateg;
    }

    /** @return the table (index) associated with this PageId */
    public int getTableId() {
    	//Some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageno() {
    	//Some code goes here
        return pgNo;
    }


    /**
     * @return the category of this page
     */
    public int pgcateg() {
    	//Some code goes here
        return pgCateg;
    }



    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number
     * @see BufferPool
     */
    public int hashCode() {
    	//Some code goes here
    	return 100*tableId+pgNo;
    }


    /**
     * Compares one IndexPageId to another.
     *
     * @param o The object to compare against (must be a IndexPageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids and page categories are the same)
     */
    public boolean equals(Object o) {
    	if((o==null ) ||  !(o instanceof IndexPageId))
    		return false;
    	IndexPageId pgId = (IndexPageId) o;
    	if (pgId.pgNo == this.pgNo && pgId.tableId == this.tableId && pgId.pgCateg == this.pgCateg)
    		return true;
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args in the class
     */

    public int[] serialize() {
    	//Some code goes here
    	int[] res = new int[3];
    	res[0] = tableId;
    	res[1] = pgNo;
    	res[2] = pgCateg;
        return res;
   }
}


