package simpledb;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

	private PageId pid;
	private int tupleno;
	
    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
    	this.pid = pid;
    	this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	// some code goes here
    	if((o==null ) ||  !(o instanceof RecordId)){
    		return false;
    	}
    	RecordId recId = (RecordId) o;
    	if (recId.pid.equals(pid) && recId.tupleno == tupleno)
    			return true;
        return false;
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	// some code goes here
    	return pid.pageno()*100+tupleno;
    }
    
}
