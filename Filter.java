package simpledb;
import java.util.*;



/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {

    private Predicate p;
	private DbIterator child;
	private boolean opened;

	/**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        opened = true;
        child.open();
    }

    public void close() {
        
        if(!opened){
        	try {
				throw new DbException("Iterator not opened to close yet");
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        opened = false;
        child.close();
     
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        Tuple local;
        while(child.hasNext()){
        	local  = child.next();
        	if(p.filter(local)){
        		return local;
        	}
        }
        return null;
    }
}
