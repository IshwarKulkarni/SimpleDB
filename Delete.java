package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
	
	private TransactionId tid;
	private DbIterator child;
	private int tableid;
	private DbFile file;
	private TupleDesc td;
	private boolean run;
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	Type[] types = new Type[1];
    	types[0] = Type.INT_TYPE;
    	td = new TupleDesc(types);
    	run = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	run = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be access via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(child.hasNext() || !run)
    	{
    		int i = 0;
    		Tuple next = null;
    		while(child.hasNext()) {
    			i++;
    			next = child.next();
    			tableid = next.getRecordId().getPageId().getTableId();
    			file = Database.getCatalog().getDbFile(tableid);
    			file.deleteTuple(tid, next);
    		}
    		Tuple t = new Tuple(td);
    		t.setField(0, new IntField(i));
    		run = true;
    		return t;
    	}
    	return null;
    }
}
