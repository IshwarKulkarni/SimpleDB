package simpledb;
import java.security.Permissions;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private TransactionId tid;
	private int tableid;
	private String tableAlias;
	private DbFile hf;
	private DbFileIterator ite;
	
	/**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        hf = Database.getCatalog().getDbFile(tableid);
        ite = hf.iterator(tid);

    }

    public void open()
        throws DbException, TransactionAbortedException {
        if(hf==null){
        	throw new DbException(" File does not exist");
        }
        ite.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        
        String[] prefixedNames = new String[td.getNumNames()];
        Type[] types = new Type[td.numFields()];
        for(int i=0;i<prefixedNames.length;i++){
        	prefixedNames[i] = tableAlias.concat(".").concat(td.getFieldName(i));
        	
        	types[i] = td.getType(i);
        }
        return new TupleDesc(types, prefixedNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return ite.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
    	if(ite.hasNext())
    		return ite.next();
    	else
    		throw new NoSuchElementException();
    }

    public void close() {
        // some code goes here
    	ite.close();
    	//ite = null;
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	if(hf==null){
    		throw new NoSuchElementException();
    	}
    	ite.rewind();
    }
}
