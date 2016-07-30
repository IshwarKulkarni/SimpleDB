package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * IndexFile is an implementation of a DbFile that stores a collection
 * of tuples (index entries) in an index structure.
 */


public class IndexFile implements DbFile {


    static final int NUM_OF_BUCKETS = 97;	//Size of the index (in case of using static hashing)
    boolean hasDuplicates ;
    File file = null;
    int fid;
    TupleDesc indexTd = null;
    int indexFieldNum = 0;
    static int TotNumPages = 1;
    boolean heapTableIDSet = false;
    int heapId = -1; // table on 
    
    /**
     * Constructor
     * create a new IndexFile that stores pages in the specified pool.
     *
     * NOTE: Index Structure is stored similar to a table in Catalog (@Check Catalog)
     *
     *
     *
     *
     * @param ixf A reference to a base file that stores the file on disk -- your code may
     *  use additional files besides this one to store index file state.
     *
     * @param u A boolean that specifies if the index can have duplicate keys or not
     */
    public IndexFile(File ixf, boolean u) {
    	file = ixf;
    	hasDuplicates = u;
    	TotNumPages = (int) file.length()/BufferPool.DEFAULT_PAGES;
    	this.fid = file.getAbsoluteFile().hashCode();
    	
/*    	if(file.exists()){ //this index was used before software start
    		try {
				this.headerPage = Database.getBufferPool().getPage(new TransactionId(), new HeapPageId(file.getAbsoluteFile().hashCode(), 0), Permissions.READ_WRITE);
				tableIDoftableThisIndexIndexesOn = ((IndexPage)headerPage).getOverflowPgnu(); // pagenum = 1st 4 bytes, so that is the tableId
				heapTableIDSet = true;
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}*/
    }

    /**
     * This method lets you initialize state for the index file
     *
     * You clear the content of an already existing index file instance (if any)
     * and you set default values (based on your design) for different attributes
     * of the index file
     *
     * If you do not need to initialize state, you can leave this method empty.
     * Practically you might want to ignore this method and do all the things in the constructor
     */
    public void init() throws IOException{
    	IndexPageId pid; 
    	IndexPage page;
    	TransactionId tid = new TransactionId();
    	for(int i=0; i<NUM_OF_BUCKETS; i++) {
    		pid = new IndexPageId(fid, i, 0);
    		try {
				page = new IndexPage(pid, new byte[BufferPool.PAGE_SIZE]);
				page.setOverflowPageNum(-1);
        		Database.getBufferPool().forcePage(page, tid);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				e.printStackTrace();
			}
    	}
    	TotNumPages = NUM_OF_BUCKETS;
    }


	//set the index of the key field for this index
	//This IndexFile is built on that field on the indexed table
	public void setKeyField(int key){
	   indexFieldNum = key;
    }

    public int getId() {
    	return fid;
    }
    
    public void setHeapId(int hid) {
    	this.heapId = hid;
    }

    //set the TupleDesc for tuples (index entries) of the IndexFile
    //
    /*TupleDesc is fixed for an IndexEntry. An index entry has 3 fields showing (key, pgnu, slotnu) while
	* 		key --> contains the value of the index key field from the tuple the index entry corresponds to,
			(key can be either INT_TYPE or STRING_TYPE in SimpleDB)
	* 		pgnu --> pgnu of the tuple the index entry corresponds to,
    * 		slotnu --> tupleno of the tuple the index entry corresponds to
    *@param keyFieldType type of the indexed field (will be set according to the indexed table, when the method is called)
    */
    public void setTupleDesc(Type keyFieldType){
    	Type[] type = new Type[3];
    	type[0] = keyFieldType;
    	type[2] = type[1] = Type.INT_TYPE;
    	
	 	indexTd = new TupleDesc(type); // anonymous columns, 
    }

    public TupleDesc getTupleDesc() {
    	//Some code goes here
    	return indexTd;
    }


    /**
     * Returns an index Page from the index file
     */
    public Page readPage(PageId pid) throws NoSuchElementException {
    	//Some code goes here
    	
    	int pageNum = pid.pageno();
    	int pageSize = Database.getBufferPool().PAGE_SIZE;
    	byte[] readBuffer = new byte[pageSize];
    	FileInputStream fis = null;
    	if( pageNum*pageSize + pageSize >= file.length()){
    		throw new NoSuchElementException();
    	}
    	
    	try {
			fis = new FileInputStream(file);
			fis.skip(pageNum*pageSize);
			fis.read(readBuffer, 0, pageSize);
			fis.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Integer i = null ;
		while(!Database.getCatalog().IDToFile.keys().hasMoreElements()){
			i = Database.getCatalog().IDToFile.keys().nextElement();
			if(Database.getCatalog().IDToFile.get(i) == this){
				break;
			}
		}
		
		// = new IndexPageId(i.intValue(), pageNum, NUM_OF_BUCKETS > pageNum ?1:0);
		
		IndexPage ip = null;
		try {
			ip = new IndexPage((IndexPageId)pid, readBuffer);
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		return ip;
    }

    /**
     * Push the specified page to disk.
     * This page must have been previously read from this file via a call to
     * readPage.
     *
     * @throws IOException if the write fails
     */
    public void writePage(Page page) throws IOException {
    	IndexPage iPage = (IndexPage)page;
    	byte[] byteArray = iPage.getPageData();
    	
    	int pageNumber = iPage.getId().pageno();
    	int offSet = (pageNumber)*BufferPool.PAGE_SIZE;
    	
    	RandomAccessFile oStream = null;
    	
    	oStream = new RandomAccessFile(file,"rw");
    	oStream.skipBytes(offSet);
    	oStream.write(byteArray);
    	
    }

    /** Create an Index entry for the the tuple t
     *  TupleDesc for index entry is defined [(key, pgnu, slotnu)]
     * 	You use this method in insert() below
     *
     * @param t : tuple to which the generated index entry corresponds
     * @return
     */
    public Tuple createIndexEntry(Tuple t){
    	//Some code goes here
    	//You may change or ignore this method based on your design decisions
    	return null;
    }

    /** Return the RecordId for the tuple to which a given index entry corresponds
     *
     * @param indexEntry
     * @return RecordId generated based on indexEntry
     */
    public RecordId getRidFromIxEntry(Tuple indexEntry){
    	
    	int pageNo = ((IntField) indexEntry.getField(1)).getValue();
    	int slotNo = ((IntField) indexEntry.getField(2)).getValue();
    	return new RecordId( new HeapPageId(heapId, pageNo) , slotNo);
    	
    	
    }

    /**
     * Adds the specified tuple to the index file on behalf of transaction.
     *
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple will be updated to reflect that
     *          it is now stored in this file.
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */

    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException,
               UnsupportedOperationException {
    	
    	Field f = t.getField(indexFieldNum);
    
    	int bucketNum = 0;
    	ArrayList<Page> overFlowLink = new ArrayList<Page>();
    	BufferPool bp = Database.getBufferPool();
    	bucketNum = HashFunction.getHash(f);
    	
    	IndexPage page = (IndexPage)bp.getPage(tid,new IndexPageId(getId(), bucketNum, 0), Permissions.READ_WRITE);//adPage(new IndexPageId(getId(), bucketNum, 0)); 
    	
    	//System.out.println("Added to page: "+page.getId().pageno()+" which had "+page.getNumEmptySlots()+" empty slots and overflow page "+page.getOverflowPgnu());
    	while(page.getNumEmptySlots() == 0){
    		if(page.getOverflowPgnu()<NUM_OF_BUCKETS){
    			page.setOverflowPageNum(TotNumPages);
    			overFlowLink.add(page);
    			//page.markDirty(true,tid);
    			page = new IndexPage(new IndexPageId(fid, TotNumPages++, 1), new byte[Database.getBufferPool().PAGE_SIZE]);
    			page.setOverflowPageNum(-1);
        		Database.getBufferPool().forcePage(page, tid);
    		
    		}
    		else{
    			page = (IndexPage)bp.getPage(tid,new IndexPageId(getId(), page.getOverflowPgnu(), 1), Permissions.READ_WRITE);//readPage(new IndexPageId(fid, page.getOverflowPgnu(), 1));
    		}
    	}
    	
    	overFlowLink.add(page);
    	Type[] types = new Type[3];
    	types[0] = f.getType();
    	types[2] = types[1] = Type.INT_TYPE;
    	Tuple u = new Tuple(new TupleDesc(types));
    	u.setField(0, f);
//    	System.out.println("Page: "+t.getRecordId().getPageId().pageno()+" Tuple: "+t.getRecordId().tupleno());
//    	System.out.println("Inserted into Page: "+page.getId().pageno());
    	u.setField(1, new IntField(t.getRecordId().getPageId().pageno()));
    	u.setField(2, new IntField(t.getRecordId().tupleno()));
    	page.addTuple(u);
		bp.dirtyPage(page, true, tid);
//    	System.out.println("U Page: "+u.getRecordId().getPageId().pageno());
//    	System.out.println("Inserted tuple: "+u.toString()+" into bucket: "+page.getId().pageno());
    	return overFlowLink;
   }


    /**
     * Removes the index entry for the specified tuple from the index file on behalf of the specified
     * transaction.
     *
     *@param tid The transaction performing the update
     *@param t The tuple (from the target table, on which index is built) for which the
     *			corresponding index entry should be removed
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException{
    	//Some code goes here
    	Field f = t.getField(indexFieldNum);
    	BufferPool bp = Database.getBufferPool();
    	int bucketNum = 0;
    	bucketNum = HashFunction.getHash(f);
    	IndexPage page = (IndexPage)bp.getPage(tid,new IndexPageId(getId(), bucketNum, 0), Permissions.READ_WRITE );//readPage(new IndexPageId(getId(), bucketNum, 0)); 
    	
    	Type[] types = new Type[3];
    	types[0] = f.getType();
    	types[2] = types[1] = Type.INT_TYPE;
    	Tuple u = new Tuple(new TupleDesc(types));
    	u.setField(0, f);
    	u.setField(1, new IntField(t.getRecordId().getPageId().pageno()));
    	u.setField(2, new IntField(t.getRecordId().tupleno()));
    	
    	boolean deleted = false;
    	while(!deleted) {
    		deleted = page.deleteTuple(u);
    		if(!deleted && page.getOverflowPgnu()==-1)
    			throw new DbException("Tuple not found");
    		else if(!deleted)
    			page = (IndexPage)bp.getPage(tid,new IndexPageId(getId(), page.getOverflowPgnu(), 1), Permissions.READ_WRITE);//readPage(new IndexPageId(file.getAbsolutePath().hashCode(), page.getOverflowPgnu(), 1));
    	}
    		
    	return page;
    }


    /**
     * Search for index entry (or index entries) correspond to field k
     * Predicate for search is defined using AccessPredicate
     *
     * @see AccessPredicate
     * @see Predicate
     *
     * @param tid The transaction performing the update
     * @param ap Desired predicate for search
     * @return List of RecordIds of tuples from the target table that have k in their indexed field
     * 		   (Empty ArrayList if no matching entry found)
     *
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public ArrayList<RecordId> search(TransactionId tid, AccessPredicate ap)
	throws DbException, TransactionAbortedException {
    	Tuple t ;
    	ArrayList<RecordId> returnList = new ArrayList<RecordId>();
    	boolean end;
    	IndexPageId pid = null;
    	IndexPage iterPage = null;
    	Iterator<Tuple> pageIterator = null;
    	BufferPool buffPool = Database.getBufferPool();
    	if(ap.getOp()==Predicate.Op.EQUALS) {
    		pid = new IndexPageId(this.getId(), HashFunction.getHash(ap.getField()), 0);
    		iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
    		pageIterator = iterPage.iterator();
    		end = false;
    		while(!end || pageIterator.hasNext()) {
    			if(!pageIterator.hasNext()) {
    				if(iterPage.getOverflowPgnu()>=NUM_OF_BUCKETS) {
    					 pid = new IndexPageId(this.getId(), iterPage.getOverflowPgnu(), 1);
    					 iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
    					 pageIterator = iterPage.iterator();
    				}
    				else
    					end = true;
    			}
    			else {
    				t = pageIterator.next();
                	if(ap.filter(t, indexFieldNum)){
                		RecordId rid = new RecordId(new HeapPageId(heapId, ((IntField)(t.getField(1))).getValue()),
                					((IntField)(t.getField(2))).getValue());
        				returnList.add(rid);
        			}
    			}	
    		}
    		return returnList;
    	}
    		
    	for(int i=0;i<NUM_OF_BUCKETS;i++){
            pid = new IndexPageId(this.getId(), i, 0);
            iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
            pageIterator = iterPage.iterator();
            end = false;
            while(!end || pageIterator.hasNext()){
                if(!pageIterator.hasNext()) {
                    if(iterPage.getOverflowPgnu()>=NUM_OF_BUCKETS) {
                        pid = new IndexPageId(this.getId(), iterPage.getOverflowPgnu(), 1);
                        iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
                        pageIterator = iterPage.iterator();
                    }
                    else
                        end = true;
                }
                else {
                	t = pageIterator.next();
                	if(ap.filter(t, indexFieldNum)){
                		RecordId rid = new RecordId(new HeapPageId(heapId, ((IntField)(t.getField(1))).getValue()),
                					((IntField)(t.getField(2))).getValue());
        				returnList.add(rid);
        			}
                }
            }
        }

		return returnList;
    }

    /**
    * Get the specified tuples from the file based on its keyField value
    * on behalf of the specified transaction.
    * In fact it is a iterator-based filtered scan. Going through the index entries
    * those who satisfy the specified AccessPredicate will be returned one by one (via returned iterator)
    *
    * @param tid The transaction performing the update
    * @param ap AccessPredicate which specifies the scan condition
    *
    * @return an Iterator going through the tuples and returning the one who satisfy the desired predicate
    */
    public DbFileIterator filterScan(TransactionId tid, AccessPredicate ap){
    	//Some code goes here
    	Tuple t ;
    	ArrayList<Tuple> returnList = new ArrayList<Tuple>();
    	boolean end;
    	for(int i=0;i<NUM_OF_BUCKETS;i++){
            IndexPageId pid = new IndexPageId(this.getId(), i, 0);
            IndexPage iterPage = null;
            try {
                BufferPool buffPool = Database.getBufferPool();
                iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
            } catch (TransactionAbortedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (DbException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            Iterator<Tuple> pageIterator = iterPage.iterator();
            end = false;
            while(!end || pageIterator.hasNext()){
                if(!pageIterator.hasNext()) {
                    if(iterPage.getOverflowPgnu()>=NUM_OF_BUCKETS) {
                        pid = new IndexPageId(this.getId(), iterPage.getOverflowPgnu(), 1);
                        BufferPool buffPool = Database.getBufferPool();
                        try {
                            iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
                        } catch (TransactionAbortedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (DbException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        pageIterator = iterPage.iterator();
                    }
                    else
                        end = true;
                }
                else {
                	t = pageIterator.next();
                	if(ap.filter(t, indexFieldNum)){
                		returnList.add(t);//t.getRecordId());
        			}
                }
            }
        }
    	return new IndexFileIterator(returnList);
    	/*
    	ArrayList<RecordId> rids = new ArrayList<RecordId>();
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	Iterator<Tuple> iter;
    	HeapPage page = null;
    	RecordId rid;
    	Tuple tuple = null;
    	BufferPool bp = Database.getBufferPool();
    	try {
			rids = search(tid, ap);
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(int i=0; i<rids.size(); i++) {
			rid = rids.get(i);
			try {
				page = (HeapPage) bp.getPage(tid, rid.getPageId(), Permissions.READ_ONLY);
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			iter = page.iterator();
			while(iter.hasNext()) {
				tuple = iter.next();
				if(tuple.getRecordId().tupleno()==rid.tupleno()){
					tuples.add(tuple);
					break;
				}
			}
			
		}
		
		IndexFileIterator it = new IndexFileIterator(tuples);
		
		return it;*/

    }

	//Creating an iterator for going through all the entries stored in this index file
    private class IndexFileIterator implements DbFileIterator{
        
        Iterator<Tuple> tupleItr;
        ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
        
        IndexFileIterator(ArrayList<Tuple> tupleList){
            this.tupleList = tupleList;
        }
    

        @Override
        public void close() {
            tupleItr = null;
        }

        @Override
        public boolean hasNext() throws DbException,
                TransactionAbortedException {
            if(tupleItr!=null){
                return tupleItr.hasNext();
            } 
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            if(tupleItr!=null){
                return tupleItr.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.tupleItr = this.tupleList.iterator();
            
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
        IndexFileIterator indexFileIterator;
        boolean end = false;
        for(int i=0;i<NUM_OF_BUCKETS;i++){
            
            IndexPageId pid = new IndexPageId(this.getId(), i, 0);
            IndexPage iterPage = null;
            try {
                BufferPool buffPool = Database.getBufferPool();
                //if(i<10)
                	//System.out.println("Iter Pid: "+pid);
                iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
            } catch (TransactionAbortedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (DbException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            Iterator<Tuple> pageIterator = iterPage.iterator();
            end = false;
            while(!end || pageIterator.hasNext()){
                if(!pageIterator.hasNext()) {
                    if(iterPage.getOverflowPgnu()>=NUM_OF_BUCKETS) {
                    	//System.out.println("Overflow on Page "+iterPage.getId().pageno()+" Page no: "+iterPage.getOverflowPgnu());
                        pid = new IndexPageId(this.getId(), iterPage.getOverflowPgnu(), 1);
                        BufferPool buffPool = Database.getBufferPool();
                        try {
                            iterPage = (IndexPage) buffPool.getPage(tid, pid, Permissions.READ_ONLY);
                        } catch (TransactionAbortedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (DbException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        pageIterator = iterPage.iterator();
                    }
                    else
                        end = true;
                }
                else {
                    tupleList.add(pageIterator.next());
                }
            }
        }
        
        indexFileIterator = new IndexFileIterator(tupleList);
        return indexFileIterator;
    }

    /**
     *
     * @return the index of keyField for the index file (index file is built on that field on the tuples of the target table)
     */
    public int keyField() {
    	//Some code goes here
    	return indexFieldNum;
    }

    /** Return the number of pages in this file */
    public int numBuckPages() {
    	//Some code goes here
    	return TotNumPages;//(int) ((double)file.length()/(double)BufferPool.DEFAULT_PAGES);
    }

    /**
     * Returns the number of bytes on an IndexPage.
     */
    public int bytesPerBuckPage() {
    	//Some code goes here
    	return -1;
    }

    /**
     * Returns the number of bytes in the header of an IndexPage.
     * Assumes sizeof(int) == 4.
     */
    private int headerBytes() {
    	//Some code goes here
    	int numTuples = (int) Math.floor((BufferPool.PAGE_SIZE*8) / (getTupleDesc().getSize()* 8 + 1));
    	return (int)Math.ceil(numTuples/8.0);
    }
}