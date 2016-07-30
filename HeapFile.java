package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	File file;
	TupleDesc tupleDesc;
	int numOfPages;
	int fid;
	
	
	//HeapPage pageArray[];
	
	/**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.tupleDesc = td;
        this.file = f;
        this.fid = file.getAbsoluteFile().hashCode();
        this.numOfPages = (int)Math.ceil((double)f.length()/(double)BufferPool.PAGE_SIZE);
        if(f.length()==0) 
//			try {
//				writePage(new HeapPage(new HeapPageId(fid, 0), new byte[BufferPool.PAGE_SIZE]));
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			this.numOfPages = 0;
//        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return fid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) { //TODO add checks for exceeding page limits, etc
        int pageNum = pid.pageno();
        int offSet = (pageNum)*BufferPool.PAGE_SIZE;
        
        long fileSize = file.length();
        //System.out.println("size: " + file.length());
        InputStream iStream = null;
        
        byte[] byteWrite = new byte[BufferPool.PAGE_SIZE];
        try {
			iStream = new FileInputStream(file);
			iStream.skip(offSet);
	    	iStream.read(byteWrite, 0, BufferPool.PAGE_SIZE);
	    	iStream.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

        try {
			HeapPage pageRead = new HeapPage((HeapPageId)pid,byteWrite);
			return (Page)pageRead;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	HeapPage heapPage = (HeapPage)page;
    	byte[] byteArray = heapPage.getPageData();
    	
    	int pageNumber = heapPage.getId().pageno();
    	int offSet = (pageNumber)*BufferPool.PAGE_SIZE;
    	
    	RandomAccessFile oStream = null;
    	
    	oStream = new RandomAccessFile(file,"rw");
    	oStream.skipBytes(offSet);
    	oStream.write(byteArray);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	//numOfPages = (int)(this.fileName.length()/BufferPool.PAGE_SIZE);
        return numOfPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	
    	boolean pageFound = false;
    	
    	HeapPage gotPage = null;
    	
    	for(int i=0;i<numOfPages;i++){
    		HeapPageId pageId = new HeapPageId(this.getId(), i);
    		gotPage = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
    		if(gotPage!=null && gotPage.getNumEmptySlots()>0){
    			pageFound = true;
    			gotPage.addTuple(t);
    			//gotPage.markDirty(true, tid);
    			break;
    		}
    	}
    	
    	if(!pageFound){
    		gotPage = new HeapPage(new HeapPageId(this.getId(),numOfPages),HeapPage.createEmptyPageData(getId()));
    		//gotPage = (HeapPage)buffPool.getPage(tid, new HeapPageId(this.getId(),this.numOfPages), Permissions.READ_WRITE);
    	    int slots = gotPage.getNumEmptySlots();
    		gotPage.addTuple(t);
    		//gotPage.markDirty(true,tid);
    		Database.getBufferPool().forcePage(gotPage, tid);
    		this.numOfPages++;
    	}
    	
        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        modifiedPages.add(gotPage);
        return modifiedPages;

    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        BufferPool buffPool = Database.getBufferPool();
        HeapPage gotPage = (HeapPage)buffPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        gotPage.deleteTuple(t);
        return gotPage;
    }
    
    private class HeapFileIterator implements DbFileIterator{
    	
    	Iterator<Tuple> tupleItr;
    	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
    	
    	HeapFileIterator(ArrayList<Tuple> tupleList){
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
    	HeapFileIterator heapFileIterator;
    	
        for(int i=0;i<this.numPages();i++){
        	
        	HeapPageId pid = new HeapPageId(this.getId(), i);
        	Page iterPage = null;
			try {
				BufferPool buffPool = Database.getBufferPool();
				iterPage = buffPool.getPage(tid, pid, Permissions.READ_ONLY);
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	HeapPage iterHeap = (HeapPage)iterPage;
        	Iterator<Tuple> pageIterator = iterHeap.iterator();
        	while(pageIterator.hasNext()){
        		tupleList.add(pageIterator.next());
        	}
        }
        heapFileIterator = new HeapFileIterator(tupleList);
        return heapFileIterator;
    }

}

