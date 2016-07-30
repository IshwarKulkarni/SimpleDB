package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.math.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page. (Note: We do not cover transactions in CS 222 @ UCI )
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private HashMap<Page,Date> pageMap;
    private HashMap<PageId,Page> pageIdMap;
    private HashMap<PageId, Boolean> dirtyPageMap;
    private int numPagesStored = 0;
    int indexPageCounter;
    
    private int numPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	this.numPages = numPages;
    	pageMap = new HashMap<Page,Date>();
    	pageIdMap = new HashMap<PageId,Page>();
    	dirtyPageMap = new HashMap<PageId, Boolean>();
    	this.numPagesStored = 0;
    	this.indexPageCounter = 0;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        if (pid != null) {
    		indexPageCounter++;
    	}
    	
        Page gotPage = pageIdMap.get(pid);
        if(gotPage!=null){
        	return gotPage;
        }
        if(this.numPagesStored>this.numPages){
        	evictPage();
        }
        
        Page pageFromDisk = null;
        DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
        if(dbFile!=null){
        	pageFromDisk = dbFile.readPage(pid);
        	if(pageFromDisk!=null){
        		pageMap.put(pageFromDisk, new Date());
        		pageIdMap.put(pid, pageFromDisk);//pageFromDisk.getId(), pageFromDisk);
        		numPagesStored++;
                gotPage = pageIdMap.get(pid);
        	}
        }
        //Set<PageId> pageList = pageIdMap.keySet();
    	//Iterator<PageId> iter = pageList.iterator();
    	//while(iter.hasNext())
    		//System.out.println(iter.next().toString());
        return pageFromDisk;
    }
    
    public void forcePage(Page newPage,TransactionId tid){
    	if(this.numPagesStored>=this.numPages){
    		try {
				evictPage();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	if(newPage!=null){
    		pageIdMap.put(newPage.getId(), newPage);
    		pageMap.put(newPage, new Date());
    		dirtyPage(newPage, true, tid);
    		numPagesStored++;
    	}
    }
    
    public void dirtyPage(Page page, boolean bool, TransactionId tid) {
    	dirtyPageMap.put(page.getId(), new Boolean(bool));
    	page.markDirty(bool, tid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public  synchronized boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public  synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for Phase 1 (in CS222 @UCI) ). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> dirtyPages = file.addTuple(tid, t);
        Iterator<Page> dirtyPageIterator = dirtyPages.iterator();
        while(dirtyPageIterator.hasNext()){
        	Page dirt = dirtyPageIterator.next();
        	dirtyPage(dirt, true, tid);
        }
        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        RecordId recId = t.getRecordId();
        Page pageToDeleteFrom = pageIdMap.get(recId.getPageId());
        if(pageToDeleteFrom != null){
        	if(pageToDeleteFrom instanceof HeapPage) {
            	((HeapPage) pageToDeleteFrom).deleteTuple(t);
            	pageToDeleteFrom.markDirty(true, tid);
        	}
        	else if(pageToDeleteFrom instanceof IndexPage) {
        		((IndexPage) pageToDeleteFrom).deleteTuple(t);
            	pageToDeleteFrom.markDirty(true, tid);
        	}
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	Set<PageId> dirtyPageList = dirtyPageMap.keySet();
    	PageId pid = null;
    	Object[] pids =  dirtyPageList.toArray();
    	for(int i=0; i<pids.length; i++){
    		pid = (PageId) pids[i];
    		flushPage(pid);
    	}
/*    	while(dirtyPageItr.hasNext()) {
    		pid = dirtyPageItr.next();
    		flushPage(pid);
    	}	
/*    	Set<PageId> allPageList = pageIdMap.keySet();
    	Iterator<PageId> allPageItr = allPageList.iterator();
    	Page page = null;
    	TransactionId tid = new TransactionId();
    	while(allPageItr.hasNext()){
    		PageId pageId = allPageItr.next();
    		try {
				page = getPage(tid, pageId, Permissions.READ_WRITE);
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(page.isDirty()!=null)
				flushPage(pageId);
    	}*/
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	Page flushedPage = pageIdMap.get(pid);
    	if(flushedPage!=null){
    		flushedPage.markDirty(false, null);
    		DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    		dbFile.writePage(flushedPage);
    		pageIdMap.remove(pid);
    		pageMap.remove(flushedPage);
    		dirtyPageMap.remove(pid);
    		this.numPagesStored--;
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for Phase 1 (in CS222 @UCI)
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private  synchronized void evictPage() throws DbException {
    	
    	Iterator<Entry<Page, Date>> pageSetItr = pageMap.entrySet().iterator();
    	Page evPage = null;
    	Date evDate = null;
    	if(pageSetItr.hasNext()){
    		Entry<Page, Date> nextEntry = pageSetItr.next();
    		evPage = nextEntry.getKey();
    		evDate = nextEntry.getValue();
    	}
    	while(pageSetItr.hasNext()){
    		Entry<Page, Date> nextEntry = pageSetItr.next();
    		Date newDate = nextEntry.getValue();
    		Page newPage = nextEntry.getKey();
    		if(newDate.before(evDate)){
    			evDate = newDate;
    			evPage = newPage;
    		}
    	}
    	try {
    		if(evPage!=null){
    			flushPage(evPage.getId());
    		}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }

}
