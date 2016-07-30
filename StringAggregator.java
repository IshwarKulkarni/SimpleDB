package simpledb;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

	private int gbfield;
	private Type type;
	private Type[] types;
	Hashtable<Field, Integer> AggreMap = new Hashtable<Field, Integer>();
	
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.type = gbfieldtype;
    	if(what.name()!="COUNT")
    		throw new IllegalArgumentException("Can only count with strings");
    	if(gbfield==-1) {
			this.types = new Type[1];
			types[0] = Type.INT_TYPE;
		}
		else {
			this.types = new Type[2];
			types[0] = type;
			types[1] = Type.INT_TYPE;
		}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	Integer count = AggreMap.get(tup.getField(gbfield));
    	if (count == null)
			count = 0;
    	count++;
		AggreMap.put(tup.getField(gbfield), count);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
		if(gbfield==-1)
			return new AggregateIterator(false);
		return new AggregateIterator(true);
	}

	private class AggregateIterator implements DbIterator {

		ArrayList<Tuple> tpls = new ArrayList<Tuple>();
		Iterator<Tuple> tplItr = null;
		TupleDesc td = new TupleDesc(types);

		public AggregateIterator(boolean grouped) {
			Set<Field> fieldList = AggreMap.keySet();
			Iterator<Field> fieldListIter = fieldList.iterator();
			Field f = null;
			Integer he = null;
			while(fieldListIter.hasNext()){
				Tuple tup  = new Tuple(td);
				f = fieldListIter.next();
				he = AggreMap.get(f);
				if(grouped) {
					tup.setField(0, f);
					tup.setField(1, new IntField((int)he.intValue()));
				}
				else
					tup.setField(0, new IntField((int)he.intValue()));
				tpls.add(tup);
			}
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			tplItr = null;
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return td;
		}

		@Override
		public boolean hasNext() throws DbException,
				TransactionAbortedException {
			// TODO Auto-generated method stub
			return tplItr.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			// TODO Auto-generated method stub
			if(tplItr.hasNext())
				return tplItr.next();
			throw new NoSuchElementException();
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			tplItr = tpls.iterator();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

	}

}
