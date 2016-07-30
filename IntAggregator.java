package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	private int gbfield;
	private int afield;
	private Type type;
	private Op op;
	private Type[] types;
	Hashtable<Field, HashEntry> AggreMap = new Hashtable<Field, HashEntry>();

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.type = gbfieldtype;
		this.afield = afield;
		this.op = what;
		if(gbfield==-1) {
			this.types = new Type[1];
			types[0] = Type.INT_TYPE;
			HashEntry he = new HashEntry();
			he.totalEntries = 0;
			he.result = 0;
			AggreMap.put(new IntField(0), he);
		}
		else {
			this.types = new Type[2];
			types[0] = type;
			types[1] = Type.INT_TYPE;
		}
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		HashEntry he;
		if(gbfield!=-1)
			he = AggreMap.get(tup.getField(gbfield));
		else
			he = AggreMap.get(new IntField(0));
		if (op.name() == "MIN") {
			if (he == null) {
				he = new HashEntry();
				he.totalEntries = 0;
				he.result = Double.POSITIVE_INFINITY;
			}
			he.totalEntries++; // no semantic meaning for minimum, only for
			// consistency
			int temp = ((IntField) tup.getField(afield)).getValue(); // min is
			// defined only for int-field
			if (temp < he.result)
				he.result = temp;
		}
		if (op.name() == "MAX") {
			if (he == null) {
				he = new HashEntry();
				he.totalEntries = 0;
				he.result = Double.NEGATIVE_INFINITY;
			}
			he.totalEntries++; // again, no semantic meaning
			int temp = ((IntField) tup.getField(afield)).getValue();
			if (temp > he.result)
				he.result = temp;
		}
		if (op.name() == "COUNT") {
			if (he == null) {
				he = new HashEntry();
				he.totalEntries = 0;
				he.result = 0;
			}
			he.totalEntries++;
			he.result++;
		}
		if (op.name() == "SUM") {
			if (he == null) {
				he = new HashEntry();
				he.totalEntries = 0;
				he.result = 0;
			}
			he.totalEntries++;
			he.result += ((IntField) tup.getField(afield)).getValue();
		}
		if (op.name() == "AVG") {
			if (he == null) {
				he = new HashEntry();
				he.totalEntries = 0;
				he.result = 0;
			}
			int temp = he.totalEntries;
			double total = he.result;
			double sum = temp * total;
			he.totalEntries++;
			he.result = (sum + ((IntField) tup.getField(afield)).getValue())
					/ he.totalEntries;
		}
		if(gbfield!=-1)
			AggreMap.put(tup.getField(gbfield), he);
		else
			AggreMap.put(new IntField(0), he);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
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
			HashEntry he = null;
			while(fieldListIter.hasNext()){
				Tuple tup  = new Tuple(td);
				f = fieldListIter.next();
				he = AggreMap.get(f);
				if(grouped) {
					tup.setField(0, f);
					tup.setField(1, new IntField((int)he.result));
				}
				else
					tup.setField(0, new IntField((int)he.result));
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

	private class HashEntry {
		int totalEntries;
		IntField aggreField;
		double result;
	}

}
