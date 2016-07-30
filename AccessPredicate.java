package simpledb;


/**
 * AccessPredicate compares a field which has index on it against a given value
 */
public class AccessPredicate {



	Predicate.Op op;
	Field fvalue;
    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public AccessPredicate(Predicate.Op op, Field fvalue) {
        //Some code goes here
    	this.op = op;
    	this.fvalue = fvalue;
    }

    public Field getField() {
    	//Some code goes here
        return fvalue;

    }

    public Predicate.Op getOp() {
    	//Some code goes here
        return op;
    }

    /**
     * Compares the field at field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific
     * in the constructor.  The comparison can be made through Field's
     * compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t, int fNum) {
    	//Some code goes here
    	Field comp = t.getField(fNum);
        return comp.compare(op, fvalue);
    }
}
