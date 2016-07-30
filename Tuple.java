package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 * 
 * 
 * All methods implemented by Ishwar
 */
public class Tuple {

	private Field[] fields;
	private TupleDesc tupleDesc;
	private RecordId recId = null;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if(td.numFields() == 0){
        	try {
				throw new InvalidArrayLengthException("lenght of tuple descriptor canot be zero");
			} catch (InvalidArrayLengthException e) {
				e.printStackTrace();
			}
        }
        this.tupleDesc = td;
        fields = new Field[td.numFields()];
        
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	recId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
    	
    	
    	if(i<tupleDesc.numFields()){
    		fields[i]= f;
    	}
    	
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	if(i < fields.length){
    		return fields[i];
    		
    	}
    	throw new NoSuchElementException();
        
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        
    	StringBuilder ret = new StringBuilder("");
        for(int i=0;i<tupleDesc.numFields();i++){
        	ret = ret.append(fields[i].toString() + " ");
        }
        return ret.append("\n").toString();
    }
}
