package simpledb;
import java.io.InvalidClassException;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 * all methods implemented by Ishwar
 * 
 */
public class TupleDesc {

	private ArrayList<Type> FieldTypeList = new ArrayList<Type>();
	private ArrayList<String> FieldNameList = new ArrayList<String>();
	private String anonymousFieldMarker = "";
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    	Type[] typeAr = new Type[td1.numFields() +  td2.numFields() ];
    	String[] fieldAr = new String[td1.numFields() +  td2.numFields() ];
    	int i = 0;
    	for(i=0;i<td1.numFields() ;i++){
    		typeAr[i] = td1.getType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	
    	for(int j=0;j<td2.numFields() ;j++){
    		typeAr[i + j] = td2.getType(j);
    		fieldAr[i + j] = td2.getFieldName(j);
    	}
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if(typeAr.length == 0){
        	try {
				throw new InvalidArrayLengthException("Length of field name array/type array is invalid");
			} catch (InvalidArrayLengthException e) {
		
				e.printStackTrace();
			}
        }
        
        
        for(int i = 0;i<typeAr.length;i++){
        	FieldTypeList.add(i,typeAr[i]);
        	if(fieldAr[i] ==null){
        		System.err.println("Feild name cannot be null ");
        		throw new NullPointerException();
        	}
        	
        	if(i >=fieldAr.length){
        		FieldNameList.add(i, anonymousFieldMarker);
        	}
        	else{
        		FieldNameList.add(i, fieldAr[i]);
        	}
        		
        }
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	if(typeAr.length == 0 ){
        	try {
				throw new InvalidArrayLengthException("Length of field type array cannot be zero");
			} catch (InvalidArrayLengthException e) {
		
				e.printStackTrace();
			}
        }
        
        
        for(int i = 0;i<typeAr.length;i++){
        	FieldTypeList.add(i,typeAr[i]);
        	FieldNameList.add(i, anonymousFieldMarker);
        }
    }
    
    

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return FieldTypeList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        
    	if(i>= numFields() ){
    		throw new NoSuchElementException(Integer.toString(i));
    	}
    	if( FieldNameList.get(i) == anonymousFieldMarker){
    		return "<<Anonymous>>";
    	}
        return FieldNameList.get(i) ;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
    	if (name == null){
    		throw new NoSuchElementException("cannot have null element"); 
    		
    	}
        for(int i=0;i<FieldNameList.size();i++){
        	if(FieldNameList.get(i).compareTo(name)==0){
        		return i;
        	}
        }
        
        throw new NoSuchElementException(name);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
    	if(i>= numFields() ){
    		throw new NoSuchElementException(Integer.toString(i));
    	}
    	
        return FieldTypeList.get(i) ;    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int tot=0;
        for(int i=0;i<numFields();i++ ){
        	tot+=FieldTypeList.get(i).getLen();
        }
        return tot;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if((o==null ) ||  !(o instanceof TupleDesc)){
    		return false;
    	}
    	TupleDesc td  = (TupleDesc) o;
    	if(td.numFields() != numFields()){
    		return false;
    	}
    	for(int i = 0;i< FieldTypeList.size();i++){
    		if(td.getType(i)!= getType(i)){
    			return false;
    		}
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuilder ret = new StringBuilder("");
    	for(int i=0;i<numFields();i++){
    		if( FieldTypeList.get(i) == Type.INT_TYPE){
    			ret = ret.append("Integer (");
    		}
    		else ret.append("String (");
    		if(i<FieldNameList.size()){
    			ret = ret.append(FieldNameList.get(i)+ ") ");
    		}
    		else{
    			ret = ret.append("<<anonymous>>");
    		}
    	}
    	
    	return ret.toString();
    }
    
    
    int getNumNames(){
    	return FieldNameList.size();
    }
   
 }


