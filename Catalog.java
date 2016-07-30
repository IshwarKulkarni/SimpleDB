package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import com.sun.xml.internal.bind.v2.runtime.Name;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * 
 * 
 */

public class Catalog {

    /**
     * 
     * 
     * Constructor.
     * Creates a new, empty catalog.
     */
	Hashtable < String, DbFile>NameToFile= new Hashtable<String, DbFile>(); 
	Hashtable < Integer, DbFile>IDToFile= new Hashtable<Integer, DbFile>();
	Hashtable< DbFile, index >IxFiletoIndex = new Hashtable< DbFile, index >(); 
	
	private class index{ 	// table name and field of index are necessary and sufficient to id an index
		String tableName;
		int keyField;
		index(String s, int kf){
			this.tableName = s;
			this.keyField = kf;
		}
	}
    public Catalog() {
        
    }

    
    public void addIndex(DbFile ixFile, String ixName, String tableName, int keyFieldNum) {
    	NameToFile.put(ixName, ixFile);
        IDToFile.put(ixFile.getId(), ixFile);
        IxFiletoIndex.put(ixFile, new index(tableName, keyFieldNum));
        ((IndexFile)ixFile).setKeyField(keyFieldNum);
        ((IndexFile)ixFile).setTupleDesc(getTupleDesc(getTableId(tableName)).getType(keyFieldNum));
        ((IndexFile)ixFile).setHeapId(getTableId(tableName));
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added	 as the table for a given name.
     */
    public void addTable(DbFile file, String name) {
    	if(name == null || file == null){
    		try {
				throw new InvalidArgumentException("Name of table/ dbFile cannot be null");
			} catch (InvalidArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
        NameToFile.put(name, file);
        IDToFile.put(file.getId(), file);
        
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     */
    /*public void addTable(DbFile file) {
        addTable(file, "");
    }*/

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException{
        // some code goes here
    	if(name==null){
	    	throw new NoSuchElementException( " null keyed searches are invalid");	
    	}
    	DbFile dbf = NameToFile.get(name);
    	if(dbf == null){
    		throw new NoSuchElementException( "search with key " + name + " retruned no results");
    	}
        
    	return dbf.getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
    	DbFile dbf = IDToFile.get(new Integer(tableid));
    	if(dbf == null ){
    		throw new NoSuchElementException();
    	}
    	return dbf.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
    	DbFile dbf = IDToFile.get(new Integer(tableid));
    	if(dbf == null ){
    		throw new NoSuchElementException();
    	}
    	return dbf;
    }

    /** Delete all tables from the catalog */
    public void clear() {
        NameToFile.clear();
        IDToFile.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(name + ".dat"), t);
                addTable(tabHf,name);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
    
    
    
}
