package simpledb;

/**
 * HashFunction supply different hash functions
 */

public class HashFunction {

	/**
	 * NOTE: You may change the parameters or define a new hashing method based on your index structure
	 * NOTE: You also need to define a method for hashing Strings
	 */

    final static int coef = 17;
    final static int offset = 123456;
    final static int prime = 97;
    final static int primeMultiplier = 15485863;

    /**
     * Calculates the linear hash value for the passed value
     * @param value you want to hash
     * @return hash value
     */

    public static int linearhash(int value) {
        //return (coef * value + offset) % 10;
    	return (primeMultiplier*value) % prime;
    }
    
    public static int linearhash(String value){
    	int sum  = 0;
    	for(int i=0; i<value.length(); i++)
    		sum += value.charAt(i);
    	return (primeMultiplier*sum) % prime;
    }
    
    static int getHash(Field f){
        if (f.getType()==Type.INT_TYPE){
                return linearhash(((IntField)f).getValue());
        }
        else{
                return linearhash(((StringField)f).getValue());
        }
    }

}
