package simpledb;

public class InvalidArgumentTypeException extends Exception {
	/**
	 * Written by Ishwar
	 */
	private static final long serialVersionUID = 1L;

	public InvalidArgumentTypeException(String ErrMsg	) {
	 System.err.println(ErrMsg);
	 
	}

}
