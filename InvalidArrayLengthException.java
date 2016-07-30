package simpledb;

public class InvalidArrayLengthException extends Exception {

	/**
	 * Written by Ishwar
	 */
	private static final long serialVersionUID = 1L;

	public InvalidArrayLengthException( String errMsg) {
		System.err.println(errMsg);
		System.exit(-1);
	}
}
