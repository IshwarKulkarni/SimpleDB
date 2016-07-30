package simpledb;

public class InvalidArgumentException extends Exception {

	/**
	 * Written by Ishwar
	 */
	private static final long serialVersionUID = 1L;

	InvalidArgumentException(String msg){
		System.err.println(msg);
	}
}
