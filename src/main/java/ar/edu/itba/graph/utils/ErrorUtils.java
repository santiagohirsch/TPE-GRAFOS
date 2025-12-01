package ar.edu.itba.graph.utils;

public class ErrorUtils {
    public static String INVALID_NUMBER_OF_ARGUMENTS = "Invalid number of arguments.";
    public static String USAGE_MESSAGE = "Usage: java -jar <program>.jar <path_to_nodes_csv> <path_to_edges_csv> <k_value>";
    public static String ARGUMENTS_MUST_NOT_BE_NULL_OR_EMPTY = "Arguments must not be null or empty.";
    public static String K_VALUE_MUST_BE_NON_NEGATIVE_INTEGER = "The third argument (k value) must be a non-negative integer.";
    public static String K_VALUE_MUST_BE_VALID_INTEGER = "The third argument (k value) must be a valid integer.";
    public static String UNDERLYING_STRUCTURE_NOT_VALID = "The underlying structure is not valid, it is a multigraph.";
    public static String PATH_MUST_NOT_BE_NULL = "Path must not be null.";
    public static String PATH_DOES_NOT_EXIST = "Path does not exist: ";
    public static String PATH_IS_NOT_A_FILE = "Path is not a file: ";
}
