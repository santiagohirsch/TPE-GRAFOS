package ar.edu.itba.graph.utils;

public class MainUtils {

    public static void validateArguments(String[] args) {
        if (args == null || args.length != 3) {
            System.err.println("Invalid number of arguments.");
            System.err.println("Usage: java -jar <program>.jar <path_to_nodes_csv> <path_to_edges_csv> <k_value>");
            System.exit(1);
        }

        for (String arg : args) {
            if (arg == null || arg.trim().isEmpty()) {
                System.err.println("Arguments must not be null or empty.");
                System.exit(1);
            }
        }

        try {
            Integer.parseInt(args[2]);
            if (Integer.parseInt(args[2]) < 0) {
                System.err.println("The third argument (k value) must be a non-negative integer.");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("The third argument (k value) must be a valid integer.");
            System.exit(1);
        }
    }

}
