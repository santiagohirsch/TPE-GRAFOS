package ar.edu.itba.graph.utils;
import static ar.edu.itba.graph.utils.ErrorUtils.*;
public class MainUtils {

    public static void validateArguments(String[] args) {
        if (args == null || args.length != 3) {
            System.err.println(INVALID_NUMBER_OF_ARGUMENTS);
            System.err.println(USAGE_MESSAGE);
            System.exit(1);
        }

        for (String arg : args) {
            if (arg == null || arg.trim().isEmpty()) {
                System.err.println(ARGUMENTS_MUST_NOT_BE_NULL_OR_EMPTY);
                System.exit(1);
            }
        }

        try {
            Integer.parseInt(args[2]);
            if (Integer.parseInt(args[2]) < 0) {
                System.err.println(K_VALUE_MUST_BE_NON_NEGATIVE_INTEGER);
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println(K_VALUE_MUST_BE_VALID_INTEGER);
            System.exit(1);
        }
    }

}
