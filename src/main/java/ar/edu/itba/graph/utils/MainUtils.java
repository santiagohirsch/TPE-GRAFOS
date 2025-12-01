package ar.edu.itba.graph.utils;
import static ar.edu.itba.graph.utils.ErrorUtils.*;
public class MainUtils {

    public static void validateArguments(String[] args) {
        if (args == null || args.length != 3) {
            throwError(INVALID_NUMBER_OF_ARGUMENTS, USAGE_MESSAGE);
        }

        for (String arg : args) {
            if (arg == null || arg.trim().isEmpty()) {
                throwError(ARGUMENTS_MUST_NOT_BE_NULL_OR_EMPTY);
            }
        }

        try {
            Integer.parseInt(args[2]);
            if (Integer.parseInt(args[2]) < 0) {
                throwError(K_VALUE_MUST_BE_NON_NEGATIVE_INTEGER);
            }
        } catch (NumberFormatException e) {
            throwError(K_VALUE_MUST_BE_VALID_INTEGER);
        }
    }

}
