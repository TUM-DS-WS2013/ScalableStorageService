package common.parsers;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class responsible for command line arguments parsing
 *
 * @author Danila Klimenko
 */
public class ArgumentParser {
    private final Map<String, Boolean>  options;
    private final String[]              arguments;
    private final int                   count;
    private int                         offset;

    /**
     * Main constructor
     *
     * @param format String describing acceptable options (a simplified version of POSIX "getopt()" format)
     * @param args Array of command line arguments
     * @throws ParseException Thrown if format has inconsistent syntax
     */
    public ArgumentParser(String format, String[] args) throws ParseException {
        this.options = new HashMap<String, Boolean>();
        this.arguments = args;
        this.count = args.length;
        this.offset = 0;

        this.parseFormat(format);
    }

    /**
     * Parses the format string and generates a map of valid options
     *
     * @throws ParseException Thrown if format has inconsistent syntax
     */
    private void parseFormat(String format) throws ParseException {
        Pattern syntax = Pattern.compile("([a-zA-Z0-9][:]?)*");

        if (!syntax.matcher(format).matches()) {
            throw new ParseException("Illegal symbols in format string.", 0);
        }

        int i = 0;
        while (i < format.length()) {
            String opt = format.substring(i, i + 1);
            Boolean has_argument = false;
            
            if (this.options.containsKey(opt)) {
                throw new ParseException("Duplicate options in format string.", 0);
            }
            
            if ((++i < format.length()) && (format.charAt(i) == ':')) {
                has_argument = true;
                ++i;
            }
            
            this.options.put(opt, has_argument);
        }
    }

    /**
     * Reenterable function which parses command line arguments and return next valid option.
     *
     * @return An `ArgumentParser.Option` instance containing the option and its argument. Either name or argument of an
     * option may be null (parameterless option and positional argument, respectively).
     * @throws ParseException Thrown if an invalid option is encountered or if an option misses an argument
     */
    public Option getNextArgument() throws ParseException {
        if (this.offset >= count) {
            return null;
        }

        String opt = this.arguments[this.offset];
        String opt_name;
        String opt_argument;

        Pattern syntax = Pattern.compile("\\-([a-zA-Z0-9])([\\S]*)");
        Matcher syntax_matcher = syntax.matcher(opt);

        if (syntax_matcher.matches()) {
            opt_name = syntax_matcher.group(1);
            opt_argument = syntax_matcher.group(2);
            if (opt_argument.length() == 0) {
                opt_argument = null;
            }
        } else {
            opt_name = null;
            opt_argument = opt;
        }

        if (opt_name != null) {
            if (!this.options.containsKey(opt_name)) {
                throw new ParseException("Option '" + opt_name + "' is not supported.", 0);
            }
            if (this.options.get(opt_name) && (opt_argument == null)) {
                if (++this.offset >= count) {
                    throw new ParseException("Option '" + opt_name + "' must have an argument.", 0);
                }
                opt_argument = this.arguments[this.offset];
            }
        }

        ++this.offset;

        return new Option(opt_name, opt_argument);
    }

    /**
     * Restarts argument parsing from the first one
     */
    public void reset() {
        this.offset = 0;
    }

    /**
     * A simple subclass for returning the option and its argument
     */
    public static class Option {
        public final String name;
        public final String argument;

        private Option(String name, String argument) {
            this.name = name;
            this.argument = argument;
        }
    }
}
