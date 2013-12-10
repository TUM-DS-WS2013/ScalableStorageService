package logger;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Represents the initialization for the server logging with Log4J.
 */
public class LogSetup {

    private static final Logger logger = Logger.getRootLogger();
    private static boolean      initialized = false;
    private static String       log_path;

    /**
     * Private constructor restricts instantiation of the class
     */
    private LogSetup() {}

    /**
     * Initializes the "rootLogger" for further use in other classes
     * @param logdir Path to log file
     * @param level Initial logging level
     * @throws IOException Thrown if log file cannot be opened.
     */
    public static void initialize(String logdir, Level level) throws IOException {
        if (initialized) {
            return;
        }
        
        log_path = logdir;
        
        // Make sure the directories exist
        File file = new File(log_path);
        file.getParentFile().mkdirs();
        
        PatternLayout layout = new PatternLayout("%d{ISO8601} %-5p [%t] %c: %m%n");
        FileAppender fileAppender = new FileAppender(layout, logdir, true);

        ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        logger.addAppender(consoleAppender);
        logger.addAppender(fileAppender);
        logger.setLevel(level);
        
        initialized = true;
    }
    
    /**
     * Returns the logger object which is directly used to log information
     * @return The logger object
     */
    public static Logger getLogger() {
        return logger;
    }

    /**
     * Changes logging level
     * @param levelString New logging level name as a string.
     * @return Info string
     */
    public static String setLogLevel(String levelString) {
        String  report;
        
        if (isValidLevel(levelString)) {
            logger.setLevel(Level.toLevel(levelString));
            report = "Logging level changed to \"" + logger.getLevel().toString() + "\".";
        } else {
            report = "Error! Logging level \"" + levelString + "\" is not valid.";
        }

        return report;
    }
    
    /**
     * Verifies whether the provided string is a valid name of a logging level
     * @param levelString Level name as a string
     * @return True if levelString is a valid logging level
     */
    public static boolean isValidLevel(String levelString) {
        boolean valid = false;

        if (levelString.equalsIgnoreCase(Level.ALL.toString()) ||
                levelString.equalsIgnoreCase(Level.DEBUG.toString()) ||
                levelString.equalsIgnoreCase(Level.INFO.toString()) ||
                levelString.equalsIgnoreCase(Level.WARN.toString()) ||
                levelString.equalsIgnoreCase(Level.ERROR.toString()) ||
                levelString.equalsIgnoreCase(Level.FATAL.toString()) ||
                levelString.equalsIgnoreCase(Level.OFF.toString())) {
            valid = true;
        }

        return valid;
    }
}
