package common.parsers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Danila Klimenko
 */
public class CommandParser {
    private final BufferedReader            reader;
    private final PrintStream               output;
    private final String                    prompt;
    private final Map<String, MetaCommand>  commands;
    private final MetaCommand               help_command;
    
    public CommandParser(InputStream is, PrintStream ps, String prompt) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.output = ps;
        this.prompt = prompt;
        this.commands = new LinkedHashMap<String, MetaCommand>();
        
        MetaCommand help_comm = null;
        try {
            help_comm = new MetaCommand("help [command]", "Print help text.");
        } catch (ParseException ex) {}
        this.help_command = help_comm;
        this.commands.put(this.help_command.name, this.help_command);
    }
    
    public void addCommand(String format, String help_text) throws ParseException {
        MetaCommand meta_command = new MetaCommand(format, help_text);
        
        if (this.commands.containsKey(meta_command.name)) {
            throw new ParseException("Duplicate command name: \"" + meta_command.name + "\".", 0);
        }
        
        this.commands.put(meta_command.name, meta_command);
    }
    
    public Command nextCommand() throws IOException {
        Command command = null;
        
        while (command == null) {
            this.output.print(this.prompt + " ");
            String[]    input = this.reader.readLine().trim().split("\\s+", 2);
            
            String      command_name = input[0];
            String      arguments = (input.length > 1) ? input[1] : "";
            
            if (command_name.length() == 0) {
                continue;
            }
            
            MetaCommand meta_command = this.commands.get(command_name);
            
            if (meta_command == null) {
                this.output.println("Error! Command \"" + command_name + "\" is not supported. "
                        + "Type \"help\" to get list of acceptable commands.");
                continue;
            }
            
            try {
                command = meta_command.commandWithArguments(arguments);
                
            } catch (ParseException ex) {
                this.output.println("Error for command \"" + meta_command.name + "\": " + ex.getMessage());
                command = null;
            }
            
            if ((command != null) && (meta_command == this.help_command)) {
                this.handleHelpCommand(command);
                command = null;
            }
        }
        
        return command;
    }
    
    private void handleHelpCommand(Command command) {
        String command_name = command.arguments.get("command");
        
        if (command_name != null) {
            MetaCommand meta_command = this.commands.get(command_name);
            if (meta_command == null) {
                this.output.println("Error! Command \"" + command_name + "\" is not supported. "
                        + "Type \"help\" to get list of acceptable commands.");
            } else {
                this.output.println(meta_command.signature + " - " + meta_command.help_text);
            }
            
        } else {
            int max_signature_length = 0;
            
            for (MetaCommand meta_command: this.commands.values()) {
                if (max_signature_length < meta_command.signature.length()) {
                    max_signature_length = meta_command.signature.length();
                }
            }
            
            for (MetaCommand meta_command: this.commands.values()) {
                StringBuilder  padding = new StringBuilder();
                for (int i = 0; i < (max_signature_length - meta_command.signature.length()); ++i) {
                    padding.append(' ');
                }
                this.output.println(meta_command.signature + padding.toString() + " - " + meta_command.help_text);
            }
        }
    }
    
    private static class MetaCommand {
        final String                name;
        final String                help_text;
        final List<MetaArgument>    arguments;
        final String                signature;
        
        MetaCommand(String format, String help_text) throws ParseException {
            String[]    tokens = format.trim().split("\\s+");
            
            if (!tokens[0].matches("[_a-zA-Z0-9]+")) {
                throw new ParseException("Illegal command name: \"" + tokens[0] + "\".", 0);
            }
            
            this.name = tokens[0];
            this.help_text = help_text;
            
            this.arguments = new ArrayList<MetaArgument>();
            boolean has_optional = false;
            boolean has_greedy = false;
            for (int i = 1; i < tokens.length; ++i) {
                if (has_greedy) {
                    throw new ParseException("Command \"" + this.name +
                            "\": Only the last argument can be a greedy one.", 0);
                }
                
                MetaArgument    meta_arg = new MetaArgument(tokens[i]);
                
                if (has_optional && !meta_arg.optional) {
                    throw new ParseException("Command \"" + this.name +
                            "\": Optional arguments can not precede mandatory ones.", 0);
                }
                
                this.arguments.add(meta_arg);
                
                has_optional = meta_arg.optional;
                has_greedy = meta_arg.greedy;
            }
            
            StringBuilder signature_builder = new StringBuilder(this.name);
            for (MetaArgument meta_arg: this.arguments) {
                signature_builder.append(" ").append(meta_arg.signature);
            }
            this.signature = signature_builder.toString();
        }
        
        private Command commandWithArguments(String arguments_string) throws ParseException {
            Map<String, String> command_args = new HashMap<String, String>(this.arguments.size());
            
            int         i = 0;
            String[]    tokens = arguments_string.split("\\s+", this.arguments.size());
            
            for (MetaArgument meta_arg: this.arguments) {
                if (i >= tokens.length) {
                    if (!meta_arg.optional) {
                        throw new ParseException("Argument \"" + meta_arg.name + "\" must be set.", 0);
                    }
                    break;
                }
                
                if ((i == this.arguments.size() - 1) && (tokens[i].split("\\s+").length > 1) && !meta_arg.greedy) {
                    throw new ParseException("Too many arguments.", 0);
                }
                
                if (meta_arg.max_length != 0 && (tokens[i].length() > meta_arg.max_length)) {
                    throw new ParseException("Argument \"" + meta_arg.name + "\" is too long.", 0);
                }
                
                if (tokens[i].length() > 0) {
                    command_args.put(meta_arg.name, tokens[i]);
                }
                ++i;
            }
            
            return new Command(this.name, command_args);
        }
        
        private static class MetaArgument {
            final String    name;
            final boolean   optional;
            final boolean   greedy;
            final int       max_length;
            final String    signature;
            
            MetaArgument(String format) throws ParseException {
                Matcher mnd_com_matcher = Pattern.compile("\\<([_a-zA-Z0-9]+)(\\.\\.\\.)?(:\\d+)?\\>").matcher(format);
                Matcher opt_com_matcher = Pattern.compile("\\[([_a-zA-Z0-9]+)(\\.\\.\\.)?(:\\d+)?\\]").matcher(format);
                Matcher matcher;
                
                if (mnd_com_matcher.matches()) {
                    this.optional = false;
                    matcher = mnd_com_matcher;
                } else if (opt_com_matcher.matches()) {
                    this.optional = true;
                    matcher = opt_com_matcher;
                } else {
                    throw new ParseException("Illegal argument format: \"" + format + "\".", 0);
                }
                
                this.name = matcher.group(1);
                this.greedy = (matcher.group(2) != null) && matcher.group(2).equals("...");
                if (matcher.group(3) != null) {
                    this.max_length = Integer.parseInt(matcher.group(3).substring(1));
                } else if ((matcher.group(2) != null) && matcher.group(2).startsWith(":")) {
                    this.max_length = Integer.parseInt(matcher.group(2).substring(1));
                } else {
                    this.max_length = 0;
                }
                
                StringBuilder signature_builder = new StringBuilder();
                signature_builder.append(this.optional ? "[" : "<");
                signature_builder.append(this.name);
                signature_builder.append(this.optional ? "]" : ">");
                this.signature = signature_builder.toString();
            }
        }
    }
    
    public static class Command {
        public final String                 name;
        public final Map<String, String>    arguments;
        
        private Command(String name, Map<String, String> arguments) {
            this.name = name;
            this.arguments = arguments;
        }
    }
}
