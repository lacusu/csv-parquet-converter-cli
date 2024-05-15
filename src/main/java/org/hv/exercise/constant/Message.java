package org.hv.exercise.constant;

public class Message {
    public final static String CMD_SYNTAX_LINE = "-i <input file path> -o <output file path>";
    public final static String OUTPUT_SUCCEED_MSG = "Conversion successful! \nOutput file: %s%n";
    public final static String FILE_EXTENSION_REQUIRED_MSG = "File extension (.csv|.parquet) is expected";
    public final static String SHOW_HELP_DESC = "Show help";
    public final static String INPUT_FILE_PATH_DESC = String.format("%s. %s", "Input File path", FILE_EXTENSION_REQUIRED_MSG);
    public final static String OUTPUT_FILE_PATH_DESC = String.format("%s. %s", "Out File path", FILE_EXTENSION_REQUIRED_MSG);
    public final static String CONVERTING_MSG = "Converting [%s] file to [%s]...%n";
    public final static String UNSUPPORTED_CONVERSION_MSG = "Unsupported conversion: [%s] to [%s]";
    public final static String INVALID_FILE_PATH_MSG = "Invalid file path: [%s]" + ". " + FILE_EXTENSION_REQUIRED_MSG;
    public final static String TIME_TAKEN_MSG = "Time taken: %s seconds";
        public final static String MISSING_REQUIRED_OPTIONS = "Please double check. Missing option: -o, -i.";

}
