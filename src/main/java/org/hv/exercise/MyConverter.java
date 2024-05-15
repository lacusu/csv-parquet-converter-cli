package org.hv.exercise;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.hv.exercise.converter.FileConverterContext;

import static org.hv.exercise.constant.Message.CMD_SYNTAX_LINE;
import static org.hv.exercise.constant.Message.INPUT_FILE_PATH_DESC;
import static org.hv.exercise.constant.Message.MISSING_REQUIRED_OPTIONS;
import static org.hv.exercise.constant.Message.OUTPUT_FILE_PATH_DESC;
import static org.hv.exercise.constant.Message.OUTPUT_SUCCEED_MSG;
import static org.hv.exercise.constant.Message.SHOW_HELP_DESC;
import static org.hv.exercise.constant.Message.TIME_TAKEN_MSG;

public class MyConverter {
    private static Options options;


    public static void main(String[] args) {
        // Record the start time
        long startTime = System.nanoTime();
//        args = new String[]{"-i ./src/main/resources/data", "-o ./src/main/resources/output3.parquet"};
        initCmdOptions();

        // Create the Options object

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            // Parse the command line arguments
            CommandLine cmd = parser.parse(options, args);

            // Check for help option
            if (cmd.hasOption("h")) {
                formatter.printHelp(CMD_SYNTAX_LINE, options);
                System.exit(0);
            }

            if (cmd.getOptions().length < 2) {
                formatter.printHelp(MISSING_REQUIRED_OPTIONS, options);
                System.exit(1);
            }
            String inputFileValue = cmd.getOptionValue("input", "").strip();
            String outputFileValue = cmd.getOptionValue("output", "").strip();

            FileConverterContext converterContext = new FileConverterContext();
            boolean succeed = converterContext.convertFile(inputFileValue, outputFileValue);
            if (succeed) {
                System.out.printf(OUTPUT_SUCCEED_MSG, outputFileValue);
                // Calculate and print the execution time
                long elapsedTime = System.nanoTime() - startTime;
                double seconds = (double) elapsedTime / 1_000_000_000.0;
                System.out.printf(TIME_TAKEN_MSG + "%n", seconds);
                System.exit(0);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp(CMD_SYNTAX_LINE, options);

            System.exit(1);
        }
    }


    public static void initCmdOptions() {
        options = new Options();

        // Add option "h" for help
        Option help = Option.builder()
                .option("h")
                .longOpt("help")
                .hasArg(false)
                .desc(SHOW_HELP_DESC)
                .build();
        options.addOption(help);

        // Add option "i" for input file
        Option inputOption = Option.builder()
                .option("i")
                .longOpt("input")
                .hasArg(true)
                .desc(INPUT_FILE_PATH_DESC)
//                .required()
                .build();
        options.addOption(inputOption);

        // Add option "o" for out file
        Option outOption = Option.builder()
                .option("o")
                .longOpt("output")
                .hasArg(true)
                .desc(OUTPUT_FILE_PATH_DESC)
//                .required()
                .build();
        options.addOption(outOption);

    }
}