package org.dbanelas;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

public class ArgumentParserUtil {

    public static ArgumentParser createArgumentParser() {
        ArgumentParser parser = ArgumentParsers.newFor("setrastream-parser").build()
                .defaultHelp(true)
                .description("SeTraStream Argument Parser");

        parser.addArgument("-id", "--id")
                .type(String.class)
                .dest("id")
                .required(true)
                .help("The name of the field that holds the id of each tuple");

        parser.addArgument("-ts", "--timestamp")
                .type(String.class)
                .dest("timestamp")
                .required(true)
                .help("The name of the field that holds the timestamp of each tuple");

        parser.addArgument("-ft", "--features")
                .nargs("+")
                .type(String.class)
                .dest("featureColumns")
                .required(true)
                .help("Names of the feature columns to be used in the segmentation process");


        parser.addArgument("-kh", "--kafka-host")
                .type(String.class)
                .dest("kafkaHost")
                .setDefault("localhost")
                .help("Host name of the Kafka broker");

        parser.addArgument("-kp", "--kafka-port")
                .type(Integer.class)
                .dest("kafkaPort")
                .setDefault(9092)
                .help("Port of the Kafka broker");

        parser.addArgument("-it", "--input-topic")
                .type(String.class)
                .dest("inputTopic")
                .setDefault("input")
                .help("Name of the input topic");

        parser.addArgument("-ot", "--output-topic")
                .type(String.class)
                .dest("outputTopic")
                .setDefault("output")
                .help("Name of the output topic");

        parser.addArgument("-bw", "--batch-window")
                .type(Integer.class)
                .dest("batchWindow")
                .setDefault(500)
                .help("Batch window size in milliseconds");

        parser.addArgument("-sw", "--segmentation-window")
                .type(Integer.class)
                .dest("segmentationWindow")
                .setDefault(60000)
                .help("Segmentation window size in milliseconds");

        return parser;
    }

    public static Namespace parseArguments(String[] args) {
        ArgumentParser parser = createArgumentParser();
        return parser.parseArgsOrFail(args);
    }

    public static void validateArguments(Namespace args) {
        return;
    }
}