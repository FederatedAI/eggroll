package com.eggroll.core.utils;

import org.apache.commons.cli.*;

public class CommandArgsUtils {

    public static CommandLine parseArgs( String[] args) {
        HelpFormatter formatter = new HelpFormatter();
        Options options = new Options();
        Option config = Option.builder("c")
                .argName("configuration file")
                .longOpt("config")
                .hasArg().numberOfArgs(1)
//      .required
                .desc("configuration file")
                .build();

        Option help = Option.builder("h")
                .argName("help")
                .longOpt("help")
                .desc("print this message")
                .build();

        Option sessionId = Option.builder("s")
                .argName("session id")
                .longOpt("session-id")
                .hasArg().numberOfArgs(1)
//      .required
                .desc("session id")
                .build();

        Option port = Option.builder("p")
                .argName("port to bind")
                .longOpt("port")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("port to bind")
                .build();

        Option transferPort = Option.builder("tp")
                .argName("transfer port to bind")
                .longOpt("transfer-port")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("transfer port to bind")
                .build();

        Option clusterManager = Option.builder("cm")
                .argName("cluster manager of this service")
                .longOpt("cluster-manager")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("cluster manager of this service")
                .build();

        Option nodeManager = Option.builder("nm")
                .argName("node manager of this service")
                .longOpt("node-manager")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("node manager of this service")
                .build();

        Option serverNodeId = Option.builder("sn")
                .argName("server node id")
                .longOpt("server-node-id")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("server node of this service")
                .build();

        Option processorId = Option.builder("prid")
                .argName("processor id")
                .longOpt("processor-id")
                .optionalArg(true)
                .hasArg().numberOfArgs(1)
                .desc("processor id of this service")
                .build();

        options
                .addOption(config)
                .addOption(help)
                .addOption(sessionId)
                .addOption(port)
                .addOption(transferPort)
                .addOption(clusterManager)
                .addOption(nodeManager)
                .addOption(serverNodeId)
                .addOption(processorId);

        DefaultParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                formatter.printHelp("", options, true);
                return null;
            }
        } catch (ParseException e) {

                e.printStackTrace();
                formatter.printHelp("", options, true);
        }

        return cmd;
    }

}
