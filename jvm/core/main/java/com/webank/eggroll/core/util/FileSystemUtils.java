//package com.webank.eggroll.core.util;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//
//public class FileSystemUtils {
//    private static final String parentDirRegex = "\\..";
//
//    public static String stripParentDirReference(String path) {
//        if (path == null) {
//            throw new NullPointerException("Path cannot be null");
//        }
//        return path.replaceAll(parentDirRegex, "");
//    }
//
//    public static void fileWriter(String fileName, String content) {
//        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
//            writer.write(content);
//        } catch (IOException e) {
//            // Handle IOException
//        }
//    }
//
//    public static String fileReader(String fileName) {
//        try {
//            byte[] bytes = Files.readAllBytes(Paths.get(fileName));
//            return new String(bytes, StandardCharsets.UTF_8);
//        } catch (IOException e) {
//            // Handle IOException
//            return null;
//        }
//    }
//}