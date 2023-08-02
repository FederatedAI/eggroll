package com.eggroll.core.utils;

import com.eggroll.core.constant.StringConstants;
import com.google.common.base.Preconditions;

public class FileSystemUtils {

    final static String parentDirRegex = "\\.\\.";

    public static String stripParentDirReference(String path ) {
        Preconditions.checkNotNull(path);
       return  path.replaceAll(parentDirRegex, StringConstants.EMPTY);
    }
//    def fileWriter(fileName: String, content: String): Unit = {
//        val writer = new PrintWriter(new File(fileName))
//        writer.write(content)
//        writer.close()
//    }

//    String  fileReader(String fileName) {
//        Source.fromFile(fileName).mkString;  //using mkString method
//
//    }



}

