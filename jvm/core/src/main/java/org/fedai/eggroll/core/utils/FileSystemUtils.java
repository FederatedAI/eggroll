package org.fedai.eggroll.core.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.fedai.eggroll.core.constant.StringConstants;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class FileSystemUtils {

    final static String parentDirRegex = "\\.\\.";

    public static String stripParentDirReference(String path) {
        Preconditions.checkNotNull(path);
        return path.replaceAll(parentDirRegex, StringConstants.EMPTY);
    }

    public static void fileWriter(String fileName, String content) throws IOException {
        FileUtils.write(new File(fileName), content, Charset.defaultCharset());
    }

    public static String fileReader(String fileName) throws IOException {
        return FileUtils.readFileToString(new File(fileName), Charset.defaultCharset());
    }

}

