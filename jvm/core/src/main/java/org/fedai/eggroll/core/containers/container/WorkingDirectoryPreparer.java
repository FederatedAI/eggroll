package org.fedai.eggroll.core.containers.container;


import lombok.Data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class WorkingDirectoryPreparer {
    private Map<String, byte[]> files = new HashMap<>();
    private Map<String, byte[]> zippedFiles = new HashMap<>();
    ;
    private boolean need_cleanup = true;
    private Path workingDir = null;

    private Path getWorkingDir() throws Exception {
        if (workingDir != null) {
            return workingDir.toAbsolutePath();
        } else {
            throw new Exception("workingDir is not set");
        }
    }


    private Path getModelsDir() throws Exception {
        return getWorkingDir().resolve("models");
    }

    private Path getLogsDir() throws Exception {
        return getWorkingDir().resolve("logs");
    }

    public Map<String, String> getContainerDirEnv() throws Exception {
        String workingDirString = getWorkingDir().toString();
        String modelsDirString = getModelsDir().toString();
        String logsDirString = getLogsDir().toString();

        Map<String, String> envMap = new HashMap<>();
        envMap.put("EGGROLL_CONTAINER_DIR", workingDirString);
        envMap.put("EGGROLL_CONTAINER_MODELS_DIR", modelsDirString);
        envMap.put("EGGROLL_CONTAINER_LOGS_DIR", logsDirString);

        return envMap;
    }

    public void prepare() {
        try {
            Files.createDirectories(getWorkingDir());
            Files.createDirectories(getModelsDir());
            Files.createDirectories(getLogsDir());

            for (Map.Entry<String, byte[]> entry : files.entrySet()) {
                String fileName = entry.getKey();
                byte[] content = entry.getValue();

                Path filePath = getWorkingDir().resolve(fileName);
                File file = filePath.toFile();

                FileOutputStream fos = new FileOutputStream(file);
                fos.write(content);
                fos.close();
            }

            for (Map.Entry<String, byte[]> entry : zippedFiles.entrySet()) {
                String dirname = entry.getKey();
                byte[] zipBytes = entry.getValue();
                ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zipBytes));
                byte[] buffer = new byte[1024];

                ZipEntry zipEntry;
                while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                    Path outputPath = getWorkingDir().resolve(dirname).resolve(zipEntry.getName());

                    if (!zipEntry.isDirectory()) {
                        File outputFile = outputPath.toFile();
                        outputFile.getParentFile().mkdirs();

                        FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                        int bytesRead;
                        while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                            fileOutputStream.write(buffer, 0, bytesRead);
                        }
                        fileOutputStream.close();
                    }

                    zipInputStream.closeEntry();
                }

                zipInputStream.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        files.clear();
        zippedFiles.clear();
    }


    public void cleanup() throws Exception {
        if (need_cleanup && workingDir != null) {
            File workingDirFile = workingDir.toFile();
            workingDirFile.delete();
        }
    }

    public Map<String, byte[]> getFiles() {
        return files;
    }

    public void setFiles(Map<String, byte[]> files) {
        this.files = files;
    }

    public Map<String, byte[]> getZippedFiles() {
        return zippedFiles;
    }

    public void setZippedFiles(Map<String, byte[]> zippedFiles) {
        this.zippedFiles = zippedFiles;
    }

    public boolean isNeed_cleanup() {
        return need_cleanup;
    }

    public void setNeed_cleanup(boolean need_cleanup) {
        this.need_cleanup = need_cleanup;
    }

    public void setWorkingDir(Path workingDir) {
        this.workingDir = workingDir;
    }
}
