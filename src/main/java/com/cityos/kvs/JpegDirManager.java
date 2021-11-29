package com.cityos.kvs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class JpegDirManager {
    private static final Logger log = LoggerFactory.getLogger(JpegDirManager.class);

    private final String dirName;

    public JpegDirManager(String dirName) {
        this.dirName = dirName;
    }

    public List<String> getSortedDescFilenameList() {
        File fileDir = new File(dirName);
        File[] fileArray = fileDir.listFiles();
        if (fileArray == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(fileArray)
                .map(File::getName)
                .filter(fileName -> {
                    String ext = fileName.substring(fileName.lastIndexOf(".") + 1);
                    return "jpg".equals(ext) || "jpeg".equals(ext);
                })
                .sorted(Comparator.reverseOrder())
                .collect(toList());
    }

    public Path getLatestFilePath() {
        if (getSortedDescFilenameList().isEmpty()) {
            return null;
        }
        return Paths.get(dirName + getSortedDescFilenameList().get(0));
    }

    public void cleanDirOlderThan(String keepFileName) {
        File fileDir = new File(dirName);
        File[] files = fileDir.listFiles();
        if (files != null) {
            for (File f : files) {
                if(f.getPath().compareTo(keepFileName) >= 0) {
                    continue;
                }
                if (!f.delete()) {
                    log.warn("failed to delete file: " + f.getAbsolutePath());
                }
            }
        }
    }
}
