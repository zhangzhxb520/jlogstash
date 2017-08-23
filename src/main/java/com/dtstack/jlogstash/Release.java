package com.dtstack.jlogstash;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zxb
 * @version 1.0.0
 * 2017年08月17日 15:20
 * @since 1.0.0
 */
public class Release {

    private static final String BASE_DIR = "D:\\git_zhangzhxb\\dtstack\\jlogstash-parent";
    private static final String FILTER_DIR = BASE_DIR + "\\jlogstash-filter-plugin";
    private static final String INPUT_DIR = BASE_DIR + "\\jlogstash-input-plugin";
    private static final String OUTPUT_DIR = BASE_DIR + "\\jlogstash-output-plugin";
    private static final String FILTER_OUTPUT = BASE_DIR + "\\jlogstash-core\\plugin\\filter";
    private static final String INPUT_OUTPUT = BASE_DIR + "\\jlogstash-core\\plugin\\input";
    private static final String OUTPUT_OUTPUT = BASE_DIR + "\\jlogstash-core\\plugin\\output";

    public static void main(String[] args) throws Exception {
        cleanDirectory(FILTER_OUTPUT);
        cleanDirectory(INPUT_OUTPUT);
        cleanDirectory(OUTPUT_OUTPUT);

        List<File> filterFiles = new ArrayList<File>();
        findJarFiles(new File(FILTER_DIR), filterFiles, 0, 4);
        moveFiles(filterFiles, FILTER_OUTPUT);

        List<File> inputFiles = new ArrayList<File>();
        findJarFiles(new File(INPUT_DIR), inputFiles, 0, 4);
        moveFiles(inputFiles, INPUT_OUTPUT);

        List<File> outputFiles = new ArrayList<File>();
        findJarFiles(new File(OUTPUT_DIR), outputFiles, 0, 4);
        moveFiles(outputFiles, OUTPUT_OUTPUT);

        File jlogstashFile = new File("D:\\git_zhangzhxb\\dtstack\\jlogstash-parent\\jlogstash-core\\target\\jlogstash-1.0.0-with-dependencies.jar");
        String outputPath = "D:\\git_zhangzhxb\\dtstack\\jlogstash-parent\\jlogstash-core\\lib";
        moveFiles(Arrays.asList(jlogstashFile), outputPath);
    }

    private static void cleanDirectory(String output) {
        File outputFile = new File(output);
        if (outputFile.isDirectory()) {
            File[] fileArr = outputFile.listFiles();
            for (File file : fileArr) {
                file.delete();
            }
        }
    }

    private static void moveFiles(List<File> filterFiles, String output) {
        for (File file : filterFiles) {
            File outputFile = new File(output + "\\" + file.getName());
            copyFile(file, outputFile);
        }
    }

    private static void copyFile(File inputFile, File outputFile) {
        BufferedInputStream fis = null;
        BufferedOutputStream fos = null;
        try {
            fis = new BufferedInputStream(new FileInputStream(inputFile));
            fos = new BufferedOutputStream(new FileOutputStream(outputFile));

            byte[] buffer = new byte[1024];
            while (fis.read(buffer) != -1) {
                fos.write(buffer, 0, buffer.length);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void findJarFiles(File file, List<File> fileList, int level, int maxLevel) {
        level++;
        if (level > maxLevel) {
            return;
        }

        if (file.getName().startsWith(".")) {
            return;
        }

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File subFile : files) {
                findJarFiles(subFile, fileList, level, maxLevel);
            }
        } else {
            if (file.getName().endsWith("with-dependencies.jar")) {
                fileList.add(file);
            }
        }
    }
}
