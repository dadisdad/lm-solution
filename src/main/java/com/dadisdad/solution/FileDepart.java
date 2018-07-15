package com.dadisdad.solution;

import java.io.*;

/**
 * @author dadisdad
 * @date 2018/7/14
 */
public class FileDepart {

    private static final int TOTAL_LINS = 100000000;

    private int parts;

    private File file;

    private String dst;

    public FileDepart(File file, int parts, String dst) {
        this.file = file;
        this.parts = parts;
        this.dst = dst;
    }

    public void depart() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        for (int i = 0; i < parts; i++) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(dst + "user_score_" + i + ".csv")));
            for (int j = 0; j < TOTAL_LINS / parts; j++) {
                writer.write(reader.readLine());
                writer.newLine();
            }
            writer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        FileDepart depart = new FileDepart(new File("F:/user_score.csv"), 100, "F:/temp/");
        depart.depart();
    }

}
