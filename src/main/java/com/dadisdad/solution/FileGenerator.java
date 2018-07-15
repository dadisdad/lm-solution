package com.dadisdad.solution;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

/**
 * @author dadisdad
 * @date 2018/7/14
 */
public class FileGenerator {


    public static void main(String[] args) throws Exception{
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File("F:/user_score.csv")));
        int i=0;
        while (i<100000000) {
            writer.write(getUUId()+","+getScore());
            writer.newLine();
            i++;
        }
        writer.close();
    }

    public static Integer getScore() {
        Random random = new Random();
        return random.nextInt(1000000);
    }

    public static String getUUId() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}
