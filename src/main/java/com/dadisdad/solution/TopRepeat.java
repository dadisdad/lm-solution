package com.dadisdad.solution;

import org.omg.PortableInterceptor.INACTIVE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * @author dadisdad
 * @date 2018/7/16
 */
public class TopRepeat {

    private static int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
        return o2.getValue().compareTo(o1.getValue());
    }

    public User getUser(String line) {
        return new User(line.substring(0, line.lastIndexOf(",")), line.substring(line.lastIndexOf(",") + 1));
    }

    public String[] getTopScore() {
        long startTime = System.currentTimeMillis();
        HashMap<Integer, Integer> map = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("F:/user_score.csv")), 1024 * 1024);
            String line = null;
            User user;
            while ((line = reader.readLine()) != null) {
                user = getUser(line);
                map.put(user.getScore(), map.getOrDefault(user.getScore(), 0) + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("running time of get rank:" + ((double) (endTime - startTime) / 1000) + "s");
        List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(map.entrySet());

        //升序排序
        Collections.sort(list, TopRepeat::compare);
        for (int i = 0; i < 10; i++) {
            System.out.println(list.get(i).getKey()+":"+list.get(i).getValue());

        }
        return null;

    }

    public static void main(String[] args) {
        TopRepeat repeat = new TopRepeat();
        repeat.getTopScore();
    }
}
