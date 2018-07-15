package com.dadisdad.solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.PriorityQueue;

/**
 * @author dadisdad
 * @date 2018/7/16
 */
public class TopScore {

    class TopKPrioritorQueue {

        private PriorityQueue<User> queue;
        private int K;

        public TopKPrioritorQueue(int maxSize) {
            this.K = maxSize;
            this.queue = new PriorityQueue(maxSize);
        }

        public void add(User user) {
            if (queue.size() < K) {
                queue.add(user);
            } else {
                User peek = queue.peek();
                if (peek.compareTo(user) < 0) {
                    queue.poll();
                    queue.add(user);
                }
            }
        }

        public User[] getUser() {
            User[] users =new User[K];
            queue.toArray(users);
            return users;
        }
    }

    public User getUser(String line) {
        return new User(line.substring(0,line.lastIndexOf(",")), line.substring(line.lastIndexOf(",") + 1));
    }

    public void getTop() {
        long startTime = System.currentTimeMillis();
        TopKPrioritorQueue queue = new TopKPrioritorQueue(100);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("F:/user_score.csv")), 1024*1024);
            String line = null;
            while ((line = reader.readLine()) != null) {
                queue.add(getUser(line));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("running time of get rank:" + ((double) (endTime - startTime) / 1000) + "s");
        System.out.println(Arrays.deepToString(queue.getUser()));
    }

    public static void main(String[] args) {
        TopScore top = new TopScore();
        top.getTop();
    }
}
