package com.dadisdad.solution;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dadisdad
 * @date 2018/7/15
 */
public class RankSearch {

    private String custId;

    private Integer score;

    private long position;

    private List<StartEnd> startEnds;

    private File file;

    private RandomAccessFile raf;

    private long fileLen;

    private AtomicInteger rank;

    private static final int CHART_SIZE = 43 * 10000;
    private static final int FIND_RECORD_THREAD_NUM = 5;
    private static final int FIND_RANK_THREAD_NUM = 5;
    private static final int BUFFER_SIZE = 50;
    private static boolean POS_FLAG = true;
    private static boolean RANK_FLAG = true;

    public RankSearch(File file, String custId) throws FileNotFoundException {
        this.file = file;
        this.fileLen = file.length();
        this.custId = custId;
        rank = new AtomicInteger();
        raf = new RandomAccessFile(file, "r");
    }

    /**
     * 获取每个分割块的开始和结束位置
     *
     * @param nums
     * @throws Exception
     */
    private void getStartEnds(int nums) throws Exception {
        List<StartEnd> result = new ArrayList<>(nums);
        long everySize = fileLen / nums;
        caculate(result, 0, everySize, fileLen);
        this.startEnds = result;
    }

    private void caculate(List<StartEnd> result, long start, long size, long length) throws Exception {
        if (start > length) {
            return;
        }
        long startPos = start;
        long endPos;
        if (start + size > length) {
            endPos = length;
        } else {
            endPos = start + size + 1;
        }
        raf.seek(endPos);
        int tmp;
        while ((tmp = raf.read()) != -1) {
            if (tmp == '\n') {
                break;
            } else {
                endPos++;
            }
        }
        result.add(new StartEnd(startPos, endPos));
        caculate(result, endPos + 1, size, length);
    }

    /**
     * 获得下一行换行符的位置
     *
     * @param randomAccessFile {@link RandomAccessFile}
     * @param index            index
     * @param maxIndex         max index
     * @return 下一换行符的位置
     * @throws Exception exception
     */
    private long getNextPointer(RandomAccessFile randomAccessFile, Long index, long maxIndex) throws Exception {
        if (index > maxIndex) {
            return maxIndex;
        }
        randomAccessFile.seek(index);
        //获取换行符
        if (index + BUFFER_SIZE < maxIndex) {
            byte[] ch = new byte[BUFFER_SIZE];
            randomAccessFile.read(ch);
            for (int i = 0; i < ch.length; i++) {
                if (ch[i] == '\r') {
                    return index + i + 1;
                }
            }
        } else {
            return maxIndex;
        }
        throw new Exception("cannot find separator");
    }

    public void searchPosition() throws Exception {
        long startTime = System.currentTimeMillis();
        ExecutorService executors = Executors.newFixedThreadPool(FIND_RECORD_THREAD_NUM);
        getStartEnds(FIND_RECORD_THREAD_NUM);
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < FIND_RECORD_THREAD_NUM; i++) {
            executors.execute(new SearchTask(startEnds.get(i), latch));
        }
        latch.await();
        executors.shutdownNow();
        long endTime = System.currentTimeMillis();
        System.out.println("running time of find record:" + ((double) (endTime - startTime) / 1000) + "s");
    }

    class SearchTask implements Runnable {

        private StartEnd startEnd;
        private CountDownLatch latch;

        public SearchTask(StartEnd startEnd, CountDownLatch latch) {
            this.startEnd = startEnd;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(startEnd.start);
                long beginIndex = startEnd.start;
                while (POS_FLAG) {
                    long nextIndex;
                    if (beginIndex + CHART_SIZE < startEnd.end) {
                        nextIndex = getNextPointer(randomAccessFile, beginIndex + CHART_SIZE, fileLen);
                    } else {
                        nextIndex = startEnd.end;
                    }
                    randomAccessFile.seek(beginIndex);
                    byte[] bufferSize = new byte[(int) (nextIndex - beginIndex)];
                    randomAccessFile.read(bufferSize);
                    String data = new String(bufferSize);
                    String[] lines = data.split(System.lineSeparator());
                    long total = 0;
                    for (String line : lines) {
                        if (custId.equals(line.substring(0, line.lastIndexOf(",")))) {
                            position = beginIndex + total;
                            score = Integer.valueOf(line.substring(line.lastIndexOf(",") + 1));
                            System.out.println("find record: " + line);
                            System.out.println("file pointer: " + position);
                            long tmp = latch.getCount();
                            while (tmp > 0) {
                                latch.countDown();
                                tmp--;
                            }
                            POS_FLAG = false;
                            break;
                        }
                        total += line.length() + 2;
                    }
                    beginIndex = nextIndex + 1;
                    if (beginIndex >= startEnd.end) {
                        break;
                    }
                }
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    public long getPosition() {
        return position;
    }

    public void searchRank() throws Exception {
        long startTime = System.currentTimeMillis();

        double rate = (double) position / fileLen;
        int beforeThreadNum = (int) ((FIND_RANK_THREAD_NUM * rate)) == 0 ? 1 : (int) (FIND_RANK_THREAD_NUM * rate);
        int afterThreadNum = FIND_RANK_THREAD_NUM - beforeThreadNum;
        List<StartEnd> before = new ArrayList<>(beforeThreadNum);
        List<StartEnd> ends = new ArrayList<>(afterThreadNum);
        long custLen = custId.length() + String.valueOf(score).length() + 3;
        caculate(before, 0, (position - 1) / beforeThreadNum, position);
        caculate(ends, position + custLen, (fileLen - custLen - position) / afterThreadNum, fileLen);

        ExecutorService beforeService = Executors.newFixedThreadPool(beforeThreadNum);
        ExecutorService afterService = Executors.newFixedThreadPool(FIND_RANK_THREAD_NUM - beforeThreadNum);
        CountDownLatch latch = new CountDownLatch(FIND_RANK_THREAD_NUM);
        for (int i = 0; i < beforeThreadNum; i++) {
            beforeService.execute(new SearchRankTask(0, before.get(i), position, latch));
        }
        for (int i = 0; i < afterThreadNum; i++) {
            afterService.execute(new SearchRankTask(1, ends.get(i), fileLen, latch));
        }
        latch.await();
        beforeService.shutdown();
        afterService.shutdown();
        long endTime = System.currentTimeMillis();
        System.out.println("running time of get rank:" + ((double) (endTime - startTime) / 1000) + "s");

    }

    class SearchRankTask implements Runnable {

        private StartEnd startEnd;
        private CountDownLatch latch;
        private int flag;
        private long size;

        public SearchRankTask(int flag, StartEnd startEnd, long size, CountDownLatch latch) {
            this.flag = flag;
            this.size = size;
            this.startEnd = startEnd;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(startEnd.start);
                long total = 0;
                long beginIndex = startEnd.start;
                while (RANK_FLAG) {
                    long nextIndex;
                    if (beginIndex + CHART_SIZE < startEnd.end) {
                        nextIndex = getNextPointer(randomAccessFile, beginIndex + CHART_SIZE, size);
                    } else {
                        nextIndex = startEnd.end;
                    }
                    randomAccessFile.seek(beginIndex);
                    byte[] bufferSize = new byte[(int) (nextIndex - beginIndex)];
                    randomAccessFile.read(bufferSize);
                    String data = new String(bufferSize);
                    String[] lines = data.split(System.lineSeparator());
                    //before
                    if (flag == 0) {
                        for (String line : lines) {
                            if (!"".equals(line) && Integer.valueOf(line.replaceAll("\r|\n", "").substring(line.lastIndexOf(",") + 1)) >= score) {
                                rank.incrementAndGet();
                            }
                        }
                    } else {
                        //after
                        for (String line : lines) {
                            if (!"".equals(line.trim()) && Integer.valueOf(line.replaceAll("\r|\n", "").substring(line.lastIndexOf(",") + 1)) > score) {
                                rank.incrementAndGet();
                            }
                        }
                    }
                    beginIndex = nextIndex + 1;
                    if (beginIndex >= startEnd.end) {
                        break;
                    }
                }
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }
    }

    public int getRank() {
        return rank.get();
    }

    public static void main(String[] args) throws Exception {
        File file = new File("F:/user_score.csv");
        RankSearch searcher = new RankSearch(file, "65c9d8ed-6430-410d-9777-28d95d406ade");
        searcher.searchPosition();
        searcher.searchRank();
        System.out.println(searcher.getRank());
    }
}
