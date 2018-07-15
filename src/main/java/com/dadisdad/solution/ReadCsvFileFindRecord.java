package com.dadisdad.solution;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by
 *
 * @Author: nicqiang
 * @DATE: 2018/7/15
 */
public class ReadCsvFileFindRecord {

    private static boolean TURE = true ;
    private static String RECORD = "";
    private static int mark = -1;
    private static long recordPointer = -1;
    private static long rank = 0;
    private static String FILE_PATH = "F:/user_score.csv";
    private static final int CHART_SIZE = 43*10000;//读取10万行
    private static final int FIND_RECORD_THREAD_NUM = 5;
    private static final int FIND_RANK_THREAD_NUM = 5;
    private static final int BUFFER_SIZE = 50;

    public static void main(String[] args) throws Exception {
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE_PATH, "r");
        System.out.println("file total :" + randomAccessFile.length());
        RECORD = "9035c3d4-1048-4b42-844d-092f718bcfff";
        int testNum = 10;
        for (int i = 0; i <1 ; i++) {
            System.out.println("\n**********************");
            rank = 0;
            TURE = true;
            findRecordFilePointer();
            TURE = true;
            getRecordRank(recordPointer);
            System.out.println("rank :"+rank);
        }
    }

    /**
     * 获取名次
     * @param recordPointer record pointer
     */
    private static void getRecordRank(long recordPointer) throws Exception {
        long startTime = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE_PATH, "r");
        long fileSzie = randomAccessFile.length();
        long size = fileSzie/FIND_RANK_THREAD_NUM;
        double rate = (double)recordPointer/fileSzie;
        int beforeThreadNum = (int)(FIND_RANK_THREAD_NUM*rate);
        Long[] beginIndexs = new Long[FIND_RANK_THREAD_NUM];
        Long[] endIndexs = new Long[FIND_RANK_THREAD_NUM];
        long last = -1;
        for (int index = 0; index < beforeThreadNum; index++) {
            beginIndexs[index] = last+1;
            last = getNextPointer(randomAccessFile,last+size,recordPointer-1);
            endIndexs[index] = last;
        }
        if(mark == -1){
            throw new Exception("can not find record");
        }
        last = last + RECORD.length()+1+String.valueOf(mark).length();
        for (int index = beforeThreadNum; index <FIND_RANK_THREAD_NUM ; index++) {
            beginIndexs[index] = last+1;
            last = getNextPointer(randomAccessFile,last+size,randomAccessFile.length());
            endIndexs[index] = last;
        }
        randomAccessFile.close();
        ExecutorService beforeService = Executors.newFixedThreadPool(beforeThreadNum);
        ExecutorService afterService = Executors.newFixedThreadPool(FIND_RANK_THREAD_NUM-beforeThreadNum);
        CountDownLatch latch = new CountDownLatch(FIND_RANK_THREAD_NUM);
        for (int i = 0; i < beforeThreadNum; i++) {
            final int index = i;
            beforeService.execute(()->{
                try {
                    getRank(0,beginIndexs[index],endIndexs[index],latch,recordPointer);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
        for (int i = beforeThreadNum; i < FIND_RANK_THREAD_NUM; i++) {
            final int index = i;
            afterService.execute(()->{
                try {
                    getRank(1,beginIndexs[index],endIndexs[index],latch,fileSzie);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
        latch.await();
        beforeService.shutdown();
        afterService.shutdown();
        long endTime = System.currentTimeMillis();
        System.out.println("running time of get rank:"+((double)(endTime-startTime)/1000)+"s");
    }

    /**
     * 获得每个块的排名
     * @param flag 0：befor; 1:befor
     * @param beginIndex begin file pointer
     * @param endIndex end file pointer
     * @param latch {@link CountDownLatch}
     * @param maxIndex max index
     * @throws IOException io exception
     */
    private static void getRank(int flag, Long beginIndex, Long endIndex, CountDownLatch latch, long maxIndex) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE_PATH,"r");
        try {
            randomAccessFile.seek(beginIndex);
            long total = 0;
            while (TURE){
                long nextIndex;
                if(beginIndex+CHART_SIZE<endIndex){
                    nextIndex = getNextPointer(randomAccessFile,beginIndex+CHART_SIZE,maxIndex);
                }else {
                    nextIndex = endIndex;
                }
                randomAccessFile.seek(beginIndex);
                byte[] bufferSize = new byte[(int)(nextIndex-beginIndex)];
                randomAccessFile.read(bufferSize);
                String data = new String(bufferSize);
                String[] lines = data.split(System.lineSeparator());
                //before
                if(flag == 0){
                    for (String line:lines) {
                        if(!"".equals(line) && Integer.valueOf(line.replaceAll("\r|\n", "").substring(line.lastIndexOf(",")+1)) >= mark){
                            total++;
                        }
                    }
                }else {
                    //after
                    for (String line:lines) {
                        if(!"".equals(line.trim()) && Integer.valueOf(line.replaceAll("\r|\n", "").substring(line.lastIndexOf(",")+1)) > mark){
                            total++;
                        }
                    }
                }
                beginIndex = nextIndex+1;
                if(beginIndex>=endIndex){
                    break;
                }
            }
            addRank(total);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            randomAccessFile.close();
            latch.countDown();
        }
    }

    /**
     * 合并结果
     * @param total total
     */
    private synchronized static void addRank(long total) {
        rank +=total;
    }

    /**
     * 查找记录的文件位置指针
     * @throws Exception exception
     */
    private static void findRecordFilePointer() throws Exception {
        long startTime = System.currentTimeMillis();
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE_PATH, "r");
        long fileSzie = randomAccessFile.length();
        long size = fileSzie/FIND_RECORD_THREAD_NUM;
        Long[] beginIndexs = new Long[FIND_RECORD_THREAD_NUM];
        Long[] endIndexs = new Long[FIND_RECORD_THREAD_NUM];
        long last = -1;
        for (int index = 0; index < FIND_RECORD_THREAD_NUM; index++) {
            beginIndexs[index] = last+1;
            last = getNextPointer(randomAccessFile,last+size,fileSzie);
            endIndexs[index] = last;
        }
        ExecutorService service = Executors.newFixedThreadPool(FIND_RECORD_THREAD_NUM);
        CountDownLatch latch = new CountDownLatch(FIND_RECORD_THREAD_NUM);
        for (int i = 0; i < FIND_RECORD_THREAD_NUM; i++) {
            final int index = i;
            service.execute(()->{
                try {
                    readCSVData(beginIndexs[index],endIndexs[index],latch);
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
        }
        randomAccessFile.close();
        latch.await();
        service.shutdownNow();
        long endTime = System.currentTimeMillis();
        System.out.println("running time of find record:"+((double)(endTime-startTime)/1000)+"s");
    }

    /**
     * 获取指定区域的数据
     * @param beginIndex begin file pointer
     * @param endIndex end file pointer
     * @param latch {@link CountDownLatch}
     * @throws IOException io exception
     */
    private static void readCSVData(Long beginIndex, Long endIndex, CountDownLatch latch) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(FILE_PATH,"r");
        long maxIndex = randomAccessFile.length();
        try {
            randomAccessFile.seek(beginIndex);
            while (TURE){
                long nextIndex;
                if(beginIndex+CHART_SIZE<endIndex){
                    nextIndex = getNextPointer(randomAccessFile,beginIndex+CHART_SIZE,maxIndex);
                }else {
                    nextIndex = endIndex;
                }
                randomAccessFile.seek(beginIndex);
                byte[] bufferSize = new byte[(int)(nextIndex-beginIndex)];
                randomAccessFile.read(bufferSize);
                String data = new String(bufferSize);
                String[] lines = data.split(System.lineSeparator());
                long total = 0;
                for (String line:lines) {
                    if(RECORD.equals(line.substring(0,line.lastIndexOf(",")))){
                        //stop all thread
                        TURE = false;
                        recordPointer = beginIndex+total;
                        System.out.println("find record: "+line);
                        mark = Integer.valueOf(line.substring(line.lastIndexOf(",")+1));
                        System.out.println("mark: "+mark);
                        System.out.println("file pointer: "+ recordPointer);
                        long tmp = latch.getCount();
                        while (tmp>0) {
                            latch.countDown();
                            tmp--;
                        }
                        break;
                    }
                    total += line.length()+1;
                }
                beginIndex = nextIndex+1;
                if(beginIndex>=endIndex){
                    break;
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            randomAccessFile.close();
            latch.countDown();
        }
    }

    /**
     * 获得下一行换行符的位置
     * @param randomAccessFile {@link RandomAccessFile}
     * @param index index
     * @param maxIndex max index
     * @return 下一换行符的位置
     * @throws Exception exception
     */
    private static long getNextPointer(RandomAccessFile randomAccessFile, Long index,long maxIndex) throws Exception {
        if(index>maxIndex){
            return maxIndex;
        }
        randomAccessFile.seek(index);
        //获取换行符
        if(index+BUFFER_SIZE < maxIndex){
            byte[] ch = new byte[BUFFER_SIZE];
            randomAccessFile.read(ch);
            StringBuilder sb = new StringBuilder(new String(ch));
            for (int i = 0; i < sb.length(); i++) {
                if("\r".equals(String.valueOf(sb.charAt(i)))){
                    return index+i+1;
                }
            }
        }else {
            return maxIndex;
        }
        throw new Exception("cannot find separator");
    }
}
