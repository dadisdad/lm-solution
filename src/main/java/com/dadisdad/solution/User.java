package com.dadisdad.solution;

/**
 * @author dadisdad
 * @date 2018/7/14
 */
public class User implements Comparable{

    private String custId;
    private Integer score;

    public User() {
    }

    public User(String custId, String score) {
        this.custId = custId;
        this.score = Integer.valueOf(score);
    }

    public String getCustId() {
        return custId;
    }

    public void setCustId(String custId) {
        this.custId = custId;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return custId + "," + score;
    }

    @Override
    public int compareTo(Object o) {
        User user = (User) o;
        if (score>((User) o).getScore()) {
            return 1;
        }else if (score<((User) o).getScore()) {
            return -1;
        }
        return 0;
    }
}
