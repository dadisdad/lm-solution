用户最大规模为1亿，用户拥有信用评分（信用评分为非负整数，且小于一百万），初始评分不固定而且全部存储再一个乱序的大文件中，
每行内容格式是：用户名,信用评分。
排名规则：信用评分相同，则名次按用户在文件中出现的先后顺序排序
限定内存512M
1、给定用户的信用评分名次
2、Top 100 的信用评分用户
3、信用评分重复频率最高的10个信用评分