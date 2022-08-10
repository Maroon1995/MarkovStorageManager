# MarkovStorageManager
马尔可夫模型在物料库存管理中的应用
![img.png](img.png)
# 项目环境及其框架
1.1 项目环境

环境：jdk-8 + hadoop-2.7.2 + hbase-2.0.0 + kafka_2.12-0.11.0.0 + mysql-5.6.24 + 
redis-3.0.4 + maxwell-1.25.0 
SparkCore + SparkStreaming

语言：scala

开启：一次开启 zookeeper、kafka、hadoop、hbase、maxwell、redis


1.2 框架
![img_1.png](img_1.png)

# 程序执行顺序
1.1 先开启数据监控程序

（1）数据流处理：`DataDealStreamAPP`；

（2）数据分流到不同的`kafka`主题中，分流：`ReadDBMaxwellStreamAPP`

1.2 马尔可夫模型实现程序

（1）先对数据进行状态计算： 
    执行完成`CaculateQuantityStatusAPP`后依次执行`StatisticSingleStatusCountAPP`、`StatisticDoubleStatusCountAPP`
（2）获取马尔可夫状态矩阵：
    执行完成`CaculateStatusMatrixAPP`



