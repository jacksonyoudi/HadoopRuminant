## MapTask并行度决定机制

数据块： block是物理上
数据切片： 逻辑上 对数据切片。

1. 一个job的map阶段并行度是由切片数决定的
2. 每个split切片分配一个MapTask并行处理
3. 默认情况下 切片大小 = blocksize
4. 切片时不考虑数据集整体， 而是逐个针对每个文件单独切片的

