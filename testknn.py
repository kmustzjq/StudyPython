# -*- coding: utf-8 -*-
"""
Created on Mon Feb 25 15:26:17 2019

@author: Administrator
"""

#!/usr/bin/python
# coding=utf-8
import knn
from numpy import *
# 生成数据集和类别标签
dataSet, labels = knn.createDataSet()
# 定义一个未知类别的数据
testX = array([1.2, 1.0])
k = 3
# 调用分类函数对未知数据分类
outputLabel = knn.kNNClassify(testX, dataSet, labels, 3)
print ("Your input is:", testX, "and classified to class: ", outputLabel)

testX = array([0.1, 0.3])
outputLabel = knn.kNNClassify(testX, dataSet, labels, 3)
print ("Your input is:", testX, "and classified to class: ", outputLabel)