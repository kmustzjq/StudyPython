
# coding: utf-8

# In[ ]:


#使用class创建一个School类
class School:
    def __init__(self,name,age):
        self.name=name
        self.age=age

    def student(self):
        print("name:%s,age:%s"%(self.name,self.age))
    def classroom(self):
        print("%s去教室"%self.name)

