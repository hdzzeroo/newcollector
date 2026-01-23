
from OverView import ERROR_Noise, OKNoise, OverView
from OverView import DEBPrint,DEBAnimaPrint,_DP,OPAexists
from OverView import overViewInit,CSV_FILENAME
import csv
import time
import Sdata


UniList = {"大学": "https://www.hiroshima-u.ac.jp/nyushi/yoko_doga/yoko"}

#----
ov1 = OverView(UniList["大学"],2,"广岛")
ov1.SetOriUrl("https://www.hiroshima-u.ac.jp/")

ov2 = OverView(UniList["大学"],3,"群马大学3")
#----仅调试用

file_path = 'Data/links.csv'        #读取人物列表

#——————————————————————————————————————————————————————————————————————————————————#
"""数据库"""

#——————————————————————————————————————————————————————————————————————————————————#
"""函数功能"""



#——————————————————————————————————————————————————————————————————————————————————#
"""最终的初始化"""
Target = []
with open(file_path, mode='r', encoding='utf-8') as f:
    reader = csv.reader(f)

    header = next(reader)
    
    # 2. 遍历每一行
    for row in reader:
        Target.append([row[0],row[3]])

Target = Target[:]              #调整你的流程
chrome = overViewInit()
_DP.reset()
DEBPrint("开始工作")

#——————————————————————————————————————————————————————————————————————————————————#
"""主程序"""
TPA = time.time()
i = 0
for _page in Target:
    print(_page)
    input()
    DEBPrint(f"任务索引{i}")
    DEBPrint(f"任务{_page[0]}")
    ov = OverView(_page[1],1,f"大学_1_{_page[0]}")
    
    
    ov.start(chrome)
    _path = ov.MemPath + "/" + CSV_FILENAME
    if OPAexists(_path):
        DEBPrint("文件存在，不执行seek")
    else:
        ov.Seek()
        print("文件不存在")

    ov.Pruning()
    #ov.Category()      #AI分类
    ov.end()
    OKNoise()
    i += 1


DEBPrint("所有程序总耗时",(time.time()- TPA)/60)
#——————————————————————————————————————————————————————————————————————————————————#
"""收尾工作"""

DEBPrint("工作顺利结束")
ERROR_Noise()
#chrome.get("https://www.gunma-u.ac.jp/")
