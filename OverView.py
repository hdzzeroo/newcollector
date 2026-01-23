
#from Support import *
#类设计的核心思路

from selenium import webdriver                              # selenium的对象
from selenium.webdriver.chrome.options import Options       # selenium的核心
from selenium.webdriver.chrome.service import Service       # 解决无限加载问题
from webdriver_manager.chrome import ChromeDriverManager    # 解决无限加载问题
from selenium.webdriver.support.ui import WebDriverWait     # 检测超时问题
from selenium.common.exceptions import TimeoutException     # 超时报错
from selenium.common.exceptions import WebDriverException   # 浏览器驱动报错
from selenium.webdriver.common.by import By                 # 人际验证用

from collections import namedtuple              # 轻量化结构体
from os.path import exists as OPAexists         # 检查某文件是否存在
from os.path import isdir as OSisdir            # 检查某路径文件夹是否存在
from os import system as Osm, times             # 清空控制台
from os import makedirs   as OSmakedirs         # 创建文件夹
from sys import stdout                          # DEBUG库用的
from sys import exit as sysExit                 # 退出程序
from time import sleep as Tsleep                # 休眠一会

from json_repair import loads                               #json修复
from collections import defaultdict                         # 父子节点
from urllib.parse import urljoin, urlparse, urlunparse      # 处理url
from bs4 import BeautifulSoup                               # 解析页面信息用
import markdownify                                          # MD文件用

import math                                                         #数学运算
import struct                                                       #结构
import io                                                           #输入输出
import wave                                                         #波形
import platform                                                     #平台检测
import csv
import json
from openai import OpenAI
import re
import time

import Sdata
__all__ = ['OverView', 'OverView_FloorMode','DebugPrinter']

#
HEURISTIC_KEYWORDS	= Sdata.HEURISTIC_KEYWORDS
CORE_KEYWORDS		= Sdata.CORE_KEYWORDS
BLACKLIST			= Sdata.BLACKLIST

DATA_BASE			= Sdata.DATA_BASE
OUTPUT_MD			= Sdata.OUTPUT_MD    
OUTPUT_FOLDER       = Sdata.OUTPUT_FOLDER
CSV_FILENAME        = Sdata.CSV_FILENAME
HTML_FILENAME       = Sdata.HTML_FILENAME
HTMLED_FILENAME       = Sdata.HTMLED_FILENAME

FILE_EXTENSIONS     = Sdata.FILE_EXTENSIONS
CSVCLEANED_FILENAME = Sdata.CSVCLEANED_FILENAME

BasicDepth    =  -1
PACK_MAX_SIZE       = Sdata.PACK_MAX_SIZE
blacklist_regex = "|".join(map(re.escape, BLACKLIST))           #正则表达式的
heuristic_regex = "|".join(map(re.escape, HEURISTIC_KEYWORDS))  #正则表达式的

EXPORT_PDF      = "pdfCollect.csv"

#定义节点结构体
NodeStruct = namedtuple("NodeStruct", ["Index", "url", "Depth","FatherIndex"])  

#豆包
Doubao_32 = OpenAI(
    # 此为默认路径，您可根据业务所在地域进行配置
    base_url="https://ark.cn-beijing.volces.com/api/v3",
    # 从环境变量中获取您的 API Key
    api_key= Sdata.Dou_Bao_Key,
)
#豆包2
Doubao_256 = OpenAI(
    # 此为默认路径，您可根据业务所在地域进行配置
    base_url="https://ark.cn-beijing.volces.com/api/v3",
    # 从环境变量中获取您的 API Key
    api_key= Sdata.Dou_Bao_Key,
)
#获得chrome的对象并初始化
def overViewInit():
    create_Folder(OUTPUT_FOLDER)
    create_Folder(DATA_BASE)

    #1.初始化Chrome
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--headless=new')  # 新版 headless 模式更稳定
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-background-networking')
    chrome_options.add_argument('--disable-sync')
    chrome_options.add_argument('--disable-translate')
    chrome_options.add_argument('--no-first-run')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.page_load_strategy = 'eager'
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    # 强制浏览器不弹出下载窗口，且禁止 PDF 自动下载
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "download.default_directory": "/dev/null",
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": False,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # 强制 20秒 超时     ---     解决某些页面无限加载/刷新的问题
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_page_load_timeout(25) 
    return driver



class OverView():
    def __init__(self,targetUrl:str,depth:int,sign:str = "大学数据"):
        #1.基础参数
        
        self.starturl    = targetUrl
        self.oriUrl      = targetUrl
        self._MAX_DEPTH  = depth        #检索深度（重要）
        self.isStart     = False
        self.chrome      = None
        
        
        self.BaseSign    = sign
        #2.复杂参数
        self.Uqueue      =  []      #队列
        self.URL_LAB     =  {}      #只用来存现有的url种类 ，每个url映射一个INDEX
        self.URL_RLAB    =  {}      #每个Index映射一个url
        self.visitedUrls   = set() #浏览过的任务
        
        self._MAX_DEPTH     = self._MAX_DEPTH if self._MAX_DEPTH < 11 else 10
        self.MemPath     = OUTPUT_FOLDER + "/" + self.BaseSign
    def SetOriUrl(self,url):
        self.oriUrl      =  url
    # [功能]插入队列节点
    def AddNode(self,url:str,fatherIndex:int,fatherDepth:int,title:str,Breadcrumb:str,message:str):
        #添加进双向映射表BreadcrumbList
        if url not in self.URL_LAB:
            _nowSize = len(self.URL_LAB.keys())
            self.URL_LAB[url] = _nowSize
            self.URL_RLAB[str(_nowSize)] = [url,_nowSize,fatherIndex,fatherDepth+1,title,Breadcrumb,message]   
            #现在链接 自己的Index 父亲Index  深度 ,面包屑路径 100字摘要
       
        #添加这个节点进入队列里
        self.Uqueue.append(NodeStruct(int(self.URL_LAB[url]),url,fatherDepth+1,fatherIndex))
        
    # [功能]初始化,启动前请运行他
    def start(self,chrome:webdriver.Chrome):
        # 初始化列表信息
        self.AddNode(self.starturl,BasicDepth,BasicDepth,"根节点","开始点","无")
        create_Folder(self.MemPath)
        # 启动浏览器
        self.chrome = chrome
        self.is_start = True
        try:
            chrome.get(self.starturl )
        except:
            print(f"启动异常")
            
    # [功能]运行结束后手动关闭chrome，释放内容
    def end(self):
        
        self.URL_LAB.clear()
        self.URL_RLAB.clear()
        self.URL_LAB  = []
        self.URL_RLAB = []
        self.visitedUrls.clear()
        self.visitedUrls   = set() 
        if self.chrome:
            try:
                # 1. 清除当前站点的 Cookies 和 LocalStorage (防止状态污染)
                self.chrome.delete_all_cookies()
                
                # 2. 导航到空白页 (about:blank) 
                # 这是最关键的一步：这会强制浏览器卸载当前页面的 DOM、JS 引擎和插件内存
                self.chrome.get("about:blank")
                # 3. 如果你打开了多个窗口，只保留当前这一个，关闭其他的
                handles = self.chrome.window_handles
                while len(handles) > 1:
                    self.chrome.switch_to.window(handles[-1])
                    self.chrome.close()
                    handles = self.chrome.window_handles
                self.chrome.switch_to.window(handles[0])

            except Exception as e:
                print(f"[-] 清理过程异常: {e}")
        
        # 返回初始 URL，方便下次直接再次调用
        return self.starturl
    
    # [功能]广度优先方案
    def Seek(self):
        TPA = time.time()
        OKNoise()

        while self.Uqueue:
            Node = self.Uqueue.pop(0)
            seeIndex = Node.Index           #正在浏览的网址的Index - 用于生成子集节点的父Index
            _Url     = Node.url             #当前要浏览的url
            DepthNow =Node.Depth            #正在浏览的深度        - 用于给子节点定义深度
            #Node.FatherIndex               #应该暂时没什么用
            
            #参观过的不加
            if _Url in self.visitedUrls:
                continue

            #虽然深度大不处理但是也加
            if DepthNow > self._MAX_DEPTH  or (any(_Url.lower().endswith(ext) for ext in FILE_EXTENSIONS)):
                print(f"-完成: {len(self.visitedUrls)} | -剩余： {len(self.Uqueue)+1} | -深度: {DepthNow}")
                self.visitedUrls.add(_Url) 
                # 1. 判断后缀是否为 .pdf
                if _Url.lower().endswith('.pdf'):

                    # 2. 以 a+ 模式打开 CSV 文件
                    # newline='' 是为了防止在某些操作系统（如 Windows）中出现多余的空行
                    with open(EXPORT_PDF, mode='a+', encoding='utf-8', newline='') as f:
                        writer = csv.writer(f)
        
                        # 3. 写入一行数据
                        # 注意：writerow 接收一个列表，列表中的每个元素对应 CSV 的一列
                        writer.writerow([_Url])
        
                    print(f"已将 '{_Url}' 写入 {EXPORT_PDF}")
                else:
                    pass
                continue
            try:
                self.chrome.get(_Url)
                # 等待 JS 渲染的补充：用显式等待代替盲目的 time.sleep
                try:
                    WebDriverWait(self.chrome, 20).until(
                        lambda d: d.execute_script('return document.readyState') == 'complete'
                    )
                except TimeoutException:
                    # 如果 8 秒内没加载完，强制停止加载，直接解析现有 DOM
                    self.chrome.execute_script("window.stop();")
                    print(f"渲染超时(20s)，已截断并解析部分内容: {_Url}")

                # r如果有人机验证 - 跳过该任务
                if not CookieGo(self.chrome):
                    self.visitedUrls.add(_Url)
                    continue 

                # 解析页面
                soup = BeautifulSoup(self.chrome.page_source, 'html.parser')
                self.visitedUrls.add(_Url) # 只有成功获取到内容才标记为已访问

            except TimeoutException:
                print(f"访问超时，跳过该页: {_Url}")
                try:
                    self.chrome.execute_script("window.stop();") # 尝试强制停止页面加载
                except:
                    pass
                self.visitedUrls.add(_Url) # 标记为已访问，防止在队列里反复尝试
                continue
            except WebDriverException as e:
                print(f"驱动异常: {e}")
                self.visitedUrls.add(_Url)
                continue
            except Exception as e:
                print(f"❌未知错误: {e}")
                self.visitedUrls.add(_Url)
                continue    
    

            """这一步是保存对应的md文件"""
            if True:
                # 解析
                soup = BeautifulSoup(self.chrome.page_source, 'html.parser')
                for tag in soup(["nav", "footer", "header", "script", "style", "aside"]):
                    tag.decompose()
                md_text = markdownify.markdownify(str(soup), heading_style="ATX")   # 转 Markdown
                page_description = GetIntroduce(soup, md_text)                      # 获取摘要
                BreadCrumbs = GetBreadcrumbs(soup)                                  # BR路径源！！
                print(BreadCrumbs)
                seeIndex
                # 判断是否为 HOT NODE，并找出命中的关键词
                matched_keywords = [word for word in CORE_KEYWORDS if word in md_text]
                is_hot_node = len(matched_keywords) > 0

                if is_hot_node:
                    print(f"【+】命中关键词: {matched_keywords}")
                else:
                    print("没有关键词，于是转身向山里走去")
                
                path = self.MemPath + "/" + f"INDEX_{seeIndex}.txt"
                
                with open(path, "w", encoding="utf-8") as f:
                    header =   (f"--- \n"
                                f"INDEX: {seeIndex}\n"
                                #f"PARENT_INDEX: {parent_idx}\n"
                                f"URL: {_Url}\n"
                                f"SUMMARY: {md_text}\n"
                                f"--- \n\n")
                    f.write(header + md_text)
    
            """这一步是去杂然后加入对应的队列节点"""
            if True:
                links = soup.find_all('a', href=True)
                for b in links:
                    link_text = b.get_text(strip=True) or "Image/None"      #标题
                    _raw_href = b['href']
                    full_url = clean_url(urljoin(_Url, _raw_href))          #链接
            
                    #DEBPrint(link_text,full_url)
        
                    # 域名检查
                    if urlparse(self.starturl).netloc not in urlparse(full_url).netloc:
                        continue

                    # 黑名单
                    # re.escape 会自动转义列表里的特殊字符（如 google.com 里的点）
                    #blacklist_regex = "|".join(map(re.escape, BLACKLIST))

                    if re.search(blacklist_regex, full_url, re.IGNORECASE) or \
                       re.search(blacklist_regex, (link_text or ""), re.IGNORECASE):
                        continue

                    # 后缀特殊化处理 - 加入队列但是深度设置为最大
                    if any(full_url.lower().endswith(ext) for ext in FILE_EXTENSIONS):
                        _bs = "" if BreadCrumbs == None else BreadCrumbs + ">PDF"
                        self.AddNode(full_url,seeIndex,DepthNow,link_text,_bs,"PDF")
                        continue
                    #
                    #heuristic_regex = "|".join(map(re.escape, HEURISTIC_KEYWORDS))

                    # 修改后的判断逻辑 - 如果是热情模式 或 地址里有好凶西 或 标题里有没有好东西
                    should_follow = (
                        is_hot_node or 
                        re.search(heuristic_regex, full_url, re.IGNORECASE) or 
                        re.search(heuristic_regex, (link_text or ""), re.IGNORECASE)
                    )
                    #如果确实是我们像哟的东西的化
                    if should_follow:
                        self.AddNode(full_url,seeIndex,DepthNow,link_text,BreadCrumbs,page_description)

            print(f"-完成: {len(self.visitedUrls)} | -剩余： {len(self.Uqueue)+1} | -深度: {DepthNow}")
        
        
        CHECK_Noise()
        _write = []
        URL_RLAB = self.URL_RLAB
        #print(self.URL_LAB)
        for _key in self.URL_RLAB.keys():
            _path     = self.MemPath + "/" + f"INDEX_{_key}.txt"
            #mdMessage = "None" if OPAexists(_path) else "None"
            #[url,_nowSize,fatherIndex,fatherDepth+1,title]
            _write.append({"Index":URL_RLAB[_key][1],
                           "FatherIndex":URL_RLAB[_key][2],
                           "Depth":URL_RLAB[_key][3],
                           "title":URL_RLAB[_key][4],
                           "Breadcrumb":URL_RLAB[_key][5],
                           "Url":URL_RLAB[_key][0],       #GetShortURL(baseUrl, URL_RLAB[_key][0])
                           "FatherTitle":URL_RLAB[ str(URL_RLAB[_key][2]) ][4] if not URL_RLAB[_key][2] == -1 else "这个节点没有父节点" ,
                           })

        _pathCsv = self.MemPath + "/" + CSV_FILENAME
        with open(_pathCsv, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ["Index", "FatherIndex", "Depth","title","Breadcrumb","Url", "FatherTitle"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(_write)
            
  
        
        _pathMd  = self.MemPath + "/" + OUTPUT_MD
        _pathHtml= self.MemPath + "/" + HTML_FILENAME
        _pathCsv = self.MemPath + "/" + CSV_FILENAME
        #生成GeneMD
        GeneMD(_pathCsv,_pathMd,self.starturl)
        
        #生成总HTML
        GeneHtml(_pathCsv,_pathHtml)
  
        DEBPrint("耗时（m）",round((time.time()- TPA)/60,2))
        
    # [功能]裁切节点操作
    def Pruning(self):
        _pathMd  = self.MemPath + "/" + OUTPUT_MD
        _pathHtml= self.MemPath + "/" + HTML_FILENAME
        _pathCsv = self.MemPath + "/" + CSV_FILENAME
                #处理简化数据供AI查询
        Easied(_pathCsv,'simpled.csv')
        GeneMD('simpled.csv','SMD.txt',self.starturl,True)
        
        #chunks = packChunks(_pathCsv,self.oriUrl)
        

        #供AI读取
        # 读取剪枝提示词
        with open(DATA_BASE + '/' + "Cutf.txt",mode = 'r',encoding='utf-8') as f:
            prtm = f.read()
        # 读取处理好的剪枝预采样
        with open("SMD.txt",mode = 'r',encoding='utf-8') as f:
            data = f.read()
        print("----- AI Pruning -----")
        completion = Doubao_32.chat.completions.create(
            # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
            model="doubao-1-5-pro-32k-250115",
            messages=[
                {"role": "system", "content": prtm},
                {"role": "user", "content": data},
            ],
             temperature=0.3
        )   
        _returndata = completion.choices[0].message.content
        _dicred = loads(_returndata)
        BlackList = [int(x) if isinstance(x, str) else x for x in (_dicred['DEL_IDX'] if 'DEL_IDX' in _dicred else [])]
        print(BlackList)
        #生成剪切枝后的文件和html
        _pathCleanCsv = self.MemPath + "/" + CSVCLEANED_FILENAME
        _pathHtmled= self.MemPath + "/" + HTMLED_FILENAME
        cutTreeNode(_pathCsv, _pathCleanCsv, BlackList)             #导入源文件切成新的

        GeneHtml(_pathCleanCsv,_pathHtmled)
    
    # [功能]数据二次粉筛审查
    def Category(self):
        _pathCleanCsv = self.MemPath + "/" + CSVCLEANED_FILENAME
        # 读取 CSV 并转换为 {Index: Url} 字典
        with open(_pathCleanCsv, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 这一句用来跳过第一行表头（Index, FatherInd...）
            # 直接取第0列做key，第5列做value
            CLEANED_DIC = {row[0]: row[5] for row in reader}

        # 打印结果查看
        #print(CLEANED_DIC)
        #input()
        # chucks就是数据包块了
        chunks = packChunks(_pathCleanCsv,self.oriUrl)
        with open(DATA_BASE + '/' + "Get.txt",mode = 'r',encoding='utf-8') as f:
            prtm = f.read()
        
        FILE = []
        PAGE = []
        OTHER= []

        for i, chunk in enumerate(chunks):
            OKNoise()
            print("----- AI Categoring -----")
            completion = Doubao_32.chat.completions.create(
                # 指定您创建的方舟推理接入点 ID，此处已帮您修改为您的推理接入点 ID
                model="doubao-1-5-pro-32k-250115",
                messages=[
                    {"role": "system", "content": prtm},
                    {"role": "user", "content": chunk},
                ],
            )
            dics = loads(completion.choices[0].message.content)

            FILE    += [{"Index":_k,"title":dics["FILE"][_k],"url":CLEANED_DIC[_k] if _k in CLEANED_DIC else "ERROR" ,"Type":"FILE"}  for _k in dics["FILE"].keys()] if "FILE" in dics else []               
            PAGE    += [{"Index":_k,"title":dics["PAGE"][_k],"url":CLEANED_DIC[_k] if _k in CLEANED_DIC else "ERROR" ,"Type":"PAGE"}  for _k in dics["PAGE"].keys()] if "PAGE" in dics else [] 
            OTHER   += [{"Index":_k,"title":dics["OTHER"][_k],"url":CLEANED_DIC[_k] if _k in CLEANED_DIC else "ERROR" ,"Type":"OTHER"} for _k in dics["OTHER"].keys()] if "OTHER" in dics else [] 
        CHECK_Noise()    
        #存储数据

        _pathSave1     = self.MemPath + "/"  + "categoryA.csv"
        _pathSave2     = self.MemPath + "/"  + "categoryB.csv"
        
        FILE += PAGE
        with open(_pathSave1, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ["Index", "title", "url", "Type"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(FILE)
            
        with open(_pathSave2, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ["Index", "title", "url", "Type"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(OTHER)
        

# 阶梯状爬行者
class OverView_FloorMode():
    pass






#解决人机验证问题
def CookieGo(driver):
    """处理Cookie弹窗，发现人机验证则回退"""
    page_source = driver.page_source
    # 1. 检测人机验证 (Captcha)
    captcha_signals = ["g-recaptcha", "hcaptcha", "captcha-delivery", "人机验证", "私はロボットではありません"]
    if any(sig in page_source for sig in captcha_signals):
        print("检测到人机验证，正在尝试返回上一级...")
        driver.back()
        return False

    # 2. 自动点击Cookie同意按钮
    cookie_keywords = ["同意", "Accept", "Agree", "OK", "はい", "承諾"]
    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            if any(k in btn.text for k in cookie_keywords):
                btn.click()
                print("\t点击Cookie")
                break
    except: pass
    return True

def clean_url(url):
    """规范化URL，去除锚点(#)和末尾斜杠，防止同一个页面被索引两次"""
    parsed = urlparse(url)
    cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))
    return cleaned.rstrip('/')


    #获得页面摘要
def GetIntroduce(soup,md_text):
    """生成页面简单总结：提取标题和前100个有意义的字符"""
    title = soup.title.string if soup.title else "No Title"
    clean_text = re.sub(r'\s+', ' ', md_text).strip()
    summary = f"Title: {title} | Content: {clean_text[:100]}..."
    return summary


def create_Folder(path):
    OSmakedirs(path) if not OSisdir(path) else 0
  
    
#面包屑路径捕捉
def GetBreadcrumbs(soup):
    container = soup.find(attrs={"typeof": "BreadcrumbList"}) or \
                soup.find(id=re.compile(r'breadcrumb|topicpath', re.I)) or \
                soup.find(class_=re.compile(r'breadcrumb|topicpath', re.I))
    if not container:
        return "None"

    raw_items = []

    for element in container.find_all(['span', 'li', 'a']):

        text = element.get_text(strip=True)
        if text and text not in ['>', '/', '»', '＞']:

            if not raw_items or text != raw_items[-1]:
                raw_items.append(text)

    clean_items = [re.sub(r'[🏠\s]', '', i) for i in raw_items]
    clean_items = [i for i in clean_items if i and i.upper() not in ['HOME', 'TOP', '🏠', '首页']]

    if clean_items:
        return " > ".join(clean_items)
    
    return "None"

#打印日志类
class DebugPrinter():
    def __init__(self,WriteDiary = False) -> None:
        self._DEBNumber = 1
        self._DEF_ANIDELTA = 0.05
        self.DEBUG = True
        self.color = "\033[36m"
        
        self.willWrite = WriteDiary
        self.debugFilePath = 'NO.txt'

        # 检查文件夹是否存在 - 不存在则创建
        OSmakedirs("_debug") if not OSisdir("_debug") else 0
        NAME = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())).replace('-','_').replace(':','_').replace(' ','')
        _ind = 0
        if WriteDiary:
            while True:
                if OPAexists("_debug/" + NAME + ('' if _ind == 0 else str(_ind)) + ".txt"): 
                    _ind += 1
                else:
                    _Fin = "_debug/" + NAME+ ('' if _ind == 0 else str(_ind)) + ".txt"
                    self.debugFilePath = _Fin
                    with open(_Fin, 'w', encoding='utf-8') as file:
                        file.write("[DEBUG]"+NAME+"\n");file.close()	
                    break
                

    #[功能] 动画流输出
    def DEBAnimaPrint(self,a,ends = '\n\n',_delta = None,coL = True):
        delta = self._DEF_ANIDELTA if _delta == None else _delta
        #coL = "\033[36m" if coL else "\033[37m"
        _col = self.color if coL else "\033[37m"
        print('\n【'+str(self._DEBNumber)+'】\t',end='')
        for _a in str(a):
            time.sleep(delta); 
            _A = print( _col + _a + "\033[0m",end='') if self.DEBUG else 0
            stdout.flush()
        print('\a',end=ends);time.sleep(1);
        self._writeDiary('\n【'+str(self._DEBNumber)+'】\t' + str(a) + ends)
        self._DEBNumber+=1;

    #[功能] 直接输出       
    def DEBPrint(self,a,b = '' ,c = '',ends = '\n\n'):
        _a = print('\n【'+str(self._DEBNumber)+'】\t',a,b,c,end=ends) if self.DEBUG else 0;
        self._writeDiary('\n【'+str(self._DEBNumber)+'】\t' + str(a) + ends)
        self._DEBNumber+=1;
        Tsleep(0.1)

    #[功能] 动画流输出
    def _writeDiary(self,content = ''):
        if (not self.willWrite):
            print("EMPTY")
            return
        with open(self.debugFilePath, 'a', encoding='utf-8') as file:
            file.write(content);file.close()

    def reset(self,Sign = 1):
        self._DEBNumber = Sign





# 和这个程序绑定的 DEBP
_DP = DebugPrinter(WriteDiary=True)
DEBPrint = _DP.DEBPrint
DEBAnimaPrint = _DP.DEBAnimaPrint





# 预定义音效逻辑
def get_signal(t, effect):
    if effect == "success":      
        if t < 0.1:
            return 4 * math.sin(2 * math.pi * 987.77 * t)
        elif t < 1.0:
            decay = 1.0 - ((t - 0.1) / 0.9)
            return 2 * math.sin(2 * math.pi * 1318.51 * t) * decay
    elif effect == "danger":
        freq = 1000 + 400 * (1 if (t * 10) % 2 > 1 else -1)
        return 1.0 if 2*math.sin(2 * math.pi * freq * t) > 0 else -1.0
    elif effect == "error":
        freq = max(30, 200 - t * 170)
        noise = math.sin(2 * math.pi * freq * t) + 0.5 * math.sin(2 * math.pi * (freq*0.5) * t)
        return 1.0 if noise > 0 else -1.0
    elif effect == "progress":
        sub_t = t % 0.5
        if sub_t < 0.15:
            return math.sin(2 * math.pi * (880 + sub_t * 1500) * sub_t)
    return 0


#[功能]  播放音频 (跨平台兼容)
def play_effect(effect_type):
    # 非 Windows 平台静默跳过音效
    if platform.system() != 'Windows':
        return

    try:
        import winsound
        sample_rate, duration, amplitude = 44100, 1.0, 3600
        byte_io = io.BytesIO()
        with wave.open(byte_io, 'wb') as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            num_samples = int(sample_rate * duration)
            samples = []
            for i in range(num_samples):
                val = get_signal(i / sample_rate, effect_type)
                samples.append(struct.pack('<h', int(val * amplitude)))
                if len(samples) > 1000:
                    wav_file.writeframes(b''.join(samples))
                    samples = []
            wav_file.writeframes(b''.join(samples))
        winsound.PlaySound(byte_io.getvalue(), winsound.SND_MEMORY)
    except ImportError:
        pass  # winsound 不可用时静默跳过
  
# 生成缩略版链接
def GetShortURL(base_url, target_url):
    #判断 target_url 是否属于 base_url 的本域。
    #如果是，返回相对路径（/path/to/page）；
    #如果不是（跨域或外部链接），返回完整的 target_url。
    try:
        base_p = urlparse(base_url)
        target_p = urlparse(target_url)

        # 核心判断：比较域名 (netloc) 
        # 例如: www.chiba-u.ac.jp 和 www.chiba-u.ac.jp 是否一致
        if base_p.netloc == target_p.netloc and base_p.scheme == target_p.scheme:
            # 组装相对路径：path + params + query + fragment
            # 注意：如果 path 为空，至少返回一个 /
            short_path = target_p.path if target_p.path else "/"
            if target_p.query:
                short_path += "?" + target_p.query
            if target_p.fragment:
                short_path += "#" + target_p.fragment
                
            return base_url if "/" == short_path else short_path
        else:
            # 跨域了，必须返回完整地址，否则 AI 会拼错
            return target_url
    except Exception:
        # 发生意外（如格式极其离奇），返回原样以保安全
        return target_url
  
    
# 生成HTML链接
def GeneHtml(csv_file, output_name="university_network.html"):
    # 1. 读取并处理数据
    data = []
    try:
        with open(csv_file, mode='r', encoding='utf-8-sig') as f:
            # 使用 DictReader 自动将表头作为 Key
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except Exception as e:
        print(f"[-] 读取文件失败: {e}")
        return

    if not data:
        print("[-] CSV 文件为空")
        return

    # 创建一个索引集，用于快速检查 FatherIndex 是否存在 (代替 pandas 的 .values)
    all_indices = {int(row['Index']) for row in data}
    
    nodes = []
    edges = []

    # 🎨 更加精美的配色方案 (定义在循环外)
    COLOR_MAP = {
        -1: {"bg": "#2f3640", "border": "#1e272e"}, # 根根
        0:  {"bg": "#ff6b6b", "border": "#ee5253"}, # 根节点 (红色)
        1:  {"bg": "#feca57", "border": "#ff9f43"}, # 一级 (橙黄)
        2:  {"bg": "#1dd1a1", "border": "#10ac84"}, # 二级 (翠绿)
        3:  {"bg": "#48dbfb", "border": "#0abde3"}, # 三级 (天蓝)
    }

    # 2. 构造节点与边
    for row in data:
        idx = int(row['Index'])
        father_idx = int(row['FatherIndex'])
        title = str(row['title']).replace('"', '\\"')
        url = str(row['Url'])
        breadcrumb = str(row['Breadcrumb']).replace('"', '\\"')
        depth = int(row['Depth'])

        # --- 节点处理 ---
        style = COLOR_MAP.get(depth, {"bg": "#c8d6e5", "border": "#8395a7"})
        
        nodes.append({
            "id": idx,
            "label": (title[:12] + "..") if len(title) > 12 else title,
            "fullTitle": title,
            "url": url,
            "breadcrumb": breadcrumb,
            "color": {
                "background": style["bg"],
                "border": style["border"],
                "highlight": {"background": "#ffffff", "border": style["bg"]},
                "hover": {"background": "#ffffff", "border": style["bg"]}
            },
            "borderWidth": 3,
            "shape": "dot",
            "size": 30 if depth == 0 else (22 if depth == 1 else 15),
            "shadow": {"enabled": True, "color": "rgba(0,0,0,0.2)", "size": 10, "x": 5, "y": 5}
        })

        # --- 边处理 ---
        if father_idx != -1 and father_idx in all_indices:
            edges.append({
                "from": father_idx, 
                "to": idx,
                "color": {"color": "#a4b0be", "highlight": "#54a0ff"},
                "width": 1.5,
                "arrows": {"to": {"enabled": True, "scaleFactor": 0.4}}
            })

    # 3. HTML 模板 (逻辑保持不变)
    html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>大学入试知识图谱</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body, html {{ margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden; font-family: 'Segoe UI', sans-serif; }}
        #mynetwork {{ width: 100%; height: 100vh; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); }}
        #loader {{
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(255,255,255,0.9);
            display: flex; flex-direction: column; justify-content: center; align-items: center;
            z-index: 9999;
        }}
        .progress-container {{ width: 300px; height: 10px; background: #eee; border-radius: 5px; overflow: hidden; margin-top: 20px; }}
        #progress-bar {{ width: 0%; height: 100%; background: #54a0ff; transition: width 0.1s; }}
        #progress-text {{ font-size: 14px; color: #576574; font-weight: bold; }}
        .custom-tooltip {{
            position: absolute; visibility: hidden;
            background: rgba(255, 255, 255, 0.9);
            backdrop-filter: blur(5px);
            border-left: 6px solid #54a0ff;
            border-radius: 12px; padding: 18px;
            box-shadow: 0 15px 35px rgba(0,0,0,0.15);
            pointer-events: none; z-index: 1000; max-width: 320px;
        }}
        .t-path {{ font-size: 11px; color: #8395a7; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }}
        .t-id {{ font-weight: bold; color: #54a0ff; font-size: 12px; margin-bottom: 5px; }}
        .t-title {{ font-size: 16px; color: #2d3436; line-height: 1.5; font-weight: 600; }}
        .t-footer {{ margin-top: 12px; font-size: 11px; color: #ee5253; text-align: right; border-top: 1px solid rgba(0,0,0,0.05); padding-top: 10px; font-style: italic; }}
    </style>
</head>
<body>
    <div id="loader">
        <div id="progress-text">正在计算节点布局... 0%</div>
        <div class="progress-container"><div id="progress-bar"></div></div>
    </div>
    <div id="tooltip" class="custom-tooltip"></div>
    <div id="mynetwork"></div>

    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(nodes)});
        var edges = new vis.DataSet({json.dumps(edges)});

        var container = document.getElementById('mynetwork');
        var tooltip = document.getElementById('tooltip');
        var progressBar = document.getElementById('progress-bar');
        var progressText = document.getElementById('progress-text');
        var loader = document.getElementById('loader');
        
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{
            physics: {{
                forceAtlas2Based: {{ gravConstant: -120, centralGravity: 0.01, springLength: 120, damping: 0.4 }},
                solver: 'forceAtlas2Based',
                stabilization: {{ iterations: 200 }}
            }},
            interaction: {{ hover: true, tooltipDelay: 0 }},
            nodes: {{ font: {{ face: 'Segoe UI', size: 14, strokeWidth: 4, strokeColor: '#ffffff' }} }}
        }};

        var network = new vis.Network(container, data, options);

        network.on("stabilizationProgress", function(params) {{
            var progress = Math.round((params.iterations / params.total) * 100);
            progressBar.style.width = progress + '%';
            progressText.innerText = '正在计算节点布局... ' + progress + '%';
        }});

        network.once("stabilizationIterationsDone", function() {{
            loader.style.opacity = '0';
            setTimeout(() => loader.style.display = 'none', 500);
        }});

        network.on("hoverNode", function (params) {{
            var node = nodes.get(params.node);
            tooltip.innerHTML = `
                <div class="t-path">${{node.breadcrumb}}</div>
                <div class="t-id">NODE INDEX: ${{node.id}}</div>
                <div class="t-title">${{node.fullTitle}}</div>
                <div class="t-footer">🖱️ 双击跳转至链接</div>
            `;
            tooltip.style.visibility = "visible";
        }});

        network.on("blurNode", function () {{ tooltip.style.visibility = "hidden"; }});

        container.addEventListener('mousemove', function(e) {{
            tooltip.style.left = (e.pageX + 20) + 'px';
            tooltip.style.top = (e.pageY + 20) + 'px';
        }});

        network.on("doubleClick", function (params) {{
            if (params.nodes.length > 0) {{
                var node = nodes.get(params.nodes[0]);
                if (node.url && node.url !== 'None') {{ window.open(node.url, '_blank'); }}
            }}
        }});
    </script>
</body>
</html>
    """

    with open(output_name, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"[+] 转换成功，已生成: {output_name}")



def GeneMD(INPUT_CSV,OUTPUT_MD,BASE_URL,IgnoreChile:bool = False):
    # 1. 全局数据索引化 (为了能随时查出任何节点的完整信息)
    all_nodes = {}  # { Index: {完整数据} }
    buckets = {}    # { FatherIndex: [子节点Index列表] }
    
    print(f"⌛ 正在全量解析数据...")
    with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            idx = row['Index']
            f_idx = row['FatherIndex']
            
            # 存储节点完整数据
            all_nodes[idx] = row
            
            # 分桶
            if f_idx not in buckets:
                buckets[f_idx] = []
            buckets[f_idx].append(idx)

    # 2. 排序（按子节点数量降序）
    sorted_f_indices = sorted(buckets.keys(), key=lambda x: len(buckets[x]), reverse=True)

    # 3. 生成输出内容
    output = []
    output.append(f"BASE_URL: {BASE_URL}")
    output.append("FORMAT_SCHEMA: Index | FatherIndex | Short_URL | Breadcrumb | Title\n")
    output.append("--- START OF STRUCTURED DATA ---\n")

    mis_f_indices = [] # 存放小规模群组的父ID

    def format_line(node_idx):
        """统一的行格式化工具"""
        node = all_nodes.get(str(node_idx))
        if not node: return f"{node_idx} | Unknown Node Data"
        
        idx = node['Index']
        f_id = node['FatherIndex']
        url = GetShortURL(BASE_URL,node['Url'])
        bc = node.get('Breadcrumb', 'None')
        title = node.get('title', 'None')
        return f"{idx} | {f_id} | {url} | {bc} | {title}"

    # 第一部分：大型群组 (子节点 > 2)
    for f_idx in sorted_f_indices:
        child_indices = buckets[f_idx]
        if len(child_indices) > 2:
            output.append(f"# [CLUSTER_GROUP_START: FatherNode {f_idx}]")
            
            # 【关键修正】: 先打印父节点本身的完整信息
            if f_idx in all_nodes:
                output.append(f"FatherNode -> {format_line(f_idx)}")
                output.append("-" * 25) # 小分隔线表示父子关系
            else:
                output.append(f"FatherNode -> {f_idx} | -1 | ROOT_OR_EXTERNAL | None | SEED_PAGE")

            # 打印子节点
            for c_idx in child_indices:
                output.append(format_line(c_idx))
            output.append(f"# [CLUSTER_GROUP_END]\n")
        else:
            mis_f_indices.append(f_idx)
    if not IgnoreChile:
        # 第二部分：散碎节点（单节点/双节点）
        if mis_f_indices:
            output.append("\n" + "="*60)
            output.append("# REMARK_FOR_AI: The following are scattered single/double nodes.")
            output.append("# FORMAT REMAINS THE SAME. Father information is embedded in each line.")
            output.append("# SECTION: MISC_LINKS_FLAT_LIST")
            output.append("="*60 + "\n")
        
            for f_idx in mis_f_indices:
                # 即使是散碎节点，也要把它们属于哪个“爸爸”打印清楚
                for c_idx in buckets[f_idx]:
                    output.append(format_line(c_idx))

    # 4. 写入文件
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write("\n".join(output))

    DEBPrint(f"\t任务OK！")
    DEBPrint(f"\t精炼报表已保存: {OUTPUT_MD}")
    DEBPrint(f"\t节点总数: {len(all_nodes)}")
    
warningNoise = lambda : play_effect("danger")       #跳过了店铺
OKNoise      = lambda : play_effect("progress")     #程序顺利进行 - 如进入页面，开始爬取数据,休息
CHECK_Noise   = lambda : play_effect("success")      #暗示书籍到手 
ERROR_Noise   = lambda : play_effect("error")        #Selenium出错 或者登录出错-这种时候可能会导致程序关闭 就会报错这个



#这里我优化两次了，强行缩小了很多内存

def Easied(input_file, output_file,max_children=3):
    """
    强制硬限抽样：确保输出结果中，任何父节点在第二列的出现次数绝对不超过 max_children。
    """
    all_nodes = {}
    children_map = defaultdict(list)
    header = None
    
    IDX_COL = 0
    PIDX_COL = 1

    # 1. 第一次遍历：构建内存索引
    with open(input_file, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if not row: continue
            idx = row[IDX_COL]
            pidx = row[PIDX_COL]
            all_nodes[idx] = row
            if pidx and pidx != "-1":
                children_map[pidx].append(row)

    final_rows_dict = {} # 使用字典确保唯一性 {idx: row}

    # 2. 遍历所有拥有子节点的父级
    for pidx, children in children_map.items():
        # 强制策略 A: 如果父节点本身不在结果集中，先把它加进去（作为背景参考）
        # 注意：父节点 row 自己的 PIDX 是它更上一级的，所以不占用当前 pidx 的计数
        if pidx in all_nodes and pidx not in final_rows_dict:
            final_rows_dict[pidx] = all_nodes[pidx]

        # 强制策略 B: 严格限制子节点数量
        # 无论原本有多少，只取前 max_children 个
        sampled_children = children[:max_children]
        
        for child_row in sampled_children:
            c_idx = child_row[IDX_COL]
            final_rows_dict[c_idx] = child_row

    # 3. 结果转换与安全性校验
    final_data = list(final_rows_dict.values())
    
    # 物理验证（防止逻辑漏洞）
    counts = defaultdict(int)
    safe_data = []
    # 重新加入表头
    safe_data.append(header)
    
    # 这一步是为了应对你说的“直白要求”：第二列相同INDEX不得超过5个（这里限制为3）
    for row in final_data:
        p_val = row[PIDX_COL]
        if p_val == "-1" or p_val == "":
            safe_data.append(row)
            continue
            
        if counts[p_val] < max_children:
            safe_data.append(row)
            counts[p_val] += 1
        else:
            # 超过了硬限，该行即使被选中也要舍弃（除非它是其他人的父节点，但在这一步我们只看它作为子节点的情况）
            pass

    # 4. 导出
    with open(output_file, mode='w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(safe_data)

    DEBPrint(f"--- 强力抽样完成 ---")
    DEBPrint(f"原始数据: {len(all_nodes)} 行")
    DEBPrint(f"抽样后(含表头): {len(safe_data)} 行")
    DEBPrint(f"强制标准: 第二列(ParentIndex)重复上限 = {max_children}")
    
    return safe_data

# 根据剪枝内容，切掉无用部分
def cutTreeNode(input_csv, output_csv, blacklist_indices):
    """
    根据AI返回的黑名单，移除对应的子节点。
    :param input_csv: 原始的65kb全量数据CSV
    :param output_csv: 清洗后的精简CSV
    :param blacklist_indices: AI判断为无效的父节点INDEX列表
    """
    # 将列表转换为 set，提高查找效率 (O(1) 复杂度)
    black_set = set(str(i) for i in blacklist_indices)
    
    cleaned_rows = []
    removed_count = 0
    
    try:
        with open(input_csv, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader) # 获取表头
            cleaned_rows.append(header)
            
            # 假设 CSV 结构：0: INDEX, 1: PARENT_INDEX
            # 如果你的列顺序不同，请修改下面的索引值
            IDX_COL = 0
            PIDX_COL = 1
            
            for row in reader:
                # 容错处理：跳过空行
                if not row: continue
                
                current_idx = row[IDX_COL]
                parent_idx = row[PIDX_COL]
                
                # 判定逻辑：
                # 如果当前节点的 PARENT_INDEX 在黑名单里，说明它是“大头噪音”的子节点，跳过（删除）
                if parent_idx in black_set:
                    removed_count += 1
                    continue
                
                # 否则保留
                cleaned_rows.append(row)
                
        # 写入新 CSV
        with open(output_csv, mode='w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(cleaned_rows)
            
        DEBPrint(f"--- 数据清洗完成 ---")
        DEBPrint(f"原始记录总数（含表头）: {len(cleaned_rows) + removed_count}")
        DEBPrint(f"成功移除子节点数量: {removed_count}")
        DEBPrint(f"保留节点数量: {len(cleaned_rows)}")
        DEBPrint(f"清洗后的文件已保存至: {output_csv}")

    except FileNotFoundError:
        DEBPrint(f"错误：找不到文件 {input_csv}，请检查路径。")


# 把数据打包成多个包发送给ai
def packChunks(input_csv,base_url):
# 1. 配置列索引
    IDX_COL = 0
    PIDX_COL = 1
    TITLE_COL = 3
    BREAD_COL = 4
    URL_COL = 5
    FTITLE_COL = 6
    
    # 定义最大字节数 (预留 500 字节给 Group 结束符等缓冲)
    MAX_BYTES = int(PACK_MAX_SIZE * 1024) 
    SAFE_LIMIT = MAX_BYTES - 500 

    # 2. 读取数据 (保持不变)
    all_nodes = {}
    parent_to_children = defaultdict(list)
    
    with open(input_csv, mode='r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if not row: continue
            all_nodes[row[IDX_COL]] = row
            if row[PIDX_COL] != "-1":
                parent_to_children[row[PIDX_COL]].append(row)

    # 3. 定义单行文本化函数 (保持不变)
    def row_to_text(row):
        return f"{row[IDX_COL]} | {row[FTITLE_COL]} | {row[BREAD_COL]} | {row[TITLE_COL]} | {GetShortURL(base_url,row[URL_COL])}\n"

    # ================= 修改重点开始 =================
    # 4. 按家族打包文本 (新增：大“家族”拆分逻辑)
    families = []
    
    for pidx, children in parent_to_children.items():
        # 准备父节点信息头
        header_text = ""
        if pidx in all_nodes:
            parent_row = all_nodes[pidx]
            header_text = f"-- GROUP START (Father: {pidx}) --\n"
            header_text += "FATHER -> " + row_to_text(parent_row)
        else:
            header_text = f"-- GROUP START (Father: {pidx} - Missing) --\n"

        footer_text = "-- GROUP END --\n\n"
        
        # 当前正在构建的文本块
        current_fam_text = header_text
        
        for child in children:
            child_line = "  CHILD -> " + row_to_text(child)
            
            # 预判加入这行后是否会超限
            # 注意：如果这是新起的一个块，必须至少放一行，否则会死循环
            current_len = len(current_fam_text.encode('utf-8'))
            child_len = len(child_line.encode('utf-8'))
            
            # 如果加上这一行子节点 + 结尾符 会超过安全限制
            if current_len + child_len + len(footer_text.encode('utf-8')) > SAFE_LIMIT:
                # 1. 封存当前块
                current_fam_text += footer_text
                families.append(current_fam_text)
                
                # 2. 开启新块 (重要：为了 AI 上下文，新块必须再次包含父节点头信息)
                current_fam_text = header_text + child_line
            else:
                # 没超限，加入当前块
                current_fam_text += child_line
        
        # 循环结束，把最后剩余的部分加上结尾并保存
        current_fam_text += footer_text
        families.append(current_fam_text)

    # 5. 分块逻辑 (逻辑简化，因为 Step 4 已经保证了单个 family 不会超限)
    chunks = []
    current_chunk = ""

    for fam_text in families:
        # 检查加入当前家族后是否超限
        potential_size = len((current_chunk + fam_text).encode('utf-8'))
        
        if potential_size <= MAX_BYTES:
            current_chunk += fam_text
        else:
            # 如果 current_chunk 有内容，先保存
            if current_chunk:
                chunks.append(current_chunk)
            # 开启新的 chunk
            current_chunk = fam_text
    
    if current_chunk:
        chunks.append(current_chunk)
    # ================= 修改重点结束 =================

    # 6. 打印分块结果报告 (保持不变)

    DEBPrint(f"分块报告:")
    DEBPrint(f"总计生成的文本片段数: {len(families)}")
    DEBPrint(f"最终切分为 AI 批次数: {len(chunks)}")
    print("-" * 30)
    
    for i, chunk in enumerate(chunks):
        size_kb = len(chunk.encode('utf-8')) / 1024
        father_count = chunk.count("FATHER ->")
        child_count = chunk.count("CHILD ->")
        
        DEBPrint(f"【Batch {i+1}】")
        DEBPrint(f"  - 数据大小: {size_kb:.2f} KB")
        DEBPrint(f"  - 节点数量: 父级 {father_count} | 子级 {child_count}")
        print("-" * 50)

    return chunks
