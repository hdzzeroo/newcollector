import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# SupportData 用于支持程序运行的数据库
#   这个数据库包含了许多内容：
#   参数设定
#   关键字
#   密匙



# 参数设定
# region SupportData

# ——————————————————————————————————————————————————————————

OUTPUT_FOLDER   = "MemMD"           # 缓存HTML文件的地方
DATA_BASE       = "AIPmt"            # 缓存
CSV_FILENAME    = "Final.csv"        
CSVCLEANED_FILENAME = "cleaned.csv"
HTML_FILENAME   = "CollegeWeb.html" #处理前的数据
HTMLED_FILENAME = "EasiedWeb.html"  #处理过后的数据
OUTPUT_MD       = "MD.txt"
MAX_DEPTH       = 2                 # 深度建议 2-3，否则链接呈指数爆炸
                                    # 深度1 - 25个网站             如果页面就是目标那u么几乎是不到一分钟就可以的
                                    # 深度2 - 80-100个网站左右     3分钟-4分钟，实际可以保存非常多的网页  群马大学页面-4分钟
                                    # 深度3 - 1000个网站           8-10分 - 可以再优化词汇
PACK_MAX_SIZE = 16           #分包最大大小
# ——————————————————————————————————————————————————————————

# endregion

# 参数设定
# region SupportData
# ——————————————————————————————————————————————————————————

# API 密钥从环境变量读取
# DeepSeek_Key = os.getenv("DEEPSEEK_API_KEY", "")
# GPT_Key      = os.getenv("OPENAI_API_KEY", "")
Dou_Bao_Key  = os.getenv("DOUBAO_API_KEY", "")
# Dou_Bao_Key_256 用不上
# ——————————————————————————————————————————————————————————

# endregion


# 1. 核心关键词库 (用于判断是否为 粗神经模式)
CORE_KEYWORDS = [
    "募集要項", "願書", "出願", "過去問", "入試問題", "試験問題", "合格発表", 
    "外国人留学生", "検定料", "試験" 
    
]
#"2025", "2026", "R6", "R7", "令和6", "令和7", 这几个太泛泛了！
#"Application Guidelines", "Admission", "Past Exam", "Past Papers", "Exam Result"   容易跳到英文网页去，很糟糕哈哈哈
# 2. 启发式关键词 (用于普通页面的链接筛选)
HEURISTIC_KEYWORDS = CORE_KEYWORDS + [
    
    "入試","試験","要項", "特別選抜"

    "大学院", "学部", "入試", "案内", "資料", "書類", "download", "admissions", "過去問", "出願","募集要項",
    "2025", "2026", "R6", "R7", "令和6", "令和7" , "PDF",
    
    "boshuyoko", "kakomon", "shutsugan", "nyushi", "daigakuin",
    "shushi", "hakushi", "kenkyuka", "mondai", "yoko",
    "application", "guidelines", "past_exam", "international",
    "master", "doctor", "shorui", "entry", "download"   ,
    "Application Guidelines", "Admission", "Past Exam", "Past Papers", "Exam Result"
]

# 3. 严格黑名单 (绝对不点)
"""
BLACKLIST = [
    "access", "map", "transport", "google.com", "facebook", "twitter", "instagram","view","archive","calendar"
    "event", "news", "新闻", "ニュース", "イベント", "寄付", "donation", "history",
    "沿革", "philosophy", "理念", "contact", "お問い合わせ", "问い合わせ", "校歌"  , 
]
"""
BLACKLIST = [
    # --- 社交与外部平台 ---
    "facebook.com", "twitter.com", "instagram.com", "youtube.com", "line.me", "linkedin", "plus.google",
    
    # --- 校园地图与交通 (最容易卡住的地方) ---
    "access", "map", "transport", "校园地图", "キャンパス", "マップ", "アクセス", "交通", "駐車場", "google.com/maps",
    
    # --- 动态生成的无限链接 (爬虫杀手) ---
    "calendar", "archive", "view", "month", "day", "year", "schedule", "timetable", "行事", "カレンダー", "予定",
    "search", "filter", "query", "sort", "tags", "category",
    
    # --- 校内新闻与活动 (通常极多且无意义) ---
    "news", "event", "新闻", "ニュース", "イベント", "トピックス", "topics", "press", "公告", "お知らせ",
    
    # --- 行政、法律与门户 ---
    "privacy", "policy", "terms", "sitemap", "site-policy", "copyright", "プライバシー", "ポリシー", "サイトマップ",
    "login", "auth", "register", "mypage", "portal", "ログイン", "マイページ",
    
    # --- 大学背景与文化 (非入试相关) ---
    "history", "沿革", "philosophy", "理念", "校歌", "anthem", "symbol", "greeting", "学長", "organization", "组织", "沿革",
    "donation", "寄付", "kifu", "基金", "fund", "giving","広報"
    
    # --- 附属医院与患者信息 (医科大学的重灾区) ---
    "hospital", "medical", "patient", "clinic", "病院", "患者", "外来", "診療", "看護",
    
    # --- 招聘与办公 ---
    "recruit", "job", "career-support", "staff", "faculty-member", "採用", "公募", "求人", "人事", "調達", "tender",
    
    # --- 联系方式 ---
    "contact", "お問い合わせ", "问い合わせ", "inquiry", "tel", "mail", "form"
]


# 绝对禁止进入浏览器的后缀名
FILE_EXTENSIONS = [
    ".pdf", ".zip", ".rar", ".docx", ".doc", ".xlsx", ".xls", ".rdf"   ,".mp3" ,".mp5"
    ".pptx", ".ppt", ".jpg", ".png", ".jpeg", ".gif", ".mp4",".mpg"
]
#options = Options()
#options.add_argument('--no-sandbox')
#options.add_argument('--log-level=3')


#扁平化处理 + 关键信息强化 + 面包屑导航
#总结 - 该种方案可能相对更适合 深度为1的情况，这样让AI根据网址等信息读取内容的时候可以相对较快的总结PDF的内容 -不烧Tokens
# 为什么不太适合全网浏览 - 不是不适合而是 浏览方案可能有所不同，比如应该借助 google search的方案来走更好点

# 这个的最大优势 -1 是快 -2是成本极低！

#扁平化处理的优势：当数据小于500的时候也就是深度为1的时候是最容易的此时可以不加任何的优化，
#但深度提高后，就必须要对源数据进行特殊化的处理了
