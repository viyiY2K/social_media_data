import time
import os
import json
import csv
from datetime import datetime
from playwright.sync_api import sync_playwright
from xhs import XhsClient

class RedBookClient:
    def __init__(self, cookies_file='redbook_cookie.json'):
        self.client = None
        self.a1 = ""
        self.web_session = ""
        self.init_client(cookies_file)
    
    def init_client(self, cookies_file):
        try:
            if not os.path.exists(cookies_file):
                print(f"❌ 找不到cookies文件: {cookies_file}")
                return
                
            with open(cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            # 提取重要cookies并保存为实例变量
            for cookie in cookies_data:
                if cookie.get('name') == 'a1':
                    self.a1 = cookie.get('value', '')
                elif cookie.get('name') == 'web_session':
                    self.web_session = cookie.get('value', '')
            
            # 转换为cookie字符串
            cookie_pairs = []
            for cookie in cookies_data:
                name = cookie.get('name', '')
                value = cookie.get('value', '')
                if name and value:
                    cookie_pairs.append(f"{name}={value}")
            cookie_string = '; '.join(cookie_pairs)
            
            # 创建客户端时只传递签名函数，不传递额外参数
            self.client = XhsClient(cookie=cookie_string, sign=self.sign)
            print("✅ 小红书客户端初始化成功")
            
        except Exception as e:
            print(f"❌ 初始化小红书客户端失败: {e}")
    
    def sign(self, uri, data=None, a1="", web_session=""):
        """签名函数，接受 uri、data、a1 和 web_session 参数"""
        # 如果传入的参数为空，使用实例变量
        if not a1:
            a1 = self.a1
        if not web_session:
            web_session = self.web_session
            
        try:
            with sync_playwright() as playwright:
                stealth_js_path = "./stealth.min.js"
                
                chromium = playwright.chromium
                browser = chromium.launch(headless=True)
                browser_context = browser.new_context()
                
                if os.path.exists(stealth_js_path):
                    browser_context.add_init_script(path=stealth_js_path)
                
                context_page = browser_context.new_page()
                context_page.goto("https://www.xiaohongshu.com")
                
                # 使用传入的 a1 参数或实例变量
                if a1:
                    browser_context.add_cookies([
                        {'name': 'a1', 'value': a1, 'domain': ".xiaohongshu.com", 'path': "/"}
                    ])
                
                context_page.reload()
                context_page.wait_for_timeout(2000)
                
                encrypt_params = context_page.evaluate("([url, data]) => window._webmsxyw(url, data)", [uri, data])
                browser.close()
                
                if encrypt_params and 'X-s' in encrypt_params:
                    return {
                        "x-s": encrypt_params["X-s"],
                        "x-t": str(encrypt_params["X-t"])
                    }
        except Exception as e:
            print(f"❌ 小红书签名生成失败: {e}")
        return {"x-s": "", "x-t": ""}
    
    def get_user_info_by_id(self, user_id):
        if not self.client:
            return None
        
        try:
            user_info = self.client.get_user_info(user_id)
            
            username = None
            followers_count = 0
            
            if isinstance(user_info, dict):
                # 获取用户名
                username_paths = [
                    ['basic_info', 'nickname'],
                    ['user_info', 'nickname'], 
                    ['data', 'basic_info', 'nickname'],
                    ['nickname']
                ]
                
                for path in username_paths:
                    try:
                        temp_data = user_info
                        for key in path:
                            temp_data = temp_data[key]
                        username = str(temp_data)
                        break
                    except (KeyError, TypeError, AttributeError):
                        continue
                
                # 获取粉丝数
                if 'interactions' in user_info:
                    for interaction in user_info['interactions']:
                        if (interaction.get('type') == 'fans' or 
                            interaction.get('name') == '粉丝'):
                            try:
                                count_str = interaction.get('count', '0')
                                followers_count = int(str(count_str).replace(',', '').replace(' ', ''))
                                break
                            except (ValueError, AttributeError, TypeError):
                                continue
            
            if not username:
                username = f'用户_{user_id}'
            
            return {
                'name': username,
                'followers': followers_count
            }
                
        except Exception as e:
            print(f"❌ 获取用户 {user_id} 信息时出错: {e}")
            return None

# 导出函数：获取小红书数据
def get_redbook_data(user_ids):
    """
    获取小红书用户数据
    :param user_ids: 用户ID列表
    :return: 数据列表
    """
    print("📖 开始获取小红书数据...")
    
    client = RedBookClient()
    if not client.client:
        return []
    
    data_list = []
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for user_id in user_ids:
        print(f"  处理用户ID: {user_id}")
        user_data = client.get_user_info_by_id(user_id)
        
        if user_data:
            data_list.append({
                '日期': current_date,
                '账号名': user_data['name'],
                '平台': '小红书',
                '粉丝数': user_data['followers']
            })
            print(f"  ✅ {user_data['name']}: {user_data['followers']:,} 粉丝")
        else:
            print(f"  ❌ 获取用户 {user_id} 失败")
        
        time.sleep(5)  # 避免请求过快
    
    return data_list

# 如果直接运行此脚本，使用默认配置
if __name__ == "__main__":
    # 默认配置
    user_list = [
        '549c2407e7798947f842c8af',  # nya酱的一生
        '5c6391880000000012009893'   # 影视飓风
    ]
    
    print("🚀 开始收集小红书粉丝数据...")
    data = get_redbook_data(user_list)
    
    if data:
        print(f"🎉 成功获取 {len(data)} 个用户的数据！")
        
        # 写入CSV文件
        csv_file = 'followers.csv'
        file_exists = os.path.exists(csv_file)
        
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            if data:
                fieldnames = ['日期', '账号名', '平台', '粉丝数']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                
                # 写入数据
                for row in data:
                    writer.writerow(row)
                
                print(f"📝 数据已写入 {csv_file}")
    else:
        print("❌ 未获取到任何数据")