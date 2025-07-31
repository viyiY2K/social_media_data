import requests
import pandas as pd
import time
import os
from datetime import datetime
import json
import re

class WeiboFollowersSimple:
    def __init__(self, cookie=""):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://weibo.com/',
            'Cookie': cookie
        })
    
    def get_user_info(self, uid):
        """获取微博用户信息"""
        try:
            print(f"📊 正在获取微博用户 {uid} 的信息...")
            
            # 尝试多个API端点
            urls = [
                f"https://weibo.com/ajax/profile/info?uid={uid}",
                f"https://m.weibo.cn/api/container/getIndex?type=uid&value={uid}",
                f"https://weibo.com/u/{uid}"
            ]
            
            for url in urls:
                try:
                    response = self.session.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        if 'application/json' in response.headers.get('content-type', ''):
                            data = response.json()
                            return self.parse_json_response(data, uid)
                        else:
                            return self.parse_html_response(response.text, uid)
                            
                except Exception as e:
                    print(f"⚠️ 尝试URL {url} 失败: {str(e)}")
                    continue
            
            return None
            
        except Exception as e:
            print(f"❌ 获取用户 {uid} 信息时出错: {str(e)}")
            return None
    
    def parse_json_response(self, data, uid):
        """解析JSON响应"""
        try:
            # 尝试不同的数据结构
            user_info = None
            
            if 'data' in data and 'user' in data['data']:
                user_info = data['data']['user']
            elif 'data' in data and 'userInfo' in data['data']:
                user_info = data['data']['userInfo']
            elif 'userInfo' in data:
                user_info = data['userInfo']
            
            if user_info:
                username = user_info.get('screen_name', f'用户_{uid}')
                followers_count = user_info.get('followers_count', 0)
                
                return {
                    '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '账号名': username,
                    '平台': '微博',
                    '粉丝数': followers_count
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ 解析JSON响应失败: {str(e)}")
            return None
    
    def parse_html_response(self, html, uid):
        """解析HTML响应"""
        try:
            # 从HTML中提取用户信息
            username_match = re.search(r'"screen_name":"([^"]+)"', html)
            followers_match = re.search(r'"followers_count":(\d+)', html)
            
            if username_match and followers_match:
                username = username_match.group(1)
                followers_count = int(followers_match.group(1))
                
                return {
                    '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '账号名': username,
                    '平台': '微博',
                    '粉丝数': followers_count
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ 解析HTML响应失败: {str(e)}")
            return None
    
    def collect_followers_data(self, uid_list):
        """批量收集粉丝数据"""
        all_data = []
        
        for uid in uid_list:
            user_data = self.get_user_info(uid)
            
            if user_data:
                all_data.append(user_data)
                print(f"✅ 成功获取 {user_data['账号名']} 的数据")
                print(f"   粉丝数: {user_data['粉丝数']:,}")
            else:
                print(f"❌ 获取用户 {uid} 的数据失败")
                all_data.append({
                    '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '账号名': f"获取失败_{uid}",
                    '平台': '微博',
                    '粉丝数': 0
                })
            
            time.sleep(3)  # 延迟3秒
        
        return all_data
    
    def save_to_csv(self, data, filename='followers.csv'):
        """保存数据到CSV文件（追加模式）"""
        if not data:
            print("❌ 没有数据可保存")
            return
        
        import csv
        
        try:
            file_exists = os.path.exists(filename)
            
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                fieldnames = ['日期', '账号名', '平台', '粉丝数']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                    print(f"\n📝 创建新的数据文件 {filename}")
                else:
                    print(f"\n📖 向 {filename} 追加数据...")
                
                # 写入数据
                for row in data:
                    writer.writerow(row)
                
                print(f"\n✅ 数据已追加保存到 {filename}")
            
            successful_data = [item for item in data if item['粉丝数'] > 0]
            print(f"\n📊 本次统计信息:")
            print(f"   总账号数: {len(data)}")
            print(f"   成功获取: {len(successful_data)}")
            print(f"   失败数量: {len(data) - len(successful_data)}")
            
        except Exception as e:
            print(f"❌ 保存数据时出错: {str(e)}")

def load_cookie_from_json(cookie_file='weibo_cookie.json'):
    """从JSON文件中读取cookie并转换为字符串格式"""
    try:
        if not os.path.exists(cookie_file):
            print(f"❌ 找不到cookie文件: {cookie_file}")
            return ""
            
        with open(cookie_file, 'r', encoding='utf-8') as f:
            cookies_data = json.load(f)
        
        # 转换为cookie字符串
        cookie_pairs = []
        for cookie in cookies_data:
            name = cookie.get('name', '')
            value = cookie.get('value', '')
            if name and value:
                cookie_pairs.append(f"{name}={value}")
        
        cookie_string = '; '.join(cookie_pairs)
        print(f"✅ 成功从 {cookie_file} 读取cookie信息")
        return cookie_string
        
    except Exception as e:
        print(f"❌ 读取cookie文件失败: {str(e)}")
        return ""

def get_weibo_data(uid_list, cookie_file='weibo_cookie.json'):
    """获取微博数据的统一接口函数"""
    if not uid_list:
        print("⚠️ 微博用户ID列表为空")
        return []
    
    try:
        print("🐦 开始获取微博数据...")
        
        # 从JSON文件读取cookie
        cookie = load_cookie_from_json(cookie_file)
        
        if not cookie:
            print("❌ 无法获取微博cookie信息")
            return []
        
        collector = WeiboFollowersSimple(cookie=cookie)
        
        print(f"📋 待处理用户ID: {', '.join(uid_list)}")
        followers_data = collector.collect_followers_data(uid_list)
        
        print(f"✅ 微博数据获取完成，共 {len(followers_data)} 条记录")
        return followers_data
        
    except Exception as e:
        print(f"❌ 获取微博数据时出错: {str(e)}")
        return []

def main():
    """主函数，用于独立运行时的默认配置"""
    # 默认配置，仅在独立运行时使用
    default_uid_list = ['1746383931', '1044980795']
    
    print("🚀 开始收集微博粉丝数据（独立运行模式）...")
    
    followers_data = get_weibo_data(default_uid_list)
    
    if followers_data:
        # 保存到CSV
        collector = WeiboFollowersSimple(cookie="")  # 创建实例用于调用save_to_csv方法
        collector.save_to_csv(followers_data, 'followers.csv')
        print("\n🎉 微博数据收集完成！")
    else:
        print("\n❌ 未获取到任何数据")

if __name__ == "__main__":
    main()