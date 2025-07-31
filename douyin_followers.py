import asyncio
import httpx
import pandas as pd
from datetime import datetime
import json
import re
import sys
import os
import time
import random
from urllib.parse import quote, unquote

class DouyinFansCollectorEnhanced:
    def __init__(self, cookie):
        self.cookie = cookie
        self.session_id = self.extract_session_id(cookie)
        
    def extract_session_id(self, cookie):
        """从cookie中提取sessionid"""
        match = re.search(r'sessionid=([^;]+)', cookie)
        return match.group(1) if match else ''
    
    def get_headers(self, referer="https://www.douyin.com/"):
        """生成请求头"""
        return {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': referer,
            'Cookie': self.cookie,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        }
    
    async def get_user_by_search(self, unique_id):
        """通过搜索API获取用户信息"""
        try:
            print(f"🔍 通过搜索获取用户 {unique_id} 的信息...")
            
            # 搜索API
            search_url = "https://www.douyin.com/aweme/v1/web/discover/search/"
            params = {
                'device_platform': 'webapp',
                'aid': '6383',
                'channel': 'channel_pc_web',
                'search_channel': 'aweme_user_web',
                'keyword': unique_id,
                'search_source': 'normal_search',
                'query_correct_type': '1',
                'is_filter_search': '0',
                'from_group_id': '',
                'offset': '0',
                'count': '10'
            }
            
            async with httpx.AsyncClient(headers=self.get_headers(), timeout=30) as client:
                response = await client.get(search_url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # 解析搜索结果
                    if 'user_list' in data and data['user_list']:
                        for user in data['user_list']:
                            user_info = user.get('user_info', {})
                            if user_info.get('unique_id') == unique_id or user_info.get('short_id') == unique_id:
                                return self.format_user_data(user_info, unique_id)
                    
                    print(f"⚠️ 在搜索结果中未找到用户 {unique_id}")
                    return None
                else:
                    print(f"❌ 搜索请求失败，状态码: {response.status_code}")
                    return None
                    
        except Exception as e:
            print(f"❌ 搜索用户信息时出错: {str(e)}")
            return None
    
    async def get_user_by_profile_page(self, unique_id):
        """通过用户主页获取信息"""
        try:
            print(f"🌐 访问用户 {unique_id} 的主页...")
            
            profile_url = f"https://www.douyin.com/user/{unique_id}"
            
            async with httpx.AsyncClient(headers=self.get_headers(), timeout=30, follow_redirects=True) as client:
                response = await client.get(profile_url)
                
                if response.status_code == 200:
                    html_content = response.text
                    
                    # 尝试从页面中提取数据
                    user_data = self.extract_from_html(html_content, unique_id)
                    if user_data:
                        return user_data
                    
                    # 尝试从INITIAL_STATE中提取
                    user_data = self.extract_from_initial_state(html_content, unique_id)
                    if user_data:
                        return user_data
                    
                    print(f"⚠️ 无法从主页提取用户 {unique_id} 的数据")
                    return None
                else:
                    print(f"❌ 访问主页失败，状态码: {response.status_code}")
                    return None
                    
        except Exception as e:
            print(f"❌ 访问用户主页时出错: {str(e)}")
            return None
    
    def extract_from_html(self, html_content, unique_id):
        """从HTML中提取用户数据"""
        try:
            # 查找用户数据的多种模式
            patterns = [
                r'"followerCount":(\d+)',
                r'"user":{[^}]*"followerCount":(\d+)',
                r'"userInfo":{[^}]*"followerCount":(\d+)'
            ]
            
            nickname_patterns = [
                r'"nickname":"([^"]+)"',
                r'"user":{[^}]*"nickname":"([^"]+)"',
                r'"userInfo":{[^}]*"nickname":"([^"]+)"'
            ]
            
            follower_count = 0
            nickname = f"用户_{unique_id}"
            
            # 提取粉丝数
            for pattern in patterns:
                match = re.search(pattern, html_content)
                if match:
                    follower_count = int(match.group(1))
                    break
            
            # 提取昵称
            for pattern in nickname_patterns:
                match = re.search(pattern, html_content)
                if match:
                    nickname = match.group(1)
                    break
            
            if follower_count > 0:
                # 生成时间戳作为唯一编码
                timestamp = int(time.time() * 1000)  # 毫秒级时间戳
                
                return {
                    '编码': timestamp,
                    '账号名': nickname,
                    '平台': '抖音',
                    '粉丝数': follower_count,
                    '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '抖音号': unique_id  # 保留用于内部处理，但不输出到CSV
                }
            
            return None
            
        except Exception as e:
            print(f"⚠️ HTML解析出错: {str(e)}")
            return None
    
    def extract_from_initial_state(self, html_content, unique_id):
        """从INITIAL_STATE中提取数据"""
        try:
            # 查找INITIAL_STATE
            match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.+?});', html_content)
            if not match:
                return None
            
            try:
                initial_state = json.loads(match.group(1))
                
                # 递归查找用户信息
                user_info = self.find_user_in_state(initial_state, unique_id)
                if user_info:
                    return self.format_user_data(user_info, unique_id)
                    
            except json.JSONDecodeError:
                pass
            
            return None
            
        except Exception as e:
            print(f"⚠️ INITIAL_STATE解析出错: {str(e)}")
            return None
    
    def find_user_in_state(self, data, unique_id, path=""):
        """在状态数据中递归查找用户信息"""
        if isinstance(data, dict):
            # 检查是否是用户信息
            if ('followerCount' in data or 'follower_count' in data) and \
               ('uniqueId' in data or 'unique_id' in data or 'nickname' in data):
                user_unique_id = data.get('uniqueId') or data.get('unique_id')
                if user_unique_id == unique_id:
                    return data
            
            # 递归搜索
            for key, value in data.items():
                result = self.find_user_in_state(value, unique_id, f"{path}.{key}")
                if result:
                    return result
        
        elif isinstance(data, list):
            for i, item in enumerate(data):
                result = self.find_user_in_state(item, unique_id, f"{path}[{i}]")
                if result:
                    return result
        
        return None
    
    def format_user_data(self, user_info, unique_id):
        """格式化用户数据"""
        follower_count = user_info.get('followerCount') or user_info.get('follower_count', 0)
        nickname = user_info.get('nickname') or user_info.get('nick_name') or f"用户_{unique_id}"
        
        return {
            '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '账号名': nickname,
            '平台': '抖音',
            '粉丝数': follower_count,
            '抖音号': unique_id,  # 保留用于内部处理，但不输出到CSV
            '关注数': user_info.get('followingCount') or user_info.get('following_count', 0),
            '获赞数': user_info.get('totalFavorited') or user_info.get('total_favorited', 0),
            '作品数': user_info.get('awemeCount') or user_info.get('aweme_count', 0)
        }
    
    async def get_user_info(self, unique_id):
        """获取用户信息的主方法"""
        print(f"\n📊 正在处理抖音号: {unique_id}")
        
        # 方法1: 通过搜索API
        user_data = await self.get_user_by_search(unique_id)
        if user_data:
            return user_data
        
        # 等待一段时间
        await asyncio.sleep(2)
        
        # 方法2: 通过用户主页
        user_data = await self.get_user_by_profile_page(unique_id)
        if user_data:
            return user_data
        
        return None
    
    async def collect_fans_data(self, user_list):
        """批量收集粉丝数据"""
        all_data = []
        
        for unique_id in user_list:
            if not unique_id or unique_id.strip() == "":
                continue
            
            user_data = await self.get_user_info(unique_id)
            
            if user_data:
                all_data.append(user_data)
                print(f"✅ 成功获取 {user_data['账号名']} 的数据")
                print(f"   粉丝数: {user_data['粉丝数']:,}")
                if '关注数' in user_data and user_data['关注数'] > 0:
                    print(f"   关注数: {user_data['关注数']:,}")
                if '作品数' in user_data and user_data['作品数'] > 0:
                    print(f"   作品数: {user_data['作品数']:,}")
            else:
                print(f"❌ 获取抖音号 {unique_id} 的数据失败")
                all_data.append({
                    '日期': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '账号名': f"获取失败_{unique_id}",
                    '平台': '抖音',
                    '粉丝数': 0,
                    '抖音号': unique_id,
                    '备注': '数据获取失败'
                })
            
            # 随机延迟
            delay = random.uniform(3, 6)
            print(f"⏱️ 等待 {delay:.1f} 秒...")
            await asyncio.sleep(delay)
        
        return all_data
    
    def save_to_csv(self, data, filename='followers.csv'):
        """保存数据到CSV文件"""
        if not data:
            print("❌ 没有数据可保存")
            return
        # 检查文件是否存在,如果存在则读取已有数据
        try:
            if os.path.exists(filename):
                existing_df = pd.read_csv(filename, encoding='utf-8-sig')
                print(f"\n📖 从 {filename} 读取已有数据...")
            else:
                existing_df = pd.DataFrame(columns=['日期', '账号名', '平台', '粉丝数'])
                print(f"\n📝 创建新的数据文件 {filename}")
        except Exception as e:
            print(f"⚠️ 读取已有数据时出错: {str(e)}")
            existing_df = pd.DataFrame(columns=['日期', '账号名', '平台', '粉丝数'])

        # 将新数据转换为DataFrame
        df = pd.DataFrame(data)
        output_columns = ['日期', '账号名', '平台', '粉丝数']
        df_output = df[output_columns]
        
        # 合并新旧数据
        combined_df = pd.concat([existing_df, df_output], ignore_index=True)
        
        # 保存合并后的数据
        combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n✅ 数据已追加保存到 {filename}")
        
        # 统计信息
        successful_data = [item for item in data if item['粉丝数'] > 0]
        print(f"\n📊 本次统计信息:")
        print(f"   总账号数: {len(data)}")
        print(f"   成功获取: {len(successful_data)}")
        print(f"   失败数量: {len(data) - len(successful_data)}")
        
        if successful_data:
            total_fans = sum(item['粉丝数'] for item in successful_data)
            avg_fans = total_fans // len(successful_data) if successful_data else 0
            print(f"   总粉丝数: {total_fans:,}")
            print(f"   平均粉丝数: {avg_fans:,}")
        
        print(f"\n📋 最新数据预览:")
        print(df_output.head().to_string(index=False))

# 在文件开头的导入部分后添加cookie读取函数
def load_cookie_from_json(cookie_file='douyin_cookie.json'):
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

async def main():
    # 从JSON文件读取cookie
    cookie = load_cookie_from_json('douyin_cookie.json')
    
    if not cookie:
        print("❌ 无法获取cookie信息，程序退出")
        return
    
    user_list = ["357368605", "superslow"]
    
    collector = DouyinFansCollectorEnhanced(cookie=cookie)
    
    print("🚀 开始收集抖音粉丝数据（增强版）...")
    print(f"📋 待处理抖音号: {', '.join(user_list)}")
    
    fans_data = await collector.collect_fans_data(user_list)
    collector.save_to_csv(fans_data, 'followers.csv')
    
    print("\n🎉 数据收集完成！")

if __name__ == "__main__":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    asyncio.run(main())