import asyncio
import datetime
from bilibili_api import Credential, user
import time
import json
import random

# 从 cookie 文件读取凭据信息
def load_credential_from_cookie():
    with open('bilibili_cookie.json', 'r', encoding='utf-8') as f:
        cookies = json.load(f)
    
    cookie_dict = {}
    for cookie in cookies:
        cookie_dict[cookie['name']] = cookie['value']
    
    credential = Credential(
        sessdata=cookie_dict.get('SESSDATA', ''),
        bili_jct=cookie_dict.get('bili_jct', ''),
        buvid3=cookie_dict.get('buvid3', ''),
        dedeuserid=cookie_dict.get('DedeUserID', '')
    )
    
    return credential

async def get_bilibili_user_info(uid: str, credential, max_retries=3):
    """
    通过UID获取Bilibili用户的名称和粉丝数，带重试机制。
    """
    for attempt in range(max_retries):
        try:
            u = user.User(uid=int(uid), credential=credential)
            user_info = await u.get_user_info()
            username = user_info['name']
            
            relation_info = await u.get_relation_info()
            follower_count = relation_info['follower']
            
            return username, follower_count
            
        except Exception as e:
            error_msg = str(e)
            print(f"  -> 错误: 获取UID {uid} 信息失败: {e}")
            
            # 检查是否是网络错误（412状态码或其他网络相关错误）
            if ("412" in error_msg or "网络错误" in error_msg or 
                "状态码" in error_msg or "timeout" in error_msg.lower() or
                "connection" in error_msg.lower()):
                
                if attempt < max_retries - 1:  # 还有重试机会
                    retry_delay = random.uniform(3, 8)  # 随机等待3-8秒
                    print(f"  🔄 第{attempt + 1}次重试失败，{retry_delay:.1f}秒后进行第{attempt + 2}次尝试...")
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    print(f"  ❌ 重试{max_retries}次后仍然失败")
            else:
                # 非网络错误，直接返回失败
                break
    
    return None, None

# 导出函数：获取B站数据
async def get_bilibili_data(uids_list):
    """
    获取B站用户数据
    :param uids_list: UID列表
    :return: (成功数据列表, 失败UID列表)
    """
    print("🎬 开始获取Bilibili数据...")
    
    try:
        credential = load_credential_from_cookie()
    except Exception as e:
        print(f"❌ 读取Bilibili凭据失败: {e}")
        return [], uids_list  # 全部失败
    
    data_list = []
    failed_uids = []
    current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for uid in uids_list:
        print(f"  处理UID: {uid}")
        username, followers = await get_bilibili_user_info(uid, credential)
        
        if username and followers is not None:
            data_list.append({
                '日期': current_date,
                '账号名': username,
                '平台': 'bilibili',
                '粉丝数': followers
            })
            print(f"  ✅ {username}: {followers:,} 粉丝")
        else:
            print(f"  ❌ 获取UID {uid} 失败")
            failed_uids.append(uid)
            
        # 每个UID之间随机等待，避免请求过快
        await asyncio.sleep(random.uniform(2, 4))
    
    return data_list, failed_uids

# 如果直接运行此脚本，使用默认配置
if __name__ == "__main__":
    import csv
    import os
    
    # 默认配置
    UIDS_TO_CHECK = ['1885078', '946974']
    OUTPUT_FILENAME = 'followers.csv'
    
    async def main():
        print("--- 开始获取Bilibili账号粉丝数据 ---")
        
        data, failed_uids = await get_bilibili_data(UIDS_TO_CHECK)
        
        if data:
            # 写入CSV文件
            file_exists = os.path.exists(OUTPUT_FILENAME)
            
            with open(OUTPUT_FILENAME, 'a', newline='', encoding='utf-8') as f:
                fieldnames = ['日期', '账号名', '平台', '粉丝数']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                
                # 写入数据
                for row in data:
                    writer.writerow(row)
                
                print(f"✅ 数据已保存到 {OUTPUT_FILENAME}")
        else:
            print("❌ 未获取到任何数据")
    
    asyncio.run(main())
