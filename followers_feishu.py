import asyncio
import time
import requests
import json
import pandas as pd
import os
import asyncio

# 导入各平台的数据获取函数
from bilibili_followers import get_bilibili_data
from youtube_followers import get_youtube_data
from redbook_followers import get_redbook_data
from douyin_followers import DouyinFansCollectorEnhanced, load_cookie_from_json
from weibo_followers import get_weibo_data
from wechat_followers import get_wechat_data
from zhihu_followers import get_zhihu_data  

# --- 统一配置区 ---
# 添加 bilibili 的 uid
BILIBILI_UIDS = [
    '18175054', # 枝因
    # '1885078' # nya 酱的一生
]
#添加 YouTube 的主页链接（注意是 about 页）
YOUTUBE_CHANNELS = [
    #'https://www.youtube.com/@mediastorm6801/about', # 影视飓风
    #'https://www.youtube.com/@linhlan19774/about', # nya 酱的一生
]
# 添加小红书账号主页 URL 中的用户 ID
REDBOOK_USER_IDS = [
    '609401890000000001009646', # 枝因
    # '549c2407e7798947f842c8af', # nya 酱的一生
    # '609401890000000001009646' # 影视飓风
]
# 添加抖音号
DOUYIN_USER_IDS = [
    # '357368605', # nya 酱的一生
    # 'superslow' # 影视飓风
]
# 添加微博用户ID
WEIBO_USER_IDS = [
    '7737430801', #枝因
    # '1746383931' # nya 酱的一生
]

# 添加微信公众号配置（可选，因为微信公众号通常只有一个）
WECHAT_ACCOUNTS = [
    # 微信公众号不需要特定ID，登录后自动获取当前账号数据
]

# 添加知乎用户slug
ZHIHU_USER_SLUGS = [
    'zhi-yin-233', # 枝因
    # 'nya-jiang-de-yi-sheng' # nya 酱的一生
]

# 飞书配置
FEISHU_APP_ID = "your_app_id"          # 飞书应用ID
FEISHU_APP_SECRET = "your_app_secret"  # 飞书应用密钥
FEISHU_APP_TOKEN = "your_app_token"    # 飞书应用令牌
FEISHU_TABLE_ID = "your_table_id"      # 飞书多维表格子表ID

# 输出文件配置
OUTPUT_FILENAME = 'data/followers.csv'
# --- 配置区结束 ---

def get_feishu_access_token():
    """获取飞书访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        result = response.json()
        
        if result.get('code') == 0:
            return result.get('tenant_access_token')
        else:
            print(f"❌ 获取飞书访问令牌失败: {result.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ 获取飞书访问令牌异常: {e}")
        return None

def write_to_feishu(data_list, access_token):
    """写入飞书多维表格"""
    if not data_list or not access_token:
        print("❌ 数据为空或访问令牌无效，跳过飞书写入")
        return False
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{FEISHU_TABLE_ID}/records/batch_create"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    current_timestamp = int(time.time() * 1000)
    
    records = []
    for item in data_list:
        records.append({
            "fields": {
                "日期": current_timestamp,
                "账号名": str(item['账号名']),
                "平台": str(item['平台']),
                "粉丝数": int(item['粉丝数'])
            }
        })
    
    try:
        print(f"📝 正在向飞书写入 {len(records)} 条记录...")
        response = requests.post(url, json={"records": records}, headers=headers)
        result = response.json()
        
        if result.get('code') == 0:
            print("✅ 成功写入飞书多维表格！")
            return True
        else:
            print(f"❌ 写入飞书失败: {result.get('msg')}")
            return False
    except Exception as e:
        print(f"❌ 写入飞书异常: {e}")
        return False

def save_to_csv(data, filename='followers.csv'):
    """保存数据到CSV文件（追加模式）"""
    if not data:
        print("❌ 没有数据可保存到CSV")
        return

    output_columns = ['日期', '账号名', '平台', '粉丝数']
    new_df = pd.DataFrame(data)[output_columns]

    try:
        if os.path.exists(filename):
            print(f"\n📖 正在向 {filename} 追加数据...")
            existing_df = pd.read_csv(filename, encoding='utf-8-sig')
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            print(f"\n📝 正在创建新的数据文件 {filename}...")
            combined_df = new_df
        
        combined_df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ 数据已保存到 {filename}")
        
        # 统计信息
        successful_data = [item for item in data if item['粉丝数'] > 0]
        print(f"\n📊 本次统计信息:")
        print(f"   总账号数: {len(data)}")
        print(f"   成功获取: {len(successful_data)}")
        print(f"   失败数量: {len(data) - len(successful_data)}") 
        
        print(f"\n📋 最新数据预览:")
        print(new_df.head().to_string(index=False))

    except Exception as e:
        print(f"❌ 保存到CSV文件时出错: {e}")

# --- 错误代码定义 ---
ERROR_CODES = {
    'BILIBILI_001': 'Bilibili数据获取失败 - 网络连接错误',
    'BILIBILI_002': 'Bilibili数据获取失败 - 用户ID无效',
    'BILIBILI_003': 'Bilibili数据获取失败 - Cookie过期或无效',
    'BILIBILI_004': 'Bilibili数据获取失败 - 其他未知错误',
    
    'YOUTUBE_001': 'YouTube数据获取失败 - 网络连接错误',
    'YOUTUBE_002': 'YouTube数据获取失败 - 频道链接无效',
    'YOUTUBE_003': 'YouTube数据获取失败 - 页面解析错误',
    'YOUTUBE_004': 'YouTube数据获取失败 - 其他未知错误',
    
    'REDBOOK_001': '小红书数据获取失败 - 网络连接错误',
    'REDBOOK_002': '小红书数据获取失败 - 用户ID无效',
    'REDBOOK_003': '小红书数据获取失败 - Cookie过期或无效',
    'REDBOOK_004': '小红书数据获取失败 - 其他未知错误',
    
    'DOUYIN_001': '抖音数据获取失败 - 网络连接错误',
    'DOUYIN_002': '抖音数据获取失败 - 用户ID无效',
    'DOUYIN_003': '抖音数据获取失败 - Cookie过期或无效',
    'DOUYIN_004': '抖音数据获取失败 - 其他未知错误',
    
    'WEIBO_001': '微博数据获取失败 - 网络连接错误',
    'WEIBO_002': '微博数据获取失败 - 用户ID无效',
    'WEIBO_003': '微博数据获取失败 - Cookie过期或无效',
    'WEIBO_004': '微博数据获取失败 - 其他未知错误',
    
    'WECHAT_001': '微信公众号数据获取失败 - 网络连接错误',
    'WECHAT_002': '微信公众号数据获取失败 - 登录状态无效',
    'WECHAT_003': '微信公众号数据获取失败 - 页面解析错误',
    'WECHAT_004': '微信公众号数据获取失败 - 其他未知错误',
    
    'ZHIHU_001': '知乎数据获取失败 - 网络连接错误',
    'ZHIHU_002': '知乎数据获取失败 - 用户slug无效',
    'ZHIHU_003': '知乎数据获取失败 - Cookie过期或无效',
    'ZHIHU_004': '知乎数据获取失败 - 其他未知错误',
}

def print_error_with_code(error_code, additional_info=""):
    """打印带错误代码的错误信息"""
    error_msg = ERROR_CODES.get(error_code, f"未知错误代码: {error_code}")
    print(f"❌ [{error_code}] {error_msg}")
    if additional_info:
        print(f"   详细信息: {additional_info}")
    return error_code

def get_douyin_data(user_ids):
    """获取抖音数据（同步包装函数）"""
    if not user_ids:
        print("⚠️ 抖音用户ID列表为空")
        return [], []
    
    try:
        print("🎵 开始获取抖音数据...")
        
        # 从JSON文件读取cookie
        cookie = load_cookie_from_json('douyin_cookie.json')
        
        if not cookie:
            error_code = print_error_with_code('DOUYIN_003', "无法读取cookie文件")
            return [], [error_code]
        
        # 创建收集器实例
        collector = DouyinFansCollectorEnhanced(cookie=cookie)
        
        # 使用asyncio运行异步函数
        douyin_data = asyncio.run(collector.collect_fans_data(user_ids))
        
        print(f"✅ 抖音数据获取完成，共 {len(douyin_data)} 条记录")
        return douyin_data, []
        
    except ConnectionError as e:
        error_code = print_error_with_code('DOUYIN_001', str(e))
        return [], [error_code]
    except ValueError as e:
        error_code = print_error_with_code('DOUYIN_002', str(e))
        return [], [error_code]
    except Exception as e:
        error_code = print_error_with_code('DOUYIN_004', str(e))
        return [], [error_code]

def get_wechat_data_wrapper():
    """获取微信公众号数据（同步包装函数）"""
    try:
        print("📱 开始获取微信公众号数据...")
        
        # 使用asyncio运行异步函数
        wechat_data, failed_wechat = asyncio.run(get_wechat_data())
        
        print(f"✅ 微信公众号数据获取完成，共 {len(wechat_data)} 条记录")
        return wechat_data, failed_wechat
        
    except ConnectionError as e:
        error_code = print_error_with_code('WECHAT_001', str(e))
        return [], [error_code]
    except ValueError as e:
        error_code = print_error_with_code('WECHAT_002', str(e))
        return [], [error_code]
    except Exception as e:
        error_code = print_error_with_code('WECHAT_004', str(e))
        return [], [error_code]

def get_zhihu_data_wrapper(user_slugs):
    """获取知乎数据（同步包装函数）"""
    if not user_slugs:
        print("⚠️ 知乎用户slug列表为空")
        return [], []
    
    try:
        print("🔍 开始获取知乎数据...")
        
        # 直接使用导入的异步函数
        zhihu_data, failed_zhihu = asyncio.run(get_zhihu_data(user_slugs))
        
        print(f"✅ 知乎数据获取完成，共 {len(zhihu_data)} 条记录")
        return zhihu_data, failed_zhihu
        
    except ConnectionError as e:
        error_code = print_error_with_code('ZHIHU_001', str(e))
        return [], [error_code]
    except ValueError as e:
        error_code = print_error_with_code('ZHIHU_002', str(e))
        return [], [error_code]
    except Exception as e:
        error_code = print_error_with_code('ZHIHU_004', str(e))
        return [], [error_code]

def main():
    """主函数，执行整个流程（同步版本）"""
    print("🚀 开始获取多平台粉丝数据并写入飞书...")
    
    all_data = []
    failed_accounts = {}  # 记录失败的账户信息
    error_summary = {}    # 记录各平台的错误代码
    
    # 获取各平台数据
    print("\n=== 开始获取各平台数据 ===")
    
    # 获取Bilibili数据
    if BILIBILI_UIDS:
        try:
            print("🎬 开始获取Bilibili数据...")
            bilibili_data, failed_bilibili = asyncio.run(get_bilibili_data(BILIBILI_UIDS))
            all_data.extend(bilibili_data)
            if failed_bilibili:
                failed_accounts['bilibili'] = failed_bilibili
        except ConnectionError as e:
            error_code = print_error_with_code('BILIBILI_001', str(e))
            error_summary['bilibili'] = [error_code]
        except ValueError as e:
            error_code = print_error_with_code('BILIBILI_002', str(e))
            error_summary['bilibili'] = [error_code]
        except Exception as e:
            error_code = print_error_with_code('BILIBILI_004', str(e))
            error_summary['bilibili'] = [error_code]
    
    # 获取YouTube数据
    if YOUTUBE_CHANNELS:
        try:
            print("📺 开始获取YouTube数据...")
            youtube_data = get_youtube_data(YOUTUBE_CHANNELS)
            all_data.extend(youtube_data)
        except ConnectionError as e:
            error_code = print_error_with_code('YOUTUBE_001', str(e))
            error_summary['youtube'] = [error_code]
        except ValueError as e:
            error_code = print_error_with_code('YOUTUBE_002', str(e))
            error_summary['youtube'] = [error_code]
        except Exception as e:
            error_code = print_error_with_code('YOUTUBE_004', str(e))
            error_summary['youtube'] = [error_code]
    
    # 获取小红书数据
    if REDBOOK_USER_IDS:
        try:
            print("📖 开始获取小红书数据...")
            redbook_data = get_redbook_data(REDBOOK_USER_IDS)
            all_data.extend(redbook_data)
        except ConnectionError as e:
            error_code = print_error_with_code('REDBOOK_001', str(e))
            error_summary['redbook'] = [error_code]
        except ValueError as e:
            error_code = print_error_with_code('REDBOOK_002', str(e))
            error_summary['redbook'] = [error_code]
        except Exception as e:
            error_code = print_error_with_code('REDBOOK_004', str(e))
            error_summary['redbook'] = [error_code]
    
    # 获取抖音数据
    if DOUYIN_USER_IDS:
        douyin_data, douyin_errors = get_douyin_data(DOUYIN_USER_IDS)
        all_data.extend(douyin_data)
        if douyin_errors:
            error_summary['douyin'] = douyin_errors
    
    # 获取微博数据
    if WEIBO_USER_IDS:
        try:
            print("🐦 开始获取微博数据...")
            weibo_data = get_weibo_data(WEIBO_USER_IDS)
            all_data.extend(weibo_data)
        except ConnectionError as e:
            error_code = print_error_with_code('WEIBO_001', str(e))
            error_summary['weibo'] = [error_code]
        except ValueError as e:
            error_code = print_error_with_code('WEIBO_002', str(e))
            error_summary['weibo'] = [error_code]
        except Exception as e:
            error_code = print_error_with_code('WEIBO_004', str(e))
            error_summary['weibo'] = [error_code]
    
    # 获取微信公众号数据
    if WECHAT_ACCOUNTS is not None:  # 即使列表为空也尝试获取
        # 获取微信公众号数据
        if WECHAT_ACCOUNTS is not None:
            wechat_data, wechat_errors = get_wechat_data_wrapper()
            all_data.extend(wechat_data)
            if wechat_errors:
                # 检查是否是错误代码
                if any(error.startswith('WECHAT_') for error in wechat_errors):
                    error_summary['wechat'] = wechat_errors
                else:
                    failed_accounts['wechat'] = wechat_errors
            
            # 添加微信公众号特殊检查
            wechat_success_count = len([item for item in wechat_data if item['平台'] == '微信公众号' and item['粉丝数'] > 0])
            if wechat_success_count == 0 and not wechat_errors:
                print("⚠️ 微信公众号数据获取可能存在问题（无数据且无错误）")
                failed_accounts['wechat'] = ['登录状态异常或数据获取失败']
    
    # 获取知乎数据
    if ZHIHU_USER_SLUGS:
        zhihu_data, zhihu_errors = get_zhihu_data_wrapper(ZHIHU_USER_SLUGS)
        all_data.extend(zhihu_data)
        if zhihu_errors:
            # 检查是否是错误代码
            if any(error.startswith('ZHIHU_') for error in zhihu_errors):
                error_summary['zhihu'] = zhihu_errors
            else:
                failed_accounts['zhihu'] = zhihu_errors
    
    if not all_data:
        print("\n❌ 未获取到任何数据")
        # 输出详细的错误信息
        if error_summary:
            print("\n🔍 详细错误信息:")
            for platform, errors in error_summary.items():
                print(f"   {platform}: {', '.join(errors)}")
        return
    
    print(f"\n📊 总共获取到 {len(all_data)} 条数据")
    
    # 过滤掉失败的数据，只保留成功的数据用于写入飞书
    successful_data = [item for item in all_data if item['粉丝数'] > 0]
    
    # 按平台分组显示统计
    platform_stats = {}
    for item in successful_data:
        platform = item['平台']
        if platform not in platform_stats:
            platform_stats[platform] = {'count': 0, 'total_fans': 0}
        platform_stats[platform]['count'] += 1
        platform_stats[platform]['total_fans'] += item['粉丝数']
    
    print("\n📈 各平台统计:")
    for platform, stats in platform_stats.items():
        print(f"   {platform}: {stats['count']} 个账号，总粉丝数 {stats['total_fans']:,}")
    
    # 写入飞书（只写入成功的数据）
    print("\n=== 开始写入飞书 ===")
    access_token = get_feishu_access_token()
    if access_token and successful_data:
        print("✅ 飞书访问令牌获取成功")
        feishu_success = write_to_feishu(successful_data, access_token)
    else:
        if not successful_data:
            print("⚠️ 没有成功的数据可写入飞书")
        else:
            print("⚠️ 未能写入飞书，因为无法获取访问令牌")
        feishu_success = False
    
    # 保存到CSV（包含所有数据，包括失败的）
    print("\n=== 开始保存到CSV ===")
    save_to_csv(all_data, OUTPUT_FILENAME)
    
    # 最终总结
    print("\n=== 任务完成总结 ===")
    print(f"📊 数据获取: 成功获取 {len(successful_data)} 条记录")
    
    # 报告失败的账户
    if failed_accounts:
        print("\n⚠️ 获取失败的账户:")
        for platform, failed_list in failed_accounts.items():
            print(f"   {platform}: {', '.join(failed_list)}")
    
    # 报告错误代码
    if error_summary:
        print("\n🔍 平台错误代码:")
        for platform, errors in error_summary.items():
            print(f"   {platform}: {', '.join(errors)}")
    
    print(f"📝 CSV保存: ✅ 已保存到 {OUTPUT_FILENAME}")
    print(f"🚀 飞书写入: {'✅ 成功' if feishu_success else '❌ 失败'}")
    
    if successful_data:
        print("\n🎉 多平台数据收集完成！")
    else:
        print("\n⚠️ 未获取到任何有效数据")
    
    # 输出详细状态信息供monitor_bot检查
    print("\n=== 状态信息 ===")
    if successful_data:
        platforms = set(item['平台'] for item in successful_data)
        print(f"STATUS:SUCCESS - 成功获取平台: {', '.join(platforms)}")
        
        # 检查微信公众号是否成功
        wechat_data = [item for item in successful_data if item['平台'] == '微信公众号']
        if not wechat_data and 'wechat' in (list(failed_accounts.keys()) + list(error_summary.keys())):
            print("STATUS:WARNING - 微信公众号数据获取失败")
    else:
        print("STATUS:FAILED - 未获取到任何数据")
        
        # 特别检查微信公众号失败情况
        if 'wechat' in failed_accounts or 'wechat' in error_summary:
            print("STATUS:WECHAT_FAILED - 微信公众号登录状态异常或数据获取失败")
    
    # 返回状态信息供外部调用
    return {
        'successful_data': successful_data,
        'failed_accounts': failed_accounts,
        'error_summary': error_summary,
        'feishu_success': feishu_success if 'feishu_success' in locals() else False
    }

if __name__ == "__main__":
    main()  # 直接调用同步函数，不使用 asyncio.run()
