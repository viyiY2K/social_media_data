import yt_dlp
import time
from datetime import datetime

def get_youtube_channel_info(url_list):
    """
    使用 yt-dlp 获取频道元数据
    """
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False,
        'skip_download': True,
        'playlist_items': '0',
        'writeinfojson': False,
    }
    
    results = []
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for url in url_list:
            try:
                print(f"  处理频道: {url}")
                
                if '@' in url and not url.endswith('/about'):
                    url = url.rstrip('/') + '/about'
                
                info = ydl.extract_info(url, download=False)
                channel_name = info.get('channel')
                follower_count = info.get('channel_follower_count')
                
                if channel_name and follower_count is not None:
                    results.append({
                        'name': channel_name,
                        'followers': follower_count
                    })
                    print(f"  ✅ {channel_name}: {follower_count:,} 粉丝")
                else:
                    print(f"  ❌ 数据不完整")
                    
            except Exception as e:
                print(f"  ❌ 错误: {e}")
                
    return results

# 导出函数：获取YouTube数据
def get_youtube_data(channel_urls):
    """
    获取YouTube频道数据
    :param channel_urls: 频道URL列表
    :return: 数据列表
    """
    print("📺 开始获取YouTube数据...")
    
    channel_data = get_youtube_channel_info(channel_urls)
    
    data_list = []
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for item in channel_data:
        data_list.append({
            '日期': current_date,
            '账号名': item['name'],
            '平台': 'YouTube',
            '粉丝数': item['followers']
        })
    
    return data_list

# 如果直接运行此脚本，使用默认配置
if __name__ == "__main__":
    import csv
    import os
    
    # 默认配置
    CHANNEL_URLS = [
        'https://www.youtube.com/@mediastorm6801/about',
        'https://www.youtube.com/@linhlan19774/about'
    ]
    OUTPUT_CSV_FILE = 'followers.csv'
    
    print("=== YouTube 粉丝数获取工具 ===")
    
    data = get_youtube_data(CHANNEL_URLS)
    
    if data:
        # 写入CSV文件
        file_exists = os.path.exists(OUTPUT_CSV_FILE)
        
        with open(OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            fieldnames = ['日期', '账号名', '平台', '粉丝数']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # 如果文件不存在，写入表头
            if not file_exists:
                writer.writeheader()
            
            # 写入数据
            for row in data:
                writer.writerow(row)
            
            print(f"📝 数据已写入 {OUTPUT_CSV_FILE}")
        
        print(f"🎉 成功获取 {len(data)} 个频道的数据！")
    else:
        print("❌ 未获取到任何数据")

