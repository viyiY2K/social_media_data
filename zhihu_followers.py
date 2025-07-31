#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知乎粉丝数获取工具 - 优化版
基于MediaCrawler的实现思路
"""

import asyncio
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from playwright.async_api import async_playwright, BrowserContext, Page
except ImportError:
    print("请先安装playwright: pip install playwright")
    print("然后安装浏览器: playwright install")
    sys.exit(1)

class ZhihuOptimizedCrawler:
    def __init__(self):
        self.browser_context: Optional[BrowserContext] = None
        self.context_page: Optional[Page] = None
        self.user_data_dir = Path.cwd() / "browser_data" / "zhihu"
        
    async def init_browser(self, headless: bool = False):
        """初始化浏览器"""
        playwright = await async_playwright().start()
        
        # 确保用户数据目录存在
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        
        # 浏览器启动参数
        browser_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-web-security",
            "--allow-running-insecure-content",
            "--no-first-run",
            "--disable-features=VizDisplayCompositor"
        ]
        
        # 启动持久化浏览器上下文
        self.browser_context = await playwright.chromium.launch_persistent_context(
            user_data_dir=str(self.user_data_dir),
            headless=headless,
            args=browser_args,
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="zh-CN",
            timezone_id="Asia/Shanghai"
        )
        
        # 获取页面
        if self.browser_context.pages:
            self.context_page = self.browser_context.pages[0]
        else:
            self.context_page = await self.browser_context.new_page()
            
        # 注入反检测脚本
        await self.context_page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5],
            });
            window.chrome = { runtime: {} };
        """)
        
        print("✅ 浏览器初始化完成")
        
    async def login(self) -> bool:
        """登录知乎"""
        try:
            await self.context_page.goto("https://www.zhihu.com")
            await asyncio.sleep(3)
            
            # 检查是否已登录
            if await self._is_logged_in():
                print("✅ 已登录")
                return True
                
            print("🔐 需要登录，请在浏览器中完成登录...")
            print("推荐使用手机号+短信验证码登录")
            
            # 等待用户登录
            while True:
                await asyncio.sleep(5)
                if await self._is_logged_in():
                    print("✅ 登录成功！")
                    return True
                    
        except Exception as e:
            print(f"❌ 登录失败: {e}")
            return False
            
    async def _is_logged_in(self) -> bool:
        """检查是否已登录"""
        try:
            selectors = [
                ".AppHeader-userInfo",
                ".Avatar",
                "[data-za-detail-view-id='2267']"
            ]
            
            for selector in selectors:
                element = await self.context_page.query_selector(selector)
                if element:
                    return True
                    
            return False
        except:
            return False
            
    async def get_user_followers(self, user_slug: str) -> Dict:
        """获取用户粉丝数 - 优化版"""
        try:
            url = f"https://www.zhihu.com/people/{user_slug}"
            print(f"📍 访问: {url}")
            
            await self.context_page.goto(url, wait_until="networkidle")
            await asyncio.sleep(3)  # 等待数据加载
            
            # 获取用户名
            username = await self._get_username()
            
            # 获取粉丝数 - 使用优化的NumberBoard方法
            followers = await self._get_followers_count_optimized()
            
            return {
                "username": username,
                "followers": followers,
                "platform": "知乎",
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
        except Exception as e:
            print(f"❌ 获取用户信息失败: {e}")
            return {}
            
    async def _get_followers_count_optimized(self) -> int:
        """获取粉丝数 - 优化版（专注NumberBoard方法）"""
        try:
            # 等待NumberBoard元素加载
            await self.context_page.wait_for_selector(".NumberBoard-item", timeout=10000)
            
            # 获取所有NumberBoard元素
            elements = await self.context_page.query_selector_all(".NumberBoard-item")
            
            for element in elements:
                text = await element.inner_text()
                # 查找包含"关注者"的元素
                if "关注者" in text:
                    followers_count = self._parse_followers_text(text)
                    if followers_count > 0:
                        return followers_count
                        
            # 如果NumberBoard方法失败，使用备用方法
            print("⚠️ NumberBoard方法未找到关注者，尝试备用方法...")
            return await self._get_followers_fallback()
            
        except Exception as e:
            print(f"⚠️ NumberBoard方法出错: {e}，尝试备用方法...")
            return await self._get_followers_fallback()
            
    async def _get_followers_fallback(self) -> int:
        """备用方法：JavaScript搜索"""
        try:
            js_result = await self.context_page.evaluate("""
                () => {
                    const elements = document.querySelectorAll('*');
                    for (let element of elements) {
                        const text = element.textContent || '';
                        if (text.includes('关注者') && text.length < 50) {
                            return text.trim();
                        }
                    }
                    return null;
                }
            """)
            
            if js_result:
                return self._parse_followers_text(js_result)
            return 0
        except:
            return 0
            
    def _parse_followers_text(self, text: str) -> int:
        """解析粉丝数文本，支持万、千等单位"""
        try:
            # 清理文本
            text = text.strip().replace(',', '').replace('，', '').replace('\n', ' ')
            
            # 匹配数字+单位的格式
            patterns = [
                r'([\d.]+)\s*万',  # X.X万
                r'([\d.]+)\s*千',  # X.X千
                r'(\d+)',         # 纯数字
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    number_str = match.group(1)
                    number = float(number_str)
                    
                    if '万' in text:
                        return int(number * 10000)
                    elif '千' in text:
                        return int(number * 1000)
                    else:
                        return int(number)
                        
            return 0
        except:
            return 0
            
    async def _get_username(self) -> str:
        """获取用户名"""
        try:
            # 从标题获取
            title = await self.context_page.title()
            if " - 知乎" in title:
                username = title.replace(" - 知乎", "").strip()
                # 清理括号内的消息提示
                username = self._clean_username(username)
                return username
                
            # 从元素获取
            selectors = [".ProfileHeader-name", ".UserLink-link"]
            for selector in selectors:
                element = await self.context_page.query_selector(selector)
                if element:
                    text = await element.inner_text()
                    if text.strip():
                        return self._clean_username(text.strip())
                        
            return "未知用户"
        except:
            return "未知用户"
            
    def _clean_username(self, username: str) -> str:
        """清理用户名中的消息提示等无关内容"""
        try:
            import re
            # 移除括号内的消息提示，如 "(2 封私信 / 4 条消息) 枝因" -> "枝因"
            cleaned = re.sub(r'\([^)]*封私信[^)]*\)\s*', '', username)
            # 移除其他可能的括号内容
            cleaned = re.sub(r'\([^)]*条消息[^)]*\)\s*', '', cleaned)
            # 移除多余的空格
            cleaned = cleaned.strip()
            return cleaned if cleaned else username
        except:
            return username
            
    def append_to_csv(self, data: Dict, filename: str = "followers.csv"):
        """追加数据到CSV文件"""
        try:
            # 检查文件是否存在
            file_exists = Path(filename).exists()
            
            with open(filename, "a", newline="", encoding="utf-8") as f:
                fieldnames = ["日期", "账号名", "平台", "粉丝数"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                
                # 写入数据
                writer.writerow({
                    "日期": data["date"],
                    "账号名": data["username"],
                    "平台": data["platform"],
                    "粉丝数": data["followers"]
                })
                
            print(f"✅ 数据已追加到 {filename}")
            
        except Exception as e:
            print(f"❌ 写入CSV失败: {e}")
            
    async def close(self):
        """关闭浏览器"""
        if self.browser_context:
            await self.browser_context.close()
            print("✅ 浏览器已关闭")

# 在文件末尾的 main() 函数之前添加这个导出函数

# 导出函数：获取知乎数据
async def get_zhihu_data(user_slugs):
    """
    获取知乎用户数据
    :param user_slugs: 用户slug列表
    :return: (成功数据列表, 失败账号列表)
    """
    print("🔍 开始获取知乎数据...")
    
    crawler = ZhihuOptimizedCrawler()
    data_list = []
    failed_accounts = []
    
    try:
        # 初始化浏览器
        await crawler.init_browser(headless=False)
        
        # 登录
        if not await crawler.login():
            print("❌ 知乎登录失败")
            return [], user_slugs
            
        # 获取用户数据
        for user_slug in user_slugs:
            print(f"🎯 处理知乎用户: {user_slug}")
            user_data = await crawler.get_user_followers(user_slug)
            
            if user_data and user_data.get("followers", 0) > 0:
                data_list.append({
                    '日期': user_data["date"],
                    '账号名': user_data["username"],
                    '平台': user_data["platform"],
                    '粉丝数': user_data["followers"]
                })
                print(f"✅ {user_data['username']}: {user_data['followers']:,} 粉丝")
            else:
                print(f"❌ 获取失败: {user_slug}")
                failed_accounts.append(user_slug)
                
            await asyncio.sleep(2)  # 避免请求过快
            
    except Exception as e:
        print(f"❌ 知乎数据获取出错: {e}")
        failed_accounts.extend(user_slugs)
    finally:
        await crawler.close()
        
    return data_list, failed_accounts

async def main():
    """主函数"""
    # 配置
    USER_SLUGS = ["zhi-yin-233"]  # 要获取的用户列表
    HEADLESS = False  # 是否无头模式
    CSV_FILENAME = "followers.csv"  # CSV文件名
    
    print("=== 知乎粉丝数获取工具（优化版）===")
    print(f"目标用户: {USER_SLUGS}")
    print(f"输出文件: {CSV_FILENAME}")
    print()
    
    crawler = ZhihuOptimizedCrawler()
    
    try:
        # 初始化浏览器
        await crawler.init_browser(headless=HEADLESS)
        
        # 登录
        if not await crawler.login():
            print("❌ 登录失败，退出程序")
            return
            
        # 获取用户数据
        success_count = 0
        for user_slug in USER_SLUGS:
            print(f"\n🎯 处理用户: {user_slug}")
            user_data = await crawler.get_user_followers(user_slug)
            
            if user_data and user_data.get("followers", 0) > 0:
                # 追加到CSV文件
                crawler.append_to_csv(user_data, CSV_FILENAME)
                print(f"✅ {user_data['username']}: {user_data['followers']:,} 粉丝")
                success_count += 1
            else:
                print(f"❌ 获取失败: {user_slug}")
                
            await asyncio.sleep(2)  # 避免请求过快
            
        print(f"\n🎉 完成！成功获取 {success_count}/{len(USER_SLUGS)} 个用户的数据")
        print(f"📄 数据已保存到: {CSV_FILENAME}")
            
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())