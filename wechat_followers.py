#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信公众号粉丝数获取工具 - 精确元素定位版
基于MediaCrawler的实现思路
"""

import asyncio
import csv
import re
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from playwright.async_api import async_playwright, BrowserContext, Page
except ImportError:
    print("请先安装playwright: pip install playwright")
    print("然后安装浏览器: playwright install")
    sys.exit(1)

class WeChatMPCrawler:
    def __init__(self):
        self.browser_context: Optional[BrowserContext] = None
        self.context_page: Optional[Page] = None
        self.user_data_dir = Path.cwd() / "browser_data" / "wechat"
        
    async def init_browser(self, headless: bool = False):
        """初始化浏览器"""
        print("🔧 开始初始化浏览器...")
        playwright = await async_playwright().start()
        
        # 确保用户数据目录存在
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 用户数据目录: {self.user_data_dir}")
        
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
        
        print("🚀 启动浏览器上下文...")
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
            print("📄 使用现有页面")
        else:
            self.context_page = await self.browser_context.new_page()
            print("📄 创建新页面")
            
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
        """登录微信公众平台"""
        try:
            print("🌐 正在访问微信公众平台...")
            await self.context_page.goto("https://mp.weixin.qq.com")
            await asyncio.sleep(3)
            
            print("🔍 检查当前页面URL和标题...")
            current_url = self.context_page.url
            current_title = await self.context_page.title()
            print(f"   当前URL: {current_url}")
            print(f"   当前标题: {current_title}")
            
            # 检查是否已登录
            print("🔐 检查登录状态...")
            if await self._is_logged_in():
                print("✅ 已登录微信公众平台")
                return True
                
            print("🔐 需要登录微信公众平台，请扫描二维码...")
            print("请在浏览器中完成微信扫码登录")
            
            # 等待用户登录
            login_attempts = 0
            while True:
                await asyncio.sleep(5)
                login_attempts += 1
                print(f"⏳ 等待登录中... (第{login_attempts}次检查)")
                
                if await self._is_logged_in():
                    print("✅ 登录成功！")
                    return True
                    
                # 避免无限等待
                if login_attempts > 60:  # 5分钟后超时
                    print("⏰ 登录等待超时")
                    return False
                    
        except Exception as e:
            print(f"❌ 登录失败: {e}")
            return False
            
    async def _is_logged_in(self) -> bool:
        """检查是否已登录"""
        try:
            # 检查是否存在"总用户数"或"内容管理"文本
            login_check = await self.context_page.evaluate("""
                () => {
                    const bodyText = document.body.textContent || '';
                    return bodyText.includes('总用户数') || bodyText.includes('内容管理');
                }
            """)
            
            return login_check
        except Exception as e:
            print(f"❌ 检查登录状态时出错: {e}")
            return False
            
    async def get_account_followers(self, account_name: str = None) -> Dict:
        """获取公众号粉丝数 - 精确定位版"""
        try:
            print("\n🎯 开始精确获取公众号数据...")
            
            # 等待页面完全加载
            await asyncio.sleep(3)
            
            # 获取账号名称
            print("📝 获取账号名称...")
            account_name = await self._get_account_name_precise()
            print(f"   账号名称: {account_name}")
            
            # 获取总用户数
            print("📊 获取总用户数...")
            followers = await self._get_total_users_precise()
            print(f"   总用户数: {followers}")
            
            result = {
                "username": account_name,
                "followers": followers,
                "platform": "微信公众号",
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"✅ 数据获取完成: {result}")
            return result
            
        except Exception as e:
            print(f"❌ 获取公众号信息失败: {e}")
            import traceback
            print(f"详细错误信息: {traceback.format_exc()}")
            return {}
    
    async def _get_account_name_precise(self) -> str:
        """精确获取账号名称 - 基于具体元素定位"""
        try:
            print("📝 精确搜索账号名称...")
            
            # 根据新的JSON信息，精确定位mp_account_box元素
            account_name = await self.context_page.evaluate("""
                () => {
                    // 查找具有mp_account_box className的元素
                    const element = document.querySelector('.mp_account_box');
                    if (element) {
                        const innerText = element.innerText || '';
                        console.log('找到元素的innerText:', innerText);
                        
                        // 直接返回innerText
                        const trimmed = innerText.trim();
                        if (trimmed) {
                            console.log('提取到的账号名:', trimmed);
                            return trimmed;
                        }
                    }
                    
                    return '未找到账号名';
                }
            """)
            
            print(f"   提取到的账号名: {account_name}")
            return account_name or "未知账号"
            
        except Exception as e:
            print(f"❌ 获取账号名称失败: {e}")
            return "获取失败"
    
    async def _get_total_users_precise(self) -> int:
        """精确获取总用户数 - 基于JSON分析"""
        try:
            print("📊 精确搜索'总用户数'...")
            
            # 使用JavaScript精确查找"总用户数"相关的数据
            user_data = await self.context_page.evaluate("""
                () => {
                    const results = [];
                    const elements = document.querySelectorAll('*');
                    
                    for (let element of elements) {
                        const text = element.textContent || '';
                        const innerText = element.innerText || '';
                        
                        // 查找包含"总用户数"的元素
                        if (text.includes('总用户数') || innerText.includes('总用户数')) {
                            results.push({
                                text: text.trim(),
                                innerText: innerText.trim(),
                                tagName: element.tagName,
                                className: element.className
                            });
                        }
                    }
                    
                    return results;
                }
            """)
            
            print(f"   找到 {len(user_data)} 个包含'总用户数'的元素")
            
            # 分析每个找到的元素
            for i, data in enumerate(user_data):
                text_to_analyze = data['innerText'] or data['text']
                
                # 尝试提取数字
                followers_count = self._extract_user_count_from_text(text_to_analyze)
                if followers_count > 0:
                    print(f"   ✅ 成功提取总用户数: {followers_count:,}")
                    return followers_count
            
            # 如果上面的方法没找到，尝试更广泛的搜索
            print("🔍 扩大搜索范围...")
            
            # 搜索页面中所有可能的数字模式
            all_numbers = await self.context_page.evaluate("""
                () => {
                    const bodyText = document.body.textContent || '';
                    const lines = bodyText.split('\n');
                    const numberPatterns = [];
                    
                    for (let line of lines) {
                        line = line.trim();
                        // 查找包含"总用户数"和数字的行
                        if (line.includes('总用户数')) {
                            numberPatterns.push(line);
                        }
                        // 查找格式为"数字,数字"的模式
                        if (/\d{1,3},\d{3}/.test(line) && line.length < 50) {
                            numberPatterns.push(line);
                        }
                    }
                    
                    return numberPatterns;
                }
            """)
            
            print(f"   找到 {len(all_numbers)} 个可能的数字模式:")
            for pattern in all_numbers:
                print(f"     - {pattern}")
                followers_count = self._extract_user_count_from_text(pattern)
                if followers_count > 0:
                    print(f"   ✅ 从模式中提取到用户数: {followers_count:,}")
                    return followers_count
            
            print("❌ 未能找到总用户数")
            return 0
            
        except Exception as e:
            print(f"⚠️ 获取总用户数失败: {e}")
            return 0
    
    def _extract_user_count_from_text(self, text: str) -> int:
        """从文本中提取用户数"""
        try:
            print(f"🔢 分析文本中...'")
            
            # 清理文本
            text = text.strip().replace('\n', ' ').replace('\t', ' ')
            
            # 多种匹配模式，专门针对"总用户数 2,186"这种格式
            patterns = [
                r'总用户数[\s\n]*([\d,]+)',     # 总用户数 2,186
                r'([\d,]+)[\s\n]*\+\d+',       # 2,186 +2 (后面跟着增长数)
                r'([\d,]+)(?=\s*\+)',          # 2,186 (后面有+号)
                r'(\d{1,3},\d{3})',            # 匹配 X,XXX 格式
                r'(\d{4,})',                   # 4位以上数字
            ]
            
            for i, pattern in enumerate(patterns):
                matches = re.findall(pattern, text)
                print(f"     模式 {i+1} ({pattern}): {matches}")
                
                for match in matches:
                    try:
                        # 移除逗号并转换为整数
                        number_str = match.replace(',', '').replace('，', '')
                        number = int(number_str)
                        
                        # 验证数字是否在合理范围内 (100到1000万)
                        if 100 <= number <= 10000000:
                            print(f"       ✅ 有效数字: {number:,}")
                            return number
                        else:
                            print(f"       ⚠️ 数字 {number} 超出合理范围")
                    except ValueError as e:
                        print(f"       ❌ 转换失败: {e}")
                        continue
            
            print(f"   ❌ 无法从文本中提取有效数字")
            return 0
            
        except Exception as e:
            print(f"   ❌ 提取数字时出错: {e}")
            return 0
            
    def append_to_csv(self, data: Dict, filename: str = "followers.csv"):
        """追加数据到CSV文件"""
        try:
            print(f"💾 保存数据到 {filename}...")
            
            # 检查文件是否存在
            file_exists = Path(filename).exists()
            
            with open(filename, "a", newline="", encoding="utf-8") as f:
                fieldnames = ["日期", "账号名", "平台", "粉丝数"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                
                # 写入数据
                row_data = {
                    "日期": data["date"],
                    "账号名": data["username"],
                    "平台": data["platform"],
                    "粉丝数": data["followers"]
                }
                writer.writerow(row_data)
                
            print(f"✅ 数据已追加到 {filename}")
            print(f"   写入数据: {row_data}")
            
        except Exception as e:
            print(f"❌ 写入CSV失败: {e}")
            
    async def close(self):
        """关闭浏览器"""
        print("🔒 关闭浏览器...")
        if self.browser_context:
            await self.browser_context.close()
            print("✅ 浏览器已关闭")

# 导出函数：获取微信公众号数据
async def get_wechat_data(account_names: List[str] = None):
    """
    获取微信公众号数据
    :param account_names: 账号名称列表（可选，微信公众号会自动获取当前登录账号）
    :return: (成功数据列表, 失败账号列表)
    """
    print("📱 开始获取微信公众号数据...")
    
    crawler = WeChatMPCrawler()
    data_list = []
    failed_accounts = []
    
    try:
        # 初始化浏览器
        await crawler.init_browser(headless=False)
        
        # 登录
        if not await crawler.login():
            print("❌ 登录失败，退出程序")
            return [], ["登录失败"]
            
        # 获取账号数据
        account_data = await crawler.get_account_followers()
        
        if account_data and account_data.get("followers", 0) >= 0:  # 允许0粉丝
            data_list.append({
                '日期': account_data["date"],
                '账号名': account_data["username"],
                '平台': account_data["platform"],
                '粉丝数': account_data["followers"]
            })
            print(f"✅ {account_data['username']}: {account_data['followers']:,} 粉丝")
        else:
            print(f"❌ 获取失败")
            failed_accounts.append("获取失败")
            
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
        failed_accounts.append(str(e))
    finally:
        await crawler.close()
        
    return data_list, failed_accounts

async def main():
    """主函数"""
    print("=== 微信公众号粉丝数获取工具 (精确元素定位版) ===")
    print()
    
    crawler = WeChatMPCrawler()
    
    try:
        # 初始化浏览器
        await crawler.init_browser(headless=False)
        
        # 登录
        if not await crawler.login():
            print("❌ 登录失败，退出程序")
            return
            
        # 获取账号数据
        print(f"\n🎯 开始获取公众号数据...")
        account_data = await crawler.get_account_followers()
        
        if account_data and account_data.get("followers", 0) >= 0:
            # 追加到CSV文件
            crawler.append_to_csv(account_data, "followers.csv")
            print(f"\n🎉 成功获取数据！")
            print(f"   账号名: {account_data.get('username', '未知')}")
            print(f"   粉丝数: {account_data.get('followers', 0):,}")
            print(f"   日期: {account_data.get('date', '未知')}")
            print(f"\n📄 数据已保存到: followers.csv")
        else:
            print(f"❌ 获取失败")
            print(f"   返回的数据: {account_data}")
            
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
    finally:
        await crawler.close()

if __name__ == "__main__":
    asyncio.run(main())