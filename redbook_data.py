#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书数据导出工具
基于playwright自动化访问创作者平台并导出数据
仅可导出半年内记录，平台限制。
"""

import asyncio
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from playwright.async_api import async_playwright, BrowserContext, Page
except ImportError:
    print("请先安装playwright: pip install playwright")
    print("然后安装浏览器: playwright install")
    import sys
    sys.exit(1)

class RedbookDataExporter:
    def __init__(self):
        self.browser_context: Optional[BrowserContext] = None
        self.context_page: Optional[Page] = None
        self.user_data_dir = Path.cwd() / "browser_data" / "redbook"
        self.download_dir = Path.cwd() / "downloads" / "redbook"  
        
    async def init_browser(self, headless: bool = False):
        """初始化浏览器"""
        playwright = await async_playwright().start()
        
        # 确保目录存在
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        # 移除 self.redbook_dir.mkdir(parents=True, exist_ok=True)
        
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
            timezone_id="Asia/Shanghai",
            downloads_path=str(self.download_dir)  # 直接下载到 downloads/redbook
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
        """登录小红书创作者平台"""
        try:
            await self.context_page.goto("https://creator.xiaohongshu.com/statistics/data-analysis")
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
            # 检查是否在数据分析页面且已登录
            current_url = self.context_page.url
            if "creator.xiaohongshu.com" in current_url and "login" not in current_url:
                # 检查页面是否包含数据导出相关元素
                selectors = [
                    "[data-testid='export-button']",
                    "button:has-text('导出数据')",
                    "button:has-text('导出')",
                    ".export-btn",
                    "[class*='export']"
                ]
                
                for selector in selectors:
                    element = await self.context_page.query_selector(selector)
                    if element:
                        return True
                        
                # 如果没有找到导出按钮，检查是否有用户信息
                user_selectors = [
                    ".user-info",
                    ".avatar",
                    "[class*='user']",
                    "[class*='profile']"
                ]
                
                for selector in user_selectors:
                    element = await self.context_page.query_selector(selector)
                    if element:
                        return True
                        
            return False
        except:
            return False
            
    async def _set_date_range(self) -> bool:
        """设置笔记首发时间为半年前到当前日期"""
        try:
            print("📅 开始设置笔记首发时间...")
            
            # 计算日期
            current_date = datetime.now()
            six_months_ago = current_date - timedelta(days=180)  # 大约半年
            
            start_date = six_months_ago.strftime("%Y-%m-%d")
            end_date = current_date.strftime("%Y-%m-%d")
            
            print(f"📅 设置时间范围: {start_date} 到 {end_date}")
            
            # 查找时间选择器的多种可能选择器
            date_selectors = [
                "input[placeholder*='开始日期']",
                "input[placeholder*='结束日期']",
                "input[type='date']",
                ".date-picker input",
                "[class*='date'] input",
                "[class*='time'] input",
                "input[placeholder*='日期']",
                "input[placeholder*='时间']"
            ]
            
            # 查找包含"笔记首发时间"的元素附近的日期输入框
            date_inputs = await self.context_page.evaluate("""
                () => {
                    const inputs = [];
                    
                    // 查找包含"笔记首发时间"或"发布时间"的元素
                    const labels = document.querySelectorAll('*');
                    for (let label of labels) {
                        const text = label.textContent || '';
                        if (text.includes('笔记首发时间') || text.includes('发布时间') || text.includes('时间范围')) {
                            // 在该元素附近查找日期输入框
                            const parent = label.closest('div, form, section');
                            if (parent) {
                                const dateInputs = parent.querySelectorAll('input[type="date"], input[placeholder*="日期"], input[placeholder*="时间"], .date-picker input');
                                dateInputs.forEach(input => {
                                    inputs.push({
                                        selector: input.tagName.toLowerCase() + (input.className ? '.' + input.className.split(' ').join('.') : '') + (input.placeholder ? '[placeholder="' + input.placeholder + '"]' : ''),
                                        placeholder: input.placeholder || '',
                                        type: input.type || '',
                                        value: input.value || ''
                                    });
                                });
                            }
                        }
                    }
                    
                    // 如果没找到，就查找所有日期输入框
                    if (inputs.length === 0) {
                        const allDateInputs = document.querySelectorAll('input[type="date"], input[placeholder*="日期"], input[placeholder*="时间"]');
                        allDateInputs.forEach(input => {
                            inputs.push({
                                selector: input.tagName.toLowerCase() + (input.className ? '.' + input.className.split(' ').join('.') : '') + (input.placeholder ? '[placeholder="' + input.placeholder + '"]' : ''),
                                placeholder: input.placeholder || '',
                                type: input.type || '',
                                value: input.value || ''
                            });
                        });
                    }
                    
                    return inputs;
                }
            """)
            
            if not date_inputs:
                print("⚠️ 未找到日期输入框，尝试查找日期选择器...")
                
                # 尝试查找并点击日期选择器
                date_picker_selectors = [
                    "button:has-text('选择日期')",
                    "button:has-text('时间')",
                    ".date-picker",
                    "[class*='date-picker']",
                    "[class*='time-picker']"
                ]
                
                for selector in date_picker_selectors:
                    try:
                        element = await self.context_page.query_selector(selector)
                        if element:
                            await element.click()
                            await asyncio.sleep(1)
                            print(f"✅ 点击了日期选择器: {selector}")
                            break
                    except:
                        continue
                        
                # 重新查找日期输入框
                await asyncio.sleep(2)
                date_inputs = await self.context_page.evaluate("""
                    () => {
                        const inputs = [];
                        const allDateInputs = document.querySelectorAll('input[type="date"], input[placeholder*="日期"], input[placeholder*="时间"], .date-picker input');
                        allDateInputs.forEach(input => {
                            if (input.offsetParent !== null) {  // 只选择可见的输入框
                                inputs.push({
                                    selector: 'input[type="' + (input.type || 'text') + '"]',
                                    placeholder: input.placeholder || '',
                                    type: input.type || '',
                                    value: input.value || ''
                                });
                            }
                        });
                        return inputs;
                    }
                """)
            
            if not date_inputs:
                print("❌ 未找到日期输入框")
                return False
                
            print(f"✅ 找到 {len(date_inputs)} 个日期输入框")
            
            # 设置日期值
            date_values = [start_date, end_date]
            
            for i, date_input in enumerate(date_inputs[:2]):  # 只处理前两个输入框
                try:
                    # 尝试多种方式查找输入框
                    input_selectors = [
                        f"input[placeholder='{date_input['placeholder']}']",
                        f"input[type='{date_input['type']}']",
                        "input[type='date']",
                        "input[placeholder*='日期']"
                    ]
                    
                    input_element = None
                    for selector in input_selectors:
                        try:
                            elements = await self.context_page.query_selector_all(selector)
                            if elements and i < len(elements):
                                input_element = elements[i]
                                break
                        except:
                            continue
                    
                    if input_element:
                        # 清空并设置新值
                        await input_element.click()
                        await input_element.fill("")
                        await input_element.fill(date_values[i] if i < len(date_values) else end_date)
                        await asyncio.sleep(0.5)
                        
                        # 验证值是否设置成功
                        current_value = await input_element.input_value()
                        print(f"✅ 设置日期输入框 {i+1}: {date_values[i] if i < len(date_values) else end_date} (当前值: {current_value})")
                    else:
                        print(f"⚠️ 未找到第 {i+1} 个日期输入框")
                        
                except Exception as e:
                    print(f"⚠️ 设置第 {i+1} 个日期输入框失败: {e}")
                    
            # 等待页面更新
            await asyncio.sleep(2)
            print("✅ 日期范围设置完成")
            return True
            
        except Exception as e:
            print(f"❌ 设置日期范围失败: {e}")
            return False
    
    async def export_data(self) -> bool:
        """导出数据"""
        try:
            print("🎯 开始导出数据...")
            
            # 确保在数据分析页面
            await self.context_page.goto("https://creator.xiaohongshu.com/statistics/data-analysis")
            await asyncio.sleep(3)
            
            # 设置日期范围
            if not await self._set_date_range():
                print("⚠️ 设置日期范围失败，继续导出...")
            
            # 查找导出按钮的多种可能选择器
            export_selectors = [
                "button:has-text('导出数据')",
                "button:has-text('导出')",
                "[data-testid='export-button']",
                ".export-btn",
                "[class*='export'][role='button']",
                "[class*='export-button']",
                "button[class*='export']",
                "a:has-text('导出')",
                "span:has-text('导出')"  # 有时导出文字在span中
            ]
            
            export_button = None
            for selector in export_selectors:
                try:
                    export_button = await self.context_page.query_selector(selector)
                    if export_button:
                        # 检查按钮是否可见和可点击
                        is_visible = await export_button.is_visible()
                        is_enabled = await export_button.is_enabled()
                        if is_visible and is_enabled:
                            print(f"✅ 找到导出按钮: {selector}")
                            break
                        else:
                            export_button = None
                except:
                    continue
                    
            if not export_button:
                print("⚠️ 未找到导出按钮，尝试通过JavaScript查找...")
                # 使用JavaScript查找包含"导出"文字的可点击元素
                export_button_js = await self.context_page.evaluate("""
                    () => {
                        const elements = document.querySelectorAll('*');
                        for (let element of elements) {
                            const text = element.textContent || '';
                            if ((text.includes('导出数据') || text.includes('导出')) && 
                                (element.tagName === 'BUTTON' || element.onclick || element.style.cursor === 'pointer')) {
                                return element;
                            }
                        }
                        return null;
                    }
                """)
                
                if export_button_js:
                    print("✅ 通过JavaScript找到导出按钮")
                else:
                    print("❌ 未找到导出按钮，请检查页面是否正确加载")
                    return False
            
            # 监听下载事件
            download_info = {"path": None}
            
            async def handle_download(download):
                # 等待下载完成
                download_path = await download.path()
                download_info["path"] = download_path
                print(f"📥 文件下载完成: {download_path}")
            
            self.context_page.on("download", handle_download)
            
            # 点击导出按钮
            if export_button:
                await export_button.click()
            else:
                # 使用JavaScript点击
                await self.context_page.evaluate("""
                    () => {
                        const elements = document.querySelectorAll('*');
                        for (let element of elements) {
                            const text = element.textContent || '';
                            if ((text.includes('导出数据') || text.includes('导出')) && 
                                (element.tagName === 'BUTTON' || element.onclick || element.style.cursor === 'pointer')) {
                                element.click();
                                return;
                            }
                        }
                    }
                """)
            
            print("🖱️ 已点击导出按钮，等待下载...")
            
            # 等待下载完成（最多等待30秒）
            for i in range(30):
                await asyncio.sleep(1)
                if download_info["path"]:
                    break
                if i % 5 == 0:
                    print(f"⏳ 等待下载... ({i+1}/30秒)")
            
            if not download_info["path"]:
                print("❌ 下载超时，请检查是否有弹窗需要确认")
                return False
                
            # 移动并重命名文件
            return await self._process_downloaded_file(download_info["path"])
            
        except Exception as e:
            print(f"❌ 导出数据失败: {e}")
            return False
            
    async def _process_downloaded_file(self, download_path: str) -> bool:
        """处理下载的文件"""
        try:
            download_file = Path(download_path)
            if not download_file.exists():
                print(f"❌ 下载文件不存在: {download_path}")
                return False
                
            # 生成新文件名
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_extension = download_file.suffix
            new_filename = f"redbook_{current_time}{file_extension}.xlsx"
            new_file_path = self.download_dir / new_filename  # 使用 download_dir 而不是 redbook_dir
            
            # 重命名文件（在同一目录内）
            download_file.rename(new_file_path)
            print(f"✅ 文件已保存到: {new_file_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ 处理下载文件失败: {e}")
            return False
            
    async def close(self):
        """关闭浏览器"""
        if self.browser_context:
            await self.browser_context.close()
            print("✅ 浏览器已关闭")

async def main():
    """主函数"""
    print("=== 小红书数据导出工具 ===")
    print("目标网址: https://creator.xiaohongshu.com/statistics/data-analysis")
    print()
    
    exporter = RedbookDataExporter()
    
    try:
        # 初始化浏览器
        await exporter.init_browser(headless=False)
        
        # 登录
        if not await exporter.login():
            print("❌ 登录失败，退出程序")
            return
            
        # 导出数据
        if await exporter.export_data():
            print("🎉 数据导出成功！")
        else:
            print("❌ 数据导出失败")
            
    except Exception as e:
        print(f"❌ 程序执行出错: {e}")
    finally:
        await exporter.close()

if __name__ == "__main__":
    asyncio.run(main())