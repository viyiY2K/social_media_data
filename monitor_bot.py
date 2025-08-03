import lark_oapi as lark
from lark_oapi.api.im.v1 import *
import json
import subprocess
import sys
import os
import time
import threading
from datetime import datetime
import logging
import schedule

"""
获取 7 个平台的关注者数据，并导出小红书创作者中心数据，同步更新到飞书。带定时功能（默认早 9 点，且可在飞书中 @ 机器人触发实时更新。
"""


# 飞书机器人配置 - 需要你在飞书开放平台创建机器人应用
FEISHU_APP_ID = "your_app_id"          # 飞书应用ID
FEISHU_APP_SECRET = "your_app_secret"  # 飞书应用密钥
FEISHU_CHAT_ID = "your_chat_id"        # 飞书群聊ID
FEISHU_BOT_OPEN_ID = "your_bot_open_id"  # 飞书机器人OpenID


# 获取当前脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 小红书脚本路径
REDBOOK_SCRIPT_PATH = os.path.join(current_dir, "redbook.py")
# 关注者数据脚本路径
FOLLOWERS_SCRIPT_PATH = os.path.join(current_dir, "followers_feishu.py")

# 配置日志 - 同时输出到控制台和文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/redbook_monitor_bot.log'),
        logging.StreamHandler()  # 控制台输出
    ]
)

class RedbookMonitor:
    def __init__(self):
        # 创建飞书客户端
        self.client = lark.Client.builder().app_id(FEISHU_APP_ID).app_secret(FEISHU_APP_SECRET).build()
        self.is_monitoring = False
        
    def send_message(self, message, chat_id=None):
        """发送消息到飞书"""
        try:
            target_chat_id = chat_id or FEISHU_CHAT_ID
            content = json.dumps({"text": message})
            
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(target_chat_id)
                    .msg_type("text")
                    .content(content)
                    .build()
                )
                .build()
            )
            
            response = self.client.im.v1.message.create(request)
            
            if response.success():
                logging.info(f"✅ 消息发送成功: {message[:50]}...")
                return True
            else:
                logging.error(f"❌ 消息发送失败: {response.msg}")
                return False
                
        except Exception as e:
            logging.error(f"❌ 发送消息异常: {e}")
            return False
    
    # 在 run_redbook_script 方法中，成功完成后添加备份逻辑
    def run_redbook_script(self, triggered_by="手动", chat_id=None):
        """运行小红书脚本并监控状态"""
        try:
            start_time = datetime.now()
            logging.info(f"🚀 开始运行小红书脚本: {start_time} (触发方式: {triggered_by})")
            
            # 发送开始运行通知
            start_message = f"🚀 小红书数据同步开始运行\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n触发方式: {triggered_by}"
            self.send_message(start_message, chat_id)
            
            # 运行脚本
            result = subprocess.run(
                [sys.executable, REDBOOK_SCRIPT_PATH],
                capture_output=True,
                text=True,
                timeout=1800  # 30分钟超时
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            if result.returncode == 0:
                # 脚本正常运行完成
                success_message = (
                    f"✅ 小红书数据同步成功完成！\n"
                    f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"运行时长: {duration}\n"
                    f"触发方式: {triggered_by}\n"
                    f"状态: 正常运行"
                )
                
                # 尝试从输出中提取处理的数据条数
                if "总共成功处理了" in result.stdout:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if "总共成功处理了" in line:
                            success_message += f"\n{line.strip()}"
                            break
                
                logging.info("✅ 小红书脚本运行成功")
                self.send_message(success_message, chat_id)
                
                # 🆕 添加自动Git备份
                backup_success, backup_message = auto_git_backup(success_message, "小红书数据同步")
                if backup_success:
                    backup_notification = f"📁 {backup_message}"
                    self.send_message(backup_notification, chat_id)
                    logging.info(f"📁 {backup_message}")
                else:
                    backup_error = f"⚠️ Git备份失败: {backup_message}"
                    self.send_message(backup_error, chat_id)
                    logging.warning(f"⚠️ {backup_message}")
                
                return True
            
            else:
                # 脚本运行失败
                error_message = (
                    f"❌ 小红书数据同步失败！\n"
                    f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"运行时长: {duration}\n"
                    f"触发方式: {triggered_by}\n"
                    f"退出代码: {result.returncode}\n"
                    f"错误信息: {result.stderr[:500] if result.stderr else '无详细错误信息'}"
                )
                
                # 尝试解析状态输出
                if "STATUS:" in result.stdout:
                    status_lines = [line for line in result.stdout.split('\n') if line.startswith('STATUS:')]
                    if status_lines:
                        error_message += f"\n详细状态: {status_lines[-1]}"
                
                logging.error(f"❌ 小红书脚本运行失败，退出代码: {result.returncode}")
                self.send_message(error_message, chat_id)
                return False
                
        except subprocess.TimeoutExpired:
            timeout_message = (
                f"⏰ 小红书数据同步超时！\n"
                f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"触发方式: {triggered_by}\n"
                f"超时时间: 30分钟\n"
                f"状态: 脚本运行超时，已强制终止"
            )
            logging.error("⏰ 小红书脚本运行超时")
            self.send_message(timeout_message, chat_id)
            return False
            
        except Exception as e:
            exception_message = (
                f"💥 小红书数据同步异常！\n"
                f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"触发方式: {triggered_by}\n"
                f"异常信息: {str(e)[:500]}"
            )
            logging.error(f"💥 小红书脚本运行异常: {e}")
            self.send_message(exception_message, chat_id)
            return False

    # 在 run_followers_script 方法中也添加相同的备份逻辑
    def run_followers_script(self, triggered_by="手动", chat_id=None):
        """运行关注者数据脚本并监控状态"""
        try:
            start_time = datetime.now()
            logging.info(f"🚀 开始运行关注者数据脚本: {start_time} (触发方式: {triggered_by})")
            
            # 发送开始运行通知
            start_message = f"🚀 关注者数据同步开始运行\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n触发方式: {triggered_by}"
            self.send_message(start_message, chat_id)
            
            # 运行脚本
            result = subprocess.run(
                [sys.executable, FOLLOWERS_SCRIPT_PATH],
                capture_output=True,
                text=True,
                timeout=1800  # 30分钟超时
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            if result.returncode == 0:
                # 脚本正常运行完成
                success_message = (
                    f"✅ 关注者数据同步成功完成！\n"
                    f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"运行时长: {duration}\n"
                    f"触发方式: {triggered_by}\n"
                    f"状态: 正常运行"
                )
                
                # 尝试从输出中提取处理的数据条数
                if "总共成功处理了" in result.stdout:
                    lines = result.stdout.split('\n')
                    for line in lines:
                        if "总共成功处理了" in line:
                            success_message += f"\n{line.strip()}"
                            break
                
                # 检查输出中是否有微信公众号相关的错误信息
                output_text = result.stdout + result.stderr
                
                # 检查是否有微信公众号登录问题
                wechat_login_issues = [
                    "微信公众号数据获取可能存在问题",
                    "登录状态异常",
                    "检测到登录页面",
                    "扫码登录"
                ]
                
                has_wechat_issues = any(issue in output_text for issue in wechat_login_issues)
                
                if has_wechat_issues:
                    success_message += "\n⚠️ 注意：微信公众号登录状态可能异常，请检查登录状态"
                
                # 尝试解析状态输出
                if "STATUS:SUCCESS" in result.stdout:
                    status_lines = [line for line in result.stdout.split('\n') if line.startswith('STATUS:')]
                    if status_lines:
                        success_message += f"\n详细状态: {status_lines[-1]}"
                
                logging.info("✅ 关注者数据脚本运行成功")
                self.send_message(success_message, chat_id)
                return True
                
            else:
                # 脚本运行失败
                error_message = (
                    f"❌ 关注者数据同步失败！\n"
                    f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"运行时长: {duration}\n"
                    f"触发方式: {triggered_by}\n"
                    f"退出代码: {result.returncode}\n"
                    f"错误信息: {result.stderr[:500] if result.stderr else '无详细错误信息'}"
                )
                
                # 尝试解析状态输出
                if "STATUS:" in result.stdout:
                    status_lines = [line for line in result.stdout.split('\n') if line.startswith('STATUS:')]
                    if status_lines:
                        error_message += f"\n详细状态: {status_lines[-1]}"
                
                logging.error(f"❌ 关注者数据脚本运行失败，退出代码: {result.returncode}")
                self.send_message(error_message, chat_id)
                return False
                
        except subprocess.TimeoutExpired:
            timeout_message = (
                f"⏰ 关注者数据同步超时！\n"
                f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"触发方式: {triggered_by}\n"
                f"超时时间: 30分钟\n"
                f"状态: 脚本运行超时，已强制终止"
            )
            logging.error("⏰ 关注者数据脚本运行超时")
            self.send_message(timeout_message, chat_id)
            return False
            
        except Exception as e:
            exception_message = (
                f"💥 关注者数据同步异常！\n"
                f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"触发方式: {triggered_by}\n"
                f"异常信息: {str(e)[:500]}"
            )
            logging.error(f"💥 关注者数据脚本运行异常: {e}")
            self.send_message(exception_message, chat_id)
            return False
    
    def start_daily_monitoring(self, run_time="09:00"):
        """开始每日定时监控"""
        if self.is_monitoring:
            logging.warning("⚠️ 监控已在运行中")
            return
            
        self.is_monitoring = True
        
        # 清除之前的任务
        schedule.clear()
        
        # 设置每天指定时间运行
        schedule.every().day.at(run_time).do(self._scheduled_task)
        
        def monitor_loop():
            while self.is_monitoring:
                try:
                    schedule.run_pending()
                    time.sleep(60)  # 每分钟检查一次
                except Exception as e:
                    logging.error(f"❌ 监控循环异常: {e}")
                    time.sleep(60)
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logging.info(f"✅ 每日定时监控已启动，运行时间: {run_time}")
        
        # 发送启动通知
        self.send_message(f"🕘 每日定时监控已启动\n运行时间: 每天 {run_time}")
    
    def _scheduled_task(self):
        """定时任务执行函数"""
        logging.info("🕘 每日定时任务触发")
        self.run_redbook_script("每日定时监控(09:00)")
    
    def start_monitoring(self, interval_hours=24):
        """开始定时监控（保持原有方法兼容性）"""
        if self.is_monitoring:
            logging.warning("⚠️ 监控已在运行中")
            return
            
        self.is_monitoring = True
        
        def monitor_loop():
            while self.is_monitoring:
                try:
                    logging.info(f"🔍 定时监控触发，间隔: {interval_hours}小时")
                    self.run_redbook_script(f"定时监控(每{interval_hours}小时)")
                    
                    # 等待指定的时间间隔
                    for _ in range(int(interval_hours * 3600)):
                        if not self.is_monitoring:
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    logging.error(f"❌ 监控循环异常: {e}")
                    time.sleep(60)  # 出错后等待1分钟再继续
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logging.info(f"✅ 定时监控已启动，间隔: {interval_hours}小时")
    
    def stop_monitoring(self):
        """停止定时监控"""
        if self.is_monitoring:
            self.is_monitoring = False
            schedule.clear()  # 清除所有定时任务
            logging.info("🛑 定时监控已停止")
        else:
            logging.warning("⚠️ 监控未在运行")
    
    def run_once(self):
        """运行一次"""
        logging.info("🎯 手动运行一次")
        return self.run_redbook_script("手动运行")

# 创建全局监控实例
monitor = RedbookMonitor()

# 长连接事件处理函数
# 在 do_p2_im_message_receive_v1 函数开头添加更多日志
def do_p2_im_message_receive_v1(data: lark.im.v1.P2ImMessageReceiveV1) -> None:
    """处理接收到的消息事件"""
    try:
        logging.info("=" * 50)
        logging.info("🎯 收到消息事件！")
        logging.info(f'完整数据: {lark.JSON.marshal(data, indent=4)}')
        
        message = data.event.message
        logging.info(f"消息内容: {message.content}")
        logging.info(f"消息类型: {message.message_type}")
        logging.info(f"聊天ID: {message.chat_id}")
        
        # 检查是否是@机器人的消息
        mentions = message.mentions or []
        logging.info(f"提及列表: {mentions}")
        logging.info(f"提及数量: {len(mentions)}")
        
        if mentions:
            for i, mention in enumerate(mentions):
                logging.info(f"提及{i+1}: name={mention.name}, id={mention.id}")
                if mention.id:
                    logging.info(f"  - open_id: {mention.id.open_id}")
                    logging.info(f"  - user_id: {mention.id.user_id}")
            
            # 检测是否@了机器人
            bot_mentioned = any(
                mention.id.open_id == FEISHU_BOT_OPEN_ID
                for mention in mentions
            )
            
            logging.info(f"是否@了机器人: {bot_mentioned}")
            logging.info(f"当前APP_ID: {FEISHU_APP_ID}")
            
            if bot_mentioned:
                chat_id = message.chat_id
                
                logging.info(f"🎯 确认收到@机器人消息，群聊: {chat_id}")
                
                # 解析消息内容，检测关键词
                try:
                    content_obj = json.loads(message.content)
                    message_text = content_obj.get('text', '')
                except:
                    message_text = str(message.content)
                
                logging.info(f"解析后的消息文本: {message_text}")
                
                # 检测关键词
                followers_keywords = ['粉丝', '关注者', '关注', '用户数']
                redbook_keywords = ['小红书']
                
                has_followers_keyword = any(keyword in message_text for keyword in followers_keywords)
                has_redbook_keyword = any(keyword in message_text for keyword in redbook_keywords)
                
                logging.info(f"包含关注者相关关键词: {has_followers_keyword}")
                logging.info(f"包含小红书关键词: {has_redbook_keyword}")
                
                if has_followers_keyword:
                    # 运行关注者数据脚本
                    monitor.send_message(f"📢 检测到关注者数据相关关键词，开始执行关注者数据同步任务...", chat_id)
                    
                    def run_followers_script_async():
                        monitor.run_followers_script("群聊@触发(关注者数据)", chat_id)
                    
                    script_thread = threading.Thread(target=run_followers_script_async, daemon=True)
                    script_thread.start()
                    
                elif has_redbook_keyword:
                    # 运行小红书脚本（保持原有逻辑）
                    monitor.send_message(f"📢 检测到小红书关键词，开始执行小红书数据同步任务...", chat_id)
                    
                    def run_redbook_script_async():
                        monitor.run_redbook_script("群聊@触发(小红书)", chat_id)
                    
                    script_thread = threading.Thread(target=run_redbook_script_async, daemon=True)
                    script_thread.start()
                    
                else:
                    # 没有匹配的关键词，提示用户
                    help_message = (
                        "🤖 请在@消息中包含以下关键词之一：\n"
                        "• 粉丝/关注者/用户数 - 执行关注者数据同步\n"
                        "• 小红书 - 执行小红书数据同步"
                    )
                    monitor.send_message(help_message, chat_id)
                    logging.info("❌ 未检测到有效关键词")
                    
            else:
                logging.info("❌ 未检测到@机器人")
        else:
            logging.info("❌ 消息中没有@任何人")
            
        logging.info("=" * 50)
                
    except Exception as e:
        logging.error(f"❌ 处理消息事件异常: {e}")
        import traceback
        logging.error(traceback.format_exc())

# 创建事件处理器
event_handler = lark.EventDispatcherHandler.builder("", "") \
    .register_p2_im_message_receive_v1(do_p2_im_message_receive_v1) \
    .build()

def start_lark_websocket_client():
    """启动飞书长连接客户端"""
    try:
        logging.info("🔗 正在建立飞书长连接...")
        
        # 创建WebSocket客户端
        ws_client = lark.ws.Client(
            FEISHU_APP_ID,
            FEISHU_APP_SECRET,
            event_handler=event_handler,
            log_level=lark.LogLevel.DEBUG
        )
        
        logging.info("✅ 飞书长连接已建立，开始监听事件")
        logging.info("📱 现在可以在群聊中@机器人来触发脚本运行")
        
        # 发送启动通知
        monitor.send_message("🤖 数据监测机器人已上线！\n现在可以在群聊中@我来触发数据同步任务。")
        
        # 开始监听（这会阻塞当前线程）
        ws_client.start()
        
    except Exception as e:
        logging.error(f"❌ 长连接启动失败: {e}")
        raise

def main():
    """主函数"""
    print("数据同步监控机器人 (长连接版本)")
    print("=" * 50)
    print("请先配置以下参数:")
    print(f"1. FEISHU_APP_ID: {FEISHU_APP_ID}")
    print(f"2. FEISHU_APP_SECRET: {FEISHU_APP_SECRET}")
    print(f"3. FEISHU_CHAT_ID: {FEISHU_CHAT_ID}")
    print("=" * 50)
    
    if FEISHU_APP_ID == "your_bot_app_id" or FEISHU_APP_SECRET == "your_bot_app_secret" or FEISHU_CHAT_ID == "your_chat_id":
        print("❌ 请先配置飞书机器人参数！")
        return
    
    print("选择运行模式:")
    print("1. 运行一次")
    print("2. 每日定时监控 (每天早上9点运行)")
    print("3. 自定义监控间隔")
    print("4. 启动长连接监听 (支持@机器人触发)")
    print("5. 启动完整服务 (每日定时监控 + 长连接监听)")
    print("6. 自定义每日运行时间")
    
    choice = input("请选择 (1-6): ").strip()
    
    if choice == "1":
        monitor.run_once()
    elif choice == "2":
        print("🕘 开始每日定时监控 (每天早上9点运行)...")
        print("按 Ctrl+C 停止监控")
        monitor.start_daily_monitoring("09:00")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            monitor.stop_monitoring()
            print("\n👋 监控已停止")
    elif choice == "3":
        try:
            hours = float(input("请输入监控间隔 (小时): "))
            print(f"🔍 开始定时监控 (每{hours}小时运行一次)...")
            print("按 Ctrl+C 停止监控")
            monitor.start_monitoring(hours)
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                monitor.stop_monitoring()
                print("\n👋 监控已停止")
        except ValueError:
            print("❌ 无效的时间间隔")
    elif choice == "4":
        print("🔗 启动长连接监听...")
        print("📱 现在可以在群聊中@机器人来触发脚本运行")
        print("按 Ctrl+C 停止监听")
        try:
            start_lark_websocket_client()
        except KeyboardInterrupt:
            print("\n👋 长连接监听已停止")
    elif choice == "5":
        print("🚀 启动完整服务 (每日定时监控 + 长连接监听)...")
        print("🔗 长连接监听: 支持@机器人触发")
        print("🕘 每日定时监控: 每天早上9点运行")
        print("按 Ctrl+C 停止所有服务")
        
        # 启动每日定时监控
        monitor.start_daily_monitoring("09:00")
        
        # 启动长连接监听
        try:
            start_lark_websocket_client()
        except KeyboardInterrupt:
            monitor.stop_monitoring()
            print("\n👋 所有服务已停止")
    elif choice == "6":
        try:
            run_time = input("请输入每日运行时间 (格式: HH:MM，如 09:30): ").strip()
            # 验证时间格式
            datetime.strptime(run_time, "%H:%M")
            print(f"🕘 开始每日定时监控 (每天{run_time}运行)...")
            print("按 Ctrl+C 停止监控")
            monitor.start_daily_monitoring(run_time)
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                monitor.stop_monitoring()
                print("\n👋 监控已停止")
        except ValueError:
            print("❌ 无效的时间格式，请使用 HH:MM 格式")
    else:
        print("❌ 无效的选择")

if __name__ == "__main__":
    main()

# 在类定义前添加Git相关函数
import subprocess
import os
from datetime import datetime

def auto_git_backup(success_message="", script_type="数据同步"):
    """自动Git备份函数"""
    try:
        # 切换到viyi_data目录
        os.chdir('/Users/viyi/bili/viyi_data')
        
        # 检查是否有变更
        result = subprocess.run(['git', 'status', '--porcelain'], 
                              capture_output=True, text=True)
        
        if not result.stdout.strip():
            logging.info("📁 没有文件变更，跳过Git备份")
            return True, "没有文件变更"
        
        # 添加所有变更的文件
        subprocess.run(['git', 'add', '.'], check=True)
        
        # 创建提交信息
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        commit_message = f"Auto backup: {script_type} - {timestamp}"
        
        if success_message:
            # 从成功消息中提取处理的数据条数
            if "总共成功处理了" in success_message:
                lines = success_message.split('\n')
                for line in lines:
                    if "总共成功处理了" in line:
                        commit_message += f" ({line.strip()})"
                        break
        
        # 提交变更
        subprocess.run(['git', 'commit', '-m', commit_message], check=True)
        
        # 推送到远程仓库
        push_result = subprocess.run(['git', 'push'], 
                                   capture_output=True, text=True)
        
        if push_result.returncode == 0:
            logging.info(f"✅ Git备份成功: {commit_message}")
            return True, f"备份成功: {commit_message}"
        else:
            logging.error(f"❌ Git推送失败: {push_result.stderr}")
            return False, f"推送失败: {push_result.stderr}"
            
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Git操作失败: {e}")
        return False, f"Git操作失败: {e}"
    except Exception as e:
        logging.error(f"❌ 备份异常: {e}")
        return False, f"备份异常: {e}"