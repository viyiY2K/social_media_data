import pandas as pd
from datetime import datetime
import requests
import json
import time
import os
import subprocess
import sys
import asyncio
import logging

"""
从小红书创作者中心导出数据，本地备份并增量更新到飞书表格
"""

# 飞书配置
FEISHU_APP_ID = "your_app_id"          # 飞书应用ID
FEISHU_APP_SECRET = "your_app_secret"  # 飞书应用密钥
FEISHU_APP_TOKEN = "your_app_token"    # 飞书应用令牌
FEISHU_TABLE_ID = "your_table_id"      # 飞书多维表格子表ID

# 数据文件配置
DATA_CSV_PATH = os.path.join("data", "redbook_data.csv")
EXCEL_DIR = os.path.join("downloads", "redbook")

# 配置日志
def setup_logging():
    """设置日志配置"""
    # 确保logs目录存在
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 生成日志文件名
    log_filename = f"redbook_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = os.path.join(log_dir, log_filename)
    
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    
    return log_path

def get_feishu_access_token():
    """获取飞书访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        result = response.json()
        
        if result.get('code') == 0:
            logging.info("✅ 成功获取飞书访问令牌")
            return result.get('tenant_access_token')
        else:
            logging.error(f"❌ 获取飞书访问令牌失败: {result.get('msg')}")
            return None
    except Exception as e:
        logging.error(f"❌ 获取飞书访问令牌异常: {e}")
        return None

def write_to_feishu_table(data_list, access_token, table_id, columns):
    """写入数据到飞书数据表（支持分批写入）"""
    if not data_list or not access_token or not table_id:
        print("❌ 数据为空或参数无效，跳过飞书写入")
        return False
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records/batch_create"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    # 分批处理，每批最多500条
    batch_size = 500
    total_records = len(data_list)
    success_count = 0
    
    for i in range(0, total_records, batch_size):
        batch_data = data_list[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total_records + batch_size - 1) // batch_size
        
        print(f"📝 正在写入第 {batch_num}/{total_batches} 批数据 ({len(batch_data)} 条记录)...")
        
        # 转换数据格式
        records = []
        for item in batch_data:
            fields = {}
            for col in columns:
                value = item.get(col, '')
                
                # 处理时间字段
                if col == '首次发布时间':
                    if pd.notna(value) and value != '':
                        try:
                            if isinstance(value, str):
                                # 处理中文时间格式：2025年07月20日17时37分34秒
                                if '年' in value and '月' in value and '日' in value:
                                    # 替换中文字符为标准格式
                                    time_str = value.replace('年', '-').replace('月', '-').replace('日', ' ')
                                    time_str = time_str.replace('时', ':').replace('分', ':').replace('秒', '')
                                    # 原始时间是中国时间，需要转换为UTC
                                    time_obj = pd.to_datetime(time_str)
                                    # 减去8小时转换为UTC时间
                                    time_obj_utc = time_obj - pd.Timedelta(hours=8)
                                    fields[col] = int(time_obj_utc.timestamp() * 1000)
                                else:
                                    # 尝试直接解析，假设也是中国时间
                                    time_obj = pd.to_datetime(value)
                                    time_obj_utc = time_obj - pd.Timedelta(hours=8)
                                    fields[col] = int(time_obj_utc.timestamp() * 1000)
                            else:
                                # 假设是中国时间，转换为UTC
                                time_obj = pd.to_datetime(value)
                                time_obj_utc = time_obj - pd.Timedelta(hours=8)
                                fields[col] = int(time_obj_utc.timestamp() * 1000)
                        except Exception as e:
                            print(f"⚠️ 时间字段 '{col}' 值 '{value}' 转换失败，使用原始字符串: {e}")
                            fields[col] = str(value)
                    else:
                        fields[col] = ''
                # 处理文本字段
                elif col in ['笔记标题', '体裁']:
                    fields[col] = str(value)[:1000] if pd.notna(value) else ''  # 限制长度避免过长
                # 处理数字字段（其他所有字段）
                else:
                    try:
                        fields[col] = int(float(value)) if pd.notna(value) and value != '' else 0
                    except:
                        fields[col] = 0
            
            records.append({"fields": fields})
        
        try:
            response = requests.post(url, json={"records": records}, headers=headers)
            result = response.json()
            
            if result.get('code') == 0:
                success_count += len(batch_data)
                print(f"✅ 第 {batch_num} 批数据写入成功！")
            else:
                print(f"❌ 第 {batch_num} 批数据写入失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            print(f"❌ 第 {batch_num} 批数据写入异常: {e}")
            return False
        
        # 添加延迟避免触发频率限制（10次/秒）
        if batch_num < total_batches:
            time.sleep(0.2)  # 200ms延迟
    
    print(f"🎉 所有数据写入完成！总共成功写入 {success_count} 条记录")
    return True

def read_excel_data(file_path):
    """读取Excel文件数据（从第二行开始，第二行作为表头）"""
    try:
        print(f"📖 正在读取Excel文件: {file_path}")
        
        # 读取Excel文件，跳过第一行，第二行作为表头
        df = pd.read_excel(file_path, header=1)  # header=1表示第二行作为表头
        
        # 删除所有列都为空的行
        df = df.dropna(how='all')
        
        print(f"📊 读取到 {len(df)} 行数据，{len(df.columns)} 列")
        print(f"📋 列名: {list(df.columns)}")
        
        # 转换为字典列表
        data_list = df.to_dict('records')
        
        return data_list, list(df.columns)
        
    except Exception as e:
        print(f"❌ 读取Excel文件失败: {e}")
        return [], []

def merge_data_with_history(excel_data, existing_csv_path, unique_key='首次发布时间'):
    """将Excel数据与现有CSV数据合并，使用发布时间作为唯一标识符"""
    # 读取现有CSV数据
    if os.path.exists(existing_csv_path):
        try:
            existing_df = pd.read_csv(existing_csv_path)
            print(f"📖 读取到现有CSV数据: {len(existing_df)} 条记录")
        except Exception as e:
            print(f"⚠️ 读取现有CSV文件失败: {e}，将创建新文件")
            existing_df = pd.DataFrame()
    else:
        existing_df = pd.DataFrame()
        print("📄 未找到现有CSV文件，将创建新文件")
    
    # 将Excel数据转换为DataFrame
    excel_df = pd.DataFrame(excel_data)
    
    if existing_df.empty:
        # 如果没有历史数据，直接使用Excel数据
        merged_df = excel_df
        new_records = len(excel_df)
        updated_records = 0
    else:
        # 确保两个DataFrame都有唯一标识符列
        if unique_key not in existing_df.columns:
            print(f"⚠️ 现有CSV文件中没有找到唯一标识符列 '{unique_key}'，将直接追加Excel数据")
            merged_df = pd.concat([existing_df, excel_df], ignore_index=True)
            new_records = len(excel_df)
            updated_records = 0
        elif unique_key not in excel_df.columns:
            print(f"⚠️ Excel文件中没有找到唯一标识符列 '{unique_key}'，将直接追加Excel数据")
            merged_df = pd.concat([existing_df, excel_df], ignore_index=True)
            new_records = len(excel_df)
            updated_records = 0
        else:
            # 真正比较数据内容的变化
            excel_keys = set(excel_df[unique_key].astype(str))
            existing_keys = set(existing_df[unique_key].astype(str))
            
            # 新增的记录
            new_keys = excel_keys - existing_keys
            new_records = len(new_keys)
            
            # 检查真正有内容变化的记录
            updated_records = 0
            actually_updated_keys = set()
            
            # 为现有数据建立索引，方便查找
            existing_dict = {}
            for _, row in existing_df.iterrows():
                key = str(row[unique_key])
                existing_dict[key] = row.to_dict()
            
            # 逐条比较Excel数据与现有数据
            for _, excel_row in excel_df.iterrows():
                excel_key = str(excel_row[unique_key])
                
                if excel_key in existing_dict:
                    # 记录存在，比较内容是否有变化
                    existing_row = existing_dict[excel_key]
                    has_changes = False
                    
                    # 比较每个字段（除了时间字段）
                    for col in excel_df.columns:
                        if col == unique_key:
                            continue  # 跳过主键字段
                        
                        excel_value = excel_row[col]
                        existing_value = existing_row.get(col, '')
                        
                        # 处理NaN值
                        if pd.isna(excel_value):
                            excel_value = ''
                        if pd.isna(existing_value):
                            existing_value = ''
                        
                        # 对于数字字段，转换为数字比较
                        if col not in ['笔记标题', '体裁', '首次发布时间']:
                            try:
                                excel_num = float(excel_value) if excel_value != '' else 0
                                existing_num = float(existing_value) if existing_value != '' else 0
                                if excel_num != existing_num:
                                    has_changes = True
                                    break
                            except:
                                # 如果转换失败，按字符串比较
                                if str(excel_value) != str(existing_value):
                                    has_changes = True
                                    break
                        else:
                            # 文本字段直接比较
                            if str(excel_value) != str(existing_value):
                                has_changes = True
                                break
                    
                    if has_changes:
                        updated_records += 1
                        actually_updated_keys.add(excel_key)
            
            # 合并数据：Excel数据优先（更新现有记录）
            merged_df = pd.concat([existing_df, excel_df]).drop_duplicates(
                subset=[unique_key], keep='last'
            ).reset_index(drop=True)
            
            # 输出详细的变化信息
            if updated_records > 0:
                print(f"🔍 检测到数据变化的记录:")
                for key in actually_updated_keys:
                    print(f"  - {key}")
    
    print(f"📊 合并后数据: {len(merged_df)} 条记录")
    print(f"📈 新增记录: {new_records} 条")
    print(f"🔄 真正更新记录: {updated_records} 条")
    
    return merged_df.to_dict('records')

def get_feishu_tables(access_token):
    """获取飞书多维表格中的所有数据表"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    try:
        print("📋 正在获取现有表格列表...")
        response = requests.get(url, headers=headers)
        result = response.json()
        
        if result.get('code') == 0:
            tables = result.get('data', {}).get('items', [])
            print(f"📊 找到 {len(tables)} 个现有表格")
            return tables
        else:
            print(f"❌ 获取表格列表失败: {result.get('msg')}")
            return []
    except Exception as e:
        print(f"❌ 获取表格列表异常: {e}")
        return []

def find_target_table(access_token, target_name="小红书"):
    """查找目标表格"""
    tables = get_feishu_tables(access_token)
    
    for table in tables:
        if table.get('name') == target_name:
            table_id = table.get('table_id')
            print(f"✅ 找到目标表格: {target_name} (ID: {table_id})")
            return table_id
    
    print(f"⚠️ 未找到名为 '{target_name}' 的表格")
    return None

def save_data_to_csv(data_list, csv_path):
    """保存数据到CSV文件"""
    if not data_list:
        print("❌ 没有数据可保存到CSV")
        return None
    
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        df = pd.DataFrame(data_list)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"💾 数据已保存到本地CSV文件: {csv_path}")
        return csv_path
    except Exception as e:
        print(f"❌ 保存CSV文件时出错: {e}")
        return None

def find_latest_excel_file(excel_dir, max_age_hours=24):
    """查找最新的Excel文件，并检查文件时间"""
    if not os.path.exists(excel_dir):
        logging.error(f"❌ Excel目录不存在: {excel_dir}")
        return None
    
    excel_files = []
    current_time = time.time()
    
    for f in os.listdir(excel_dir):
        if f.endswith('.xlsx') or f.endswith('.xls'):
            file_path = os.path.join(excel_dir, f)
            file_mtime = os.path.getmtime(file_path)
            
            # 检查文件是否在指定时间内创建
            age_hours = (current_time - file_mtime) / 3600
            
            if age_hours <= max_age_hours:
                excel_files.append((file_path, file_mtime, age_hours))
                logging.info(f"✅ 找到有效文件: {f} (创建于 {age_hours:.1f} 小时前)")
            else:
                logging.warning(f"⚠️ 文件过旧，跳过: {f} (创建于 {age_hours:.1f} 小时前)")
    
    if not excel_files:
        logging.error(f"❌ 在 {excel_dir} 目录中未找到{max_age_hours}小时内的Excel文件")
        return None
    
    # 按修改时间排序，返回最新的文件
    excel_files.sort(key=lambda x: x[1], reverse=True)
    latest_file = excel_files[0][0]
    latest_age = excel_files[0][2]
    
    logging.info(f"📁 选择最新Excel文件: {os.path.basename(latest_file)} (创建于 {latest_age:.1f} 小时前)")
    return latest_file

async def run_redbook_data_export():
    """运行redbook_data.py导出最新数据"""
    try:
        logging.info("🚀 开始运行数据导出脚本...")
        
        # 获取当前脚本所在目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        redbook_data_script = os.path.join(current_dir, "redbook_data.py")
        
        if not os.path.exists(redbook_data_script):
            logging.error(f"❌ 未找到数据导出脚本: {redbook_data_script}")
            return False
        
        logging.info(f"📂 执行脚本: {redbook_data_script}")
        
        # 使用subprocess运行redbook_data.py
        process = subprocess.Popen(
            [sys.executable, redbook_data_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=current_dir
        )
        
        # 实时输出日志
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                logging.info(f"[SUBPROCESS] {output.strip()}")
        
        # 获取返回码
        return_code = process.poll()
        
        if return_code == 0:
            logging.info("✅ 数据导出脚本执行成功！")
            return True
        else:
            stderr_output = process.stderr.read()
            logging.error(f"❌ 数据导出脚本执行失败，返回码: {return_code}")
            if stderr_output:
                logging.error(f"错误信息: {stderr_output}")
            return False
            
    except Exception as e:
        logging.error(f"❌ 运行数据导出脚本时出错: {e}")
        return False

def main():
    """主函数"""
    # 设置日志
    log_path = setup_logging()
    logging.info("🔍 开始处理小红书数据...")
    logging.info(f"📝 日志文件: {log_path}")
    
    try:
        # 首先运行数据导出脚本
        logging.info("\n📥 ===== 第一步：导出最新数据 =====")
        export_success = False
        export_error = None
        
        try:
            # 运行异步函数
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            export_success = loop.run_until_complete(run_redbook_data_export())
            loop.close()
            
            if not export_success:
                logging.warning("⚠️ 数据导出失败，但继续处理现有数据...")
                export_error = "数据导出返回失败状态"
            else:
                logging.info("✅ 数据导出完成！")
                time.sleep(2)
        except Exception as e:
            logging.error(f"❌ 数据导出过程出错: {e}")
            logging.warning("⚠️ 继续处理现有数据...")
            export_error = str(e)
        
        logging.info("\n📊 ===== 第二步：处理和上传数据 =====")
        
        # 查找最新的Excel文件
        excel_file = find_latest_excel_file(EXCEL_DIR)
        if not excel_file:
            logging.error("❌ 未找到有效的Excel文件")
            if export_error:
                print(f"STATUS:DATA_EXPORT_FAILED_AND_NO_RECENT_EXCEL:{export_error}")
                sys.exit(9)  # 数据导出失败且无最近的Excel文件
            else:
                print("STATUS:NO_RECENT_EXCEL_FILE")
                sys.exit(10)  # 无最近的Excel文件
        
        # 读取Excel数据
        excel_data, columns = read_excel_data(excel_file)
        
        if not excel_data:
            logging.error("❌ 未读取到任何数据")
            if export_error:
                print(f"STATUS:DATA_EXPORT_FAILED_AND_NO_DATA:{export_error}")
                sys.exit(8)  # 数据导出失败且无数据
            else:
                print("STATUS:NO_DATA")
                sys.exit(4)
        
        # 与历史数据合并
        logging.info("\n🔄 开始合并历史数据...")
        merged_data = merge_data_with_history(excel_data, DATA_CSV_PATH)
        
        # 保存合并后的数据到CSV文件
        csv_path = save_data_to_csv(merged_data, DATA_CSV_PATH)
        if not csv_path:
            logging.error("❌ 保存CSV文件失败")
            print("STATUS:CSV_SAVE_FAILED")
            sys.exit(5)
        
        logging.info(f"\n🚀 开始增量更新飞书多维表格...")
        
        # 获取飞书访问令牌
        access_token = get_feishu_access_token()
        if not access_token:
            logging.error("❌ 无法获取飞书访问令牌，跳过飞书更新")
            print("STATUS:FEISHU_TOKEN_FAILED")
            sys.exit(6)
        
        # 使用固定的表格ID进行增量更新
        logging.info(f"📋 使用固定表格ID: {FEISHU_TABLE_ID}")
        success = incremental_update_feishu_table(merged_data, access_token, FEISHU_TABLE_ID, columns)
        
        if success:
            logging.info(f"\n🎉 数据已成功增量更新到飞书数据表")
            logging.info(f"📋 处理了 {len(merged_data)} 条数据")
            logging.info(f"💾 本地备份文件: {csv_path}")
            logging.info(f"\n📈 总共成功处理了 {len(merged_data)} 条小红书数据")
            logging.info(f"📝 详细日志已保存到: {log_path}")
            
            # 输出成功状态
            if export_error:
                print("STATUS:SUCCESS_WITH_EXPORT_WARNING")
                print(f"EXPORT_WARNING:{export_error}")
            else:
                print("STATUS:SUCCESS")
            
            print(f"PROCESSED_RECORDS:{len(merged_data)}")
            print(f"CSV_PATH:{csv_path}")
            print(f"LOG_PATH:{log_path}")
            sys.exit(0)  # 成功退出
        else:
            logging.error("❌ 飞书数据增量更新失败")
            if export_error:
                print("STATUS:FEISHU_UPDATE_FAILED_WITH_EXPORT_WARNING")
                print(f"EXPORT_WARNING:{export_error}")
            else:
                print("STATUS:FEISHU_UPDATE_FAILED")
            
            print(f"PROCESSED_RECORDS:{len(merged_data)}")
            print(f"CSV_PATH:{csv_path}")
            sys.exit(1)  # 飞书更新失败
            
    except KeyboardInterrupt:
        logging.error("❌ 用户中断执行")
        print("STATUS:USER_INTERRUPTED")
        sys.exit(130)  # 用户中断
        
    except Exception as e:
        logging.error(f"❌ 脚本运行异常: {e}")
        print(f"STATUS:EXCEPTION:{str(e)[:200]}")
        sys.exit(2)  # 异常退出

def get_existing_records(access_token, table_id):
    """获取表格中的现有记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    all_records = []
    page_token = None
    
    try:
        while True:
            params = {'page_size': 500}
            if page_token:
                params['page_token'] = page_token
            
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            
            if result.get('code') != 0:
                print(f"❌ 获取记录失败: {result.get('msg')}")
                return {}
            
            records = result.get('data', {}).get('items', [])
            all_records.extend(records)
            
            # 检查是否还有更多页
            page_token = result.get('data', {}).get('page_token')
            if not page_token:
                break
        
        print(f"📊 获取到 {len(all_records)} 条现有记录")
        
        # 构建以发布时间为key的字典，方便查找
        records_dict = {}
        for record in all_records:
            fields = record.get('fields', {})
            publish_time = fields.get('首次发布时间', '')
            if publish_time:
                # 将时间戳转换为字符串用于匹配
                if isinstance(publish_time, (int, float)):
                    # 飞书存储的是UTC时间戳（毫秒），转换为中国时间字符串
                    import datetime
                    utc_time = datetime.datetime.fromtimestamp(publish_time / 1000, tz=datetime.timezone.utc)
                    china_time = utc_time + datetime.timedelta(hours=8)
                    time_str = china_time.strftime('%Y年%m月%d日%H时%M分%S秒')
                else:
                    time_str = str(publish_time)
                
                records_dict[time_str] = {
                    'record_id': record['record_id'],
                    'fields': fields
                }
        
        return records_dict
        
    except Exception as e:
        print(f"❌ 获取现有记录异常: {e}")
        return {}

def compare_and_prepare_updates(new_data, existing_records, columns):
    """比较新旧数据，准备更新、新增和删除的记录"""
    updates = []  # 需要更新的记录
    creates = []  # 需要新增的记录
    
    print("🔍 开始比较数据变化...")
    
    for item in new_data:
        publish_time = item.get('首次发布时间', '')
        if not publish_time:
            continue
        
        # 标准化时间格式用于匹配
        if isinstance(publish_time, str) and ('年' in publish_time):
            time_key = publish_time
        else:
            # 如果是其他格式，尝试转换
            try:
                if isinstance(publish_time, str):
                    time_obj = pd.to_datetime(publish_time)
                else:
                    time_obj = pd.to_datetime(publish_time)
                time_key = time_obj.strftime('%Y年%m月%d日%H时%M分%S秒')
            except:
                time_key = str(publish_time)
        
        if time_key in existing_records:
            # 记录存在，检查是否需要更新
            existing_record = existing_records[time_key]
            existing_fields = existing_record['fields']
            record_id = existing_record['record_id']
            
            # 比较各字段是否有变化
            has_changes = False
            updated_fields = {}
            
            for col in columns:
                new_value = item.get(col, '')
                
                # 处理不同类型的字段
                if col == '首次发布时间':
                    # 时间字段不需要更新（作为主键）
                    continue
                elif col in ['笔记标题', '体裁']:
                    # 文本字段
                    new_str = str(new_value)[:1000] if pd.notna(new_value) else ''
                    existing_str = str(existing_fields.get(col, ''))
                    if new_str != existing_str:
                        updated_fields[col] = new_str
                        has_changes = True
                else:
                    # 数字字段
                    try:
                        new_num = int(float(new_value)) if pd.notna(new_value) and new_value != '' else 0
                    except:
                        new_num = 0
                    
                    existing_num = existing_fields.get(col, 0)
                    if isinstance(existing_num, str):
                        try:
                            existing_num = int(float(existing_num))
                        except:
                            existing_num = 0
                    
                    if new_num != existing_num:
                        updated_fields[col] = new_num
                        has_changes = True
            
            if has_changes:
                updates.append({
                    'record_id': record_id,
                    'fields': updated_fields
                })
        else:
            # 新记录，需要创建
            fields = {}
            for col in columns:
                value = item.get(col, '')
                
                # 处理时间字段
                if col == '首次发布时间':
                    if pd.notna(value) and value != '':
                        try:
                            if isinstance(value, str) and '年' in value:
                                time_str = value.replace('年', '-').replace('月', '-').replace('日', ' ')
                                time_str = time_str.replace('时', ':').replace('分', ':').replace('秒', '')
                                time_obj = pd.to_datetime(time_str)
                                time_obj_utc = time_obj - pd.Timedelta(hours=8)
                                fields[col] = int(time_obj_utc.timestamp() * 1000)
                            else:
                                time_obj = pd.to_datetime(value)
                                time_obj_utc = time_obj - pd.Timedelta(hours=8)
                                fields[col] = int(time_obj_utc.timestamp() * 1000)
                        except Exception as e:
                            print(f"⚠️ 时间字段转换失败: {e}")
                            fields[col] = str(value)
                    else:
                        fields[col] = ''
                elif col in ['笔记标题', '体裁']:
                    fields[col] = str(value)[:1000] if pd.notna(value) else ''
                else:
                    try:
                        fields[col] = int(float(value)) if pd.notna(value) and value != '' else 0
                    except:
                        fields[col] = 0
            
            creates.append({'fields': fields})
    
    print(f"📊 数据比较完成:")
    print(f"🔄 需要更新: {len(updates)} 条记录")
    print(f"📝 需要新增: {len(creates)} 条记录")
    
    return updates, creates

def batch_update_records(access_token, table_id, updates):
    """批量更新记录"""
    if not updates:
        print("📄 没有需要更新的记录")
        return True
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records/batch_update"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    # 分批处理
    batch_size = 500
    success_count = 0
    
    for i in range(0, len(updates), batch_size):
        batch_updates = updates[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(updates) + batch_size - 1) // batch_size
        
        print(f"🔄 正在更新第 {batch_num}/{total_batches} 批数据 ({len(batch_updates)} 条记录)...")
        
        try:
            response = requests.post(url, json={"records": batch_updates}, headers=headers)
            result = response.json()
            
            if result.get('code') == 0:
                success_count += len(batch_updates)
                print(f"✅ 第 {batch_num} 批更新成功！")
            else:
                print(f"❌ 第 {batch_num} 批更新失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            print(f"❌ 第 {batch_num} 批更新异常: {e}")
            return False
        
        # 添加延迟避免频率限制
        if batch_num < total_batches:
            time.sleep(0.2)
    
    print(f"✅ 批量更新完成，成功更新 {success_count} 条记录")
    return True

def batch_create_records(access_token, table_id, creates):
    """批量创建记录"""
    if not creates:
        print("📄 没有需要新增的记录")
        return True
    
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records/batch_create"
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'}
    
    # 分批处理
    batch_size = 500
    success_count = 0
    
    for i in range(0, len(creates), batch_size):
        batch_creates = creates[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(creates) + batch_size - 1) // batch_size
        
        print(f"📝 正在新增第 {batch_num}/{total_batches} 批数据 ({len(batch_creates)} 条记录)...")
        
        try:
            response = requests.post(url, json={"records": batch_creates}, headers=headers)
            result = response.json()
            
            if result.get('code') == 0:
                success_count += len(batch_creates)
                print(f"✅ 第 {batch_num} 批新增成功！")
            else:
                print(f"❌ 第 {batch_num} 批新增失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            print(f"❌ 第 {batch_num} 批新增异常: {e}")
            return False
        
        # 添加延迟避免频率限制
        if batch_num < total_batches:
            time.sleep(0.2)
    
    print(f"✅ 批量新增完成，成功新增 {success_count} 条记录")
    return True

def incremental_update_feishu_table(data_list, access_token, table_id, columns):
    """增量更新飞书数据表（只更新变化的数据）"""
    if not data_list or not access_token or not table_id:
        print("❌ 数据为空或参数无效，跳过飞书更新")
        return False
    
    print(f"🔄 开始增量更新飞书表格 (ID: {table_id})...")
    
    # 第一步：获取现有记录
    existing_records = get_existing_records(access_token, table_id)
    
    # 第二步：比较数据，准备更新和新增列表
    updates, creates = compare_and_prepare_updates(data_list, existing_records, columns)
    
    # 第三步：执行更新
    update_success = batch_update_records(access_token, table_id, updates)
    if not update_success:
        return False
    
    # 第四步：执行新增
    create_success = batch_create_records(access_token, table_id, creates)
    if not create_success:
        return False
    
    print(f"🎉 增量更新完成！更新了 {len(updates)} 条记录，新增了 {len(creates)} 条记录")
    return True

if __name__ == "__main__":
    main()