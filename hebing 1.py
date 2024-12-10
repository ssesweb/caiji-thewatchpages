import csv
import os
import pandas as pd
import re
import logging
from datetime import datetime

# 配置日志记录
logging.basicConfig(filename='csv_merge_and_process.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义要合并的CSV文件目录
input_directory = r'C:\Users\Administrator\Downloads\2024年9月19日\watch'
output_directory = os.path.join(input_directory, 'merged_files')

# 如果输出文件夹不存在，则创建它
os.makedirs(output_directory, exist_ok=True)

# 获取目录中所有CSV文件
csv_files = [filename for filename in os.listdir(input_directory) if filename.endswith('.csv')]
csv_count = len(csv_files)

if csv_count > 0:
    logging.info(f'发现 {csv_count} 个CSV文件，正在合并...')
    print(f'发现 {csv_count} 个CSV文件，正在合并...')

    # 创建一个基于时间戳的唯一输出文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    merged_csv_filename = f"merged_files_{timestamp}.csv"
    merged_csv_file = os.path.join(output_directory, merged_csv_filename)

    # 打开输出文件以写入模式
    with open(merged_csv_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        header_written = False
        
        # 遍历目录中的所有CSV文件
        for filename in csv_files:
            with open(os.path.join(input_directory, filename), 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                rows = list(reader)
                if len(rows) > 1:
                    if not header_written:
                        writer.writerow(rows[0])  # 写入首行
                        header_written = True
                    for row in rows[1:]:
                        writer.writerow(row)  # 写入剩余行

    logging.info(f'合并完成，结果保存在 {merged_csv_file}')
    print(f'合并完成，结果保存在 {merged_csv_file}')

    # 尝试多种编码读取合并后的CSV文件
    encodings = ['utf-8', 'GBK', 'ISO-8859-1', 'cp1252']
    df = None

    for encoding in encodings:
        try:
            df = pd.read_csv(merged_csv_file, encoding=encoding)
            logging.info(f"成功使用 {encoding} 编码读取文件。")
            break  # 成功读取后跳出循环
        except (UnicodeDecodeError, pd.errors.ParserError) as e:
            logging.warning(f"使用 {encoding} 编码读取文件失败: {e}")

    if df is not None and not df.empty:
        # 创建一个空的列表来存储所有HTML表格
        html_tables = []
        
        # 遍历每一行
        for index, row in df.iterrows():
            # 获取'商品描述'列的内容
            text = row['商品描述']  # 假设列的名称是'商品描述'
            
            # 创建一个HTML表格
            html_table = "<table border='1'>\n"
            pattern = re.compile(r"(\w+): ([\w\s,]+)")
            matches = pattern.findall(text)
            
            # 为每个条目添加行到HTML表格
            for key, value in matches:
                html_table += f"  <tr>\n    <th>{key}</th>\n    <td>{value}</td>\n  </tr>\n"
            html_table += "</table>"
            
            # 将整个HTML表格作为一个单元格的内容
            html_tables.append(html_table)
        
        # 将HTML表格列表添加为新列
        df['HTML_Table'] = html_tables
        
        # 获取CSV文件的目录
        output_csv_file = os.path.join(output_directory, f'processed_output_with_html_{timestamp}.csv')

        # 保存处理后的DataFrame到CSV文件
        try:
            df.to_csv(output_csv_file, index=False, encoding='utf-8', escapechar='\\')
            logging.info(f"处理后的CSV文件已保存到: {output_csv_file}")
            print(f"处理后的CSV文件已保存到: {output_csv_file}")
        except Exception as e:
            logging.error(f"保存处理后的CSV文件时出错: {e}")
            print(f"保存处理后的CSV文件时出错: {e}")
    else:
        logging.error("未能成功读取合并后的CSV文件，无法生成处理后的CSV文件。")
        print("未能成功读取合并后的CSV文件，无法生成处理后的CSV文件。")
else:
    logging.error('未找到CSV文件')
    print('未找到CSV文件')