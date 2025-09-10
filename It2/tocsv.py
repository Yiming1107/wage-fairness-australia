import mysql.connector
import csv
from mysql.connector import Error

def export_table_to_csv():
    try:
        connection = mysql.connector.connect(
            host='fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password='fairwageaustralia',
            database='fairwageaustralia'
        )
        
        cursor = connection.cursor()
        
        table_name = input("输入表名: ").strip()
        
        # 检查表是否存在
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            print(f"表 '{table_name}' 不存在")
            return
        
        # 询问是否要导出所有数据或限制行数
        limit_choice = input("是否限制导出行数? (y/n): ").lower()
        if limit_choice == 'y':
            limit = input("输入要导出的行数: ").strip()
            if limit.isdigit():
                query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
            else:
                print("无效行数，导出所有数据")
                query = f"SELECT * FROM `{table_name}`"
        else:
            query = f"SELECT * FROM `{table_name}`"
        
        print(f"正在执行查询: {query}")
        cursor.execute(query)
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 获取所有数据
        rows = cursor.fetchall()
        
        if not rows:
            print("表中无数据")
            return
        
        # 生成CSV文件名
        csv_filename = f"{table_name}.csv"
        
        # 写入CSV文件
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # 写入列名
            writer.writerow(column_names)
            
            # 写入数据行
            for row in rows:
                writer.writerow(row)
        
        print(f"成功导出 {len(rows)} 行数据到文件: {csv_filename}")
        
    except Error as e:
        print(f"数据库错误: {e}")
    except Exception as e:
        print(f"导出错误: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    export_table_to_csv()