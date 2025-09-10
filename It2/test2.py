import mysql.connector
from mysql.connector import Error

def view_field_values():
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
        
        # 获取表结构，找出文本字段
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()
        
        text_fields = []
        for col in columns:
            field, type_, null, key, default, extra = col
            if not any(num_type in type_.lower() for num_type in ['int', 'double', 'float', 'decimal']):
                text_fields.append(field)
        
        # 显示每个字段的值
        for field in text_fields:
            cursor.execute(f"SELECT DISTINCT `{field}` FROM `{table_name}` WHERE `{field}` IS NOT NULL ORDER BY `{field}` LIMIT 50")
            values = cursor.fetchall()
            
            print(f"\n{field}:")
            for value in values:
                print(f"  {value[0]}")
                
    except Error as e:
        print(f"错误: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    view_field_values()