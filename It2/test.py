import mysql.connector
from mysql.connector import Error

def connect_and_explore_database():
    try:
        # 数据库连接配置
        connection = mysql.connector.connect(
            host='fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password='fairwageaustralia'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            print("✅ 成功连接到MySQL数据库服务器")
            
            # 1. 查看所有数据库
            print("\n📊 数据库列表:")
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            for db in databases:
                if db[0] not in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                    print(f"  - {db[0]}")
            
            # 2. 选择一个数据库
            database_name = input("\n请输入要查看的数据库名: ")
            
            try:
                cursor.execute(f"USE {database_name}")
                print(f"✅ 已选择数据库: {database_name}")
                
                # 3. 查看该数据库中的所有表
                print(f"\n📋 数据库 '{database_name}' 中的表:")
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                table_list = []
                for i, table in enumerate(tables, 1):
                    table_name = table[0]
                    table_list.append(table_name)
                    print(f"  {i}. {table_name}")
                
                # 4. 查看每个表的结构
                print(f"\n🔍 表结构详情:")
                for table_name in table_list:
                    print(f"\n{'='*50}")
                    print(f"表名: {table_name}")
                    print('='*50)
                    
                    # 获取表结构
                    cursor.execute(f"DESCRIBE {table_name}")
                    columns = cursor.fetchall()
                    
                    # 格式化输出表结构
                    print(f"{'字段名':<20} {'类型':<15} {'空值':<8} {'键':<8} {'默认值':<15} {'额外信息'}")
                    print('-' * 80)
                    for col in columns:
                        field, type_, null, key, default, extra = col
                        print(f"{field:<20} {type_:<15} {null:<8} {key:<8} {str(default):<15} {extra}")
                    
                    # 获取表的行数
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    print(f"\n📊 数据行数: {row_count}")
                
                # 5. 可选：导出表结构到文本文件
                export_choice = input("\n是否要导出表结构到文本文件? (y/n): ").lower()
                if export_choice == 'y':
                    export_table_structures(cursor, database_name, table_list)
                
            except Error as e:
                print(f"❌ 选择数据库时出错: {e}")
            
    except Error as e:
        print(f"❌ 连接数据库时出错: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("\n🔌 数据库连接已关闭")

def export_table_structures(cursor, database_name, table_list):
    """导出所有表结构到文本文件"""
    filename = f"{database_name}_table_structures.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"数据库: {database_name}\n")
        f.write("="*50 + "\n\n")
        
        for table_name in table_list:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            f.write(f"表名: {table_name}\n")
            f.write("-"*30 + "\n")
            f.write(f"{'字段名':<20} {'类型':<15} {'空值':<8} {'键':<8} {'默认值':<15} {'额外信息'}\n")
            f.write("-"*80 + "\n")
            
            for col in columns:
                field, type_, null, key, default, extra = col
                f.write(f"{field:<20} {type_:<15} {null:<8} {key:<8} {str(default):<15} {extra}\n")
            
            f.write("\n" + "="*50 + "\n\n")
    
    print(f"✅ 表结构已导出到: {filename}")

def quick_table_info():
    """快速查看指定表的信息"""
    try:
        connection = mysql.connector.connect(
            host='fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password='fairwageaustralia'
        )
        
        cursor = connection.cursor()
        
        database_name = input("数据库名: ")
        table_name = input("表名: ")
        
        cursor.execute(f"USE {database_name}")
        
        # 表结构
        print(f"\n🔍 表 {table_name} 的结构:")
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        for col in columns:
            print(f"  {col[0]} - {col[1]} ({col[2]}, {col[3]})")
        
        # 示例数据
        print(f"\n📄 前5行数据:")
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        rows = cursor.fetchall()
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        print(f"列名: {column_names}")
        
        for row in rows:
            print(row)
            
    except Error as e:
        print(f"❌ 错误: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    print("🗄️  MySQL数据库查看工具")
    print("1. 完整探索数据库")
    print("2. 快速查看指定表")
    
    choice = input("\n选择操作 (1/2): ")
    
    if choice == "1":
        connect_and_explore_database()
    elif choice == "2":
        quick_table_info()
    else:
        print("无效选择")