import pymysql.cursors

# 数据库配置
DB_CONFIG = {
    'host': 'fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com',
    'user': 'admin',
    'password': 'fairwageaustralia',
    'port': 3306,
    'database': 'fairwageaustralia',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def debug_actual_query():
    """检查实际的查询结果"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        with connection.cursor() as cursor:
            
            print("=== 直接执行交通查询 ===")
            
            # 执行完全相同的SQL
            sql = """
            SELECT 
                COUNT(*) as total_stops,
                SUM(CASE WHEN MODE LIKE %s THEN 1 ELSE 0 END) as train_stops,
                SUM(CASE WHEN MODE LIKE %s THEN 1 ELSE 0 END) as tram_stops,
                SUM(CASE WHEN MODE LIKE %s THEN 1 ELSE 0 END) as bus_stops
            FROM epic3_transport_stops 
            WHERE SAL_CODE21 = %s
            """
            
            cursor.execute(sql, ('%TRAIN%', '%TRAM%', '%BUS%', 20002))
            result = cursor.fetchone()
            
            print("查询结果:")
            for key, value in result.items():
                print(f"  {key}: {value}")
                
            # 检查SAL_CODE21=20002的原始数据
            print("\n=== SAL_CODE21=20002的前5条记录 ===")
            cursor.execute("SELECT * FROM epic3_transport_stops WHERE SAL_CODE21 = 20002 LIMIT 5")
            records = cursor.fetchall()
            
            for i, record in enumerate(records, 1):
                print(f"记录{i}: {record}")
                
            # 检查SAL_CODE21字段的数据类型
            print("\n=== 检查SAL_CODE21字段类型 ===")
            cursor.execute("DESCRIBE epic3_transport_stops")
            columns = cursor.fetchall()
            
            for col in columns:
                if 'SAL_CODE21' in col['Field']:
                    print(f"SAL_CODE21字段: {col}")
                    
    except Exception as e:
        print(f"错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    debug_actual_query()