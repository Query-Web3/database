"""
示例Migration脚本 - migration_1.py

每个migration脚本必须定义一个migrate(conn)函数，该函数接收数据库连接对象作为参数。
"""


def migrate(conn):
    """
    执行数据库迁移操作
    
    Args:
        conn: mysql.connector.connection.MySQLConnection 数据库连接对象
    """
    cursor = conn.cursor()
    
    try:
        # 示例：创建一个新表
        create_table_sql = """
        ALTER TABLE Bifrost_site_table
        ADD COLUMN liq TEXT;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("Migration 1: Created Bifrost_site_table liq successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"Migration 1 failed: {e}")
        raise
    finally:
        cursor.close()

