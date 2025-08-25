import pyodbc
from typing import Dict, Any, Optional, List

class MSSQLConnector:
    def __init__(self, server: str, database: str, username: str, password: str):
        """
        初始化 MSSQL 数据库连接
        
        参数:
            server: 服务器地址
            database: 数据库名称
            username: 用户名
            password: 密码
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None
        
    def connect(self) -> bool:
        """
        连接到 MSSQL 数据库
        
        返回:
            bool: 连接是否成功
        """
        try:
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            return True
        except pyodbc.Error as e:
            print(f"连接数据库失败: {e}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def insert_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """
        向指定表插入数据
        
        参数:
            table_name: 表名
            data: 要插入的数据字典，键为列名，值为数据
            
        返回:
            bool: 插入是否成功
        """
        if not self.connection or not self.cursor:
            print("数据库未连接")
            return False
        
        try:
            # 构建列名和值
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            values = list(data.values())
            
            # 构建 SQL 语句
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # 执行插入
            self.cursor.execute(sql, values)
            self.connection.commit()
            return True
        except pyodbc.Error as e:
            print(f"插入数据失败: {e}")
            self.connection.rollback()
            return False
    
    def bulk_insert(self, table_name: str, columns: List[str], data: List[tuple]) -> bool:
        """
        批量插入数据
        
        参数:
            table_name: 表名
            columns: 列名列表
            data: 要插入的数据列表，每个元素是一个元组，对应一行数据
            
        返回:
            bool: 插入是否成功
        """
        if not self.connection or not self.cursor:
            print("数据库未连接")
            return False
        
        try:
            # 构建 SQL 语句
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['?'] * len(columns))
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 执行批量插入
            self.cursor.executemany(sql, data)
            self.connection.commit()
            return True
        except pyodbc.Error as e:
            print(f"批量插入失败: {e}")
            self.connection.rollback()
            return False

    def __enter__(self):
        """支持 with 语句"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持 with 语句"""
        self.disconnect()


# 使用示例
if __name__ == "__main__":
    # 连接参数
    server = "your_server_name"
    database = "your_database_name"
    username = "your_username"
    password = "your_password"
    
    # 创建连接对象
    with MSSQLConnector(server, database, username, password) as connector:
        # 单条插入示例
        user_data = {
            "id": 1,
            "name": "张三",
            "age": 30,
            "email": "zhangsan@example.com"
        }
        if connector.insert_data("users", user_data):
            print("数据插入成功")
        
        # 批量插入示例
        columns = ["id", "name", "age", "email"]
        users_data = [
            (2, "李四", 25, "lisi@example.com"),
            (3, "王五", 28, "wangwu@example.com"),
            (4, "赵六", 35, "zhaoliu@example.com")
        ]
        if connector.bulk_insert("users", columns, users_data):
            print("批量插入成功")