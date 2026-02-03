from logging_config import logger
import os
import glob
import importlib.util
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import errorcode
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import retry

class Migration:
    def __init__(self, user=None, password=None, host=None, database=None, port=None, 
                 code_version=None, migration_dir=None):
        """
        初始化Migration类
        
        Args:
            user: 数据库用户名
            password: 数据库密码
            host: 数据库主机
            database: 数据库名称
            port: 数据库端口
            code_version: 代码版本号，如果为None则从环境变量CODE_VERSION读取
            migration_dir: migration脚本所在目录，默认为当前文件所在目录
        """

        self.conn = mysql.connector.connect(
            user=user, password=password, host=host, database=database, port=port
        )
        
        # 创建版本表
        sql_command = """
        CREATE TABLE IF NOT EXISTS db_version (
            id INT AUTO_INCREMENT PRIMARY KEY,
            version INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """
        self.executeSQL(sql_command)
        
        # 如果版本表为空，插入初始版本0
        check_version = "SELECT COUNT(*) as count FROM db_version"
        result = self.executeSQL(check_version)
        if result and result[0][0] == 0:
            self.executeSQL("INSERT INTO db_version (version) VALUES (0)")
        
        # 设置代码版本
        self.code_version = code_version if code_version is not None else int(os.getenv("CODE_VERSION", "0"))
        
        # 设置migration脚本目录
        if migration_dir is None:
            migration_dir = os.path.dirname(os.path.abspath(__file__))
        self.migration_dir = migration_dir
        
    @retry(max_retries=3, delay=1)
    def executeSQL(self, query, params=None):
        """
        执行SQL语句
        
        Args:
            query: SQL查询语句
            params: 查询参数（可选）
            
        Returns:
            查询结果（如果有）
        """
        try:
            cursor = self.conn.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            try:
                values = cursor.fetchall()
            except:
                values = None
            self.conn.commit()
            cursor.close()
            return values
        except mysql.connector.Error as err:
            self.conn.rollback()
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(str(err))
            logger.error(f"SQL Error: {err}")
            raise
        except Exception as err:
            self.conn.rollback()
            self.errorMessage(f"Unexpected error: {str(err)}")
            logger.error(f"Unexpected error: {err}")
            raise
    
    def get_db_version(self):
        """
        获取数据库当前版本
        
        Returns:
            数据库版本号（整数）
        """
        try:
            query = "SELECT version FROM db_version ORDER BY id DESC LIMIT 1"
            result = self.executeSQL(query)
            if result and len(result) > 0:
                return int(result[0][0])
            return 0
        except Exception as err:
            logger.error(f"Error getting DB version: {err}")
            return 0
    
    def update_db_version(self, version):
        """
        更新数据库版本
        
        Args:
            version: 新的版本号
        """
        try:
            query = "UPDATE db_version SET version = %s WHERE id = (SELECT id FROM (SELECT id FROM db_version ORDER BY id DESC LIMIT 1) AS tmp)"
            self.executeSQL(query, (version,))
            logger.info(f"Database version updated to {version}")
        except Exception as err:
            logger.error(f"Error updating DB version: {err}")
            raise
    
    def get_migration_scripts(self):
        """
        获取所有migration脚本文件，按版本号排序
        
        Returns:
            排序后的migration脚本文件路径列表
        """
        pattern = os.path.join(self.migration_dir, "migration_*.py")
        scripts = glob.glob(pattern)
        
        # 提取版本号并排序
        script_versions = []
        for script in scripts:
            try:
                # 从文件名中提取版本号，例如 migration_1.py -> 1
                filename = os.path.basename(script)
                version_str = filename.replace("migration_", "").replace(".py", "")
                version = int(version_str)
                script_versions.append((version, script))
            except ValueError:
                logger.warning(f"Invalid migration script name: {filename}, skipping")
                continue
        
        # 按版本号排序
        script_versions.sort(key=lambda x: x[0])
        return [script for _, script in script_versions]
    
    def execute_migration_script(self, script_path):
        """
        执行单个migration脚本
        
        Args:
            script_path: migration脚本的路径
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Executing migration script: {script_path}")
            
            # 动态加载并执行migration脚本
            spec = importlib.util.spec_from_file_location("migration_module", script_path)
            if spec is None or spec.loader is None:
                raise ValueError(f"Could not load migration script: {script_path}")
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 如果脚本定义了migrate函数，则调用它
            if hasattr(module, 'migrate'):
                module.migrate(self.conn)
                logger.info(f"Successfully executed migration script: {script_path}")
                return True
            else:
                logger.warning(f"Migration script {script_path} does not define a 'migrate' function")
                return False
                
        except Exception as err:
            logger.error(f"Error executing migration script {script_path}: {err}")
            raise
    
    def migrate(self):
        """
        主迁移方法：比较代码版本和数据库版本，顺序执行需要的migration脚本
        """
        try:
            db_version = self.get_db_version()
            logger.info(f"Current DB version: {db_version}, Code version: {self.code_version}")
            
            if self.code_version <= db_version:
                logger.info("Database is up to date. No migration needed.")
                return
            
            # 获取所有migration脚本
            migration_scripts = self.get_migration_scripts()
            
            if not migration_scripts:
                logger.warning("No migration scripts found")
                return
            
            # 执行需要执行的migration脚本（版本号大于当前数据库版本）
            scripts_to_run = []
            for script_path in migration_scripts:
                try:
                    # 从文件名提取版本号
                    filename = os.path.basename(script_path)
                    version_str = filename.replace("migration_", "").replace(".py", "")
                    script_version = int(version_str)
                    
                    if script_version > db_version and script_version <= self.code_version:
                        scripts_to_run.append((script_version, script_path))
                except ValueError:
                    continue
            
            if not scripts_to_run:
                logger.info("No migration scripts need to be executed")
                return
            
            # 按版本号排序
            scripts_to_run.sort(key=lambda x: x[0])
            
            # 顺序执行migration脚本
            for script_version, script_path in scripts_to_run:
                logger.info(f"Running migration script version {script_version}: {script_path}")
                self.execute_migration_script(script_path)
                # 每执行完一个脚本，更新数据库版本
                self.update_db_version(script_version)
                logger.info(f"Migration to version {script_version} completed")
            
            # 最终更新到代码版本
            self.update_db_version(self.code_version)
            logger.info(f"Migration completed. Database version is now {self.code_version}")
            
        except Exception as err:
            logger.error(f"Migration failed: {err}")
            raise
    
    def errorMessage(self, message):
        """
        输出错误信息
        
        Args:
            message: 错误消息
        """
        logger.error(f"Migration Error: {message}")
    
    def close(self):
        """
        关闭数据库连接
        """
        if self.conn and self.conn.is_connected():
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """支持上下文管理器"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持上下文管理器"""
        self.close()

