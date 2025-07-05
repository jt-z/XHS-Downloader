#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库调试脚本 - 用于快速检查数据库状态
"""

import sqlite3
import os

def debug_database(db_path: str, table_name: str = "explore_data"):
    """调试数据库状态"""
    print(f"=== 数据库调试信息 ===")
    print(f"数据库路径: {db_path}")
    print(f"表名: {table_name}")
    
    # 1. 检查文件是否存在
    if not os.path.exists(db_path):
        print(f"❌ 错误: 数据库文件不存在")
        return
    
    print(f"✅ 数据库文件存在")
    
    try:
        # 2. 连接数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print(f"✅ 数据库连接成功")
        
        # 3. 查看所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"📋 数据库中的表: {tables}")
        
        # 4. 检查目标表是否存在
        if table_name not in tables:
            print(f"❌ 错误: 表 '{table_name}' 不存在")
            print(f"💡 可用的表: {tables}")
            return
        
        print(f"✅ 表 '{table_name}' 存在")
        
        # 5. 查看表结构
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]
        print(f"📊 表结构: {columns}")
        
        # 6. 统计记录数
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]
        print(f"📈 总记录数: {total_count}")
        
        # 7. 检查关键字段
        key_fields = ['作品标题', '作品标签', '作品ID']
        missing_fields = [field for field in key_fields if field not in columns]
        if missing_fields:
            print(f"⚠️  缺少关键字段: {missing_fields}")
        else:
            print(f"✅ 关键字段完整")
        
        # 8. 查看前几条记录
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_records = cursor.fetchall()
        print(f"📄 前3条记录示例:")
        for i, record in enumerate(sample_records):
            print(f"  记录 {i+1}: {record}")
        
        # 9. 检查分类列是否存在
        category_columns = ['类别1', '类别2', '类别1_ID', '类别2_ID']
        existing_cats = [col for col in category_columns if col in columns]
        missing_cats = [col for col in category_columns if col not in columns]
        
        if existing_cats:
            print(f"📋 已存在的分类列: {existing_cats}")
        if missing_cats:
            print(f"🆕 需要添加的分类列: {missing_cats}")
        
        # 10. 如果有分类列，统计已分类数量
        if '类别1' in columns:
            cursor.execute(f"""
            SELECT COUNT(*) FROM {table_name} 
            WHERE 类别1 IS NOT NULL AND 类别1 != ''
            """)
            classified_count = cursor.fetchone()[0]
            unclassified_count = total_count - classified_count
            print(f"📊 已分类记录: {classified_count}")
            print(f"📊 未分类记录: {unclassified_count}")
        
        conn.close()
        print(f"✅ 数据库调试完成")
        
    except Exception as e:
        print(f"❌ 数据库操作出错: {e}")
        import traceback
        traceback.print_exc()

def main():
    # 请根据你的实际情况修改这些路径
    possible_paths = [
        "/mnt/d/xiaohongshu/XHS-Downloader_V2.5_Windows_X64/_internal/Download/ExploreData.db"
    ]
    
    # 找到存在的数据库文件
    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            break
    
    if db_path is None:
        print("❌ 未找到数据库文件，请检查以下路径:")
        for path in possible_paths:
            print(f"  - {path}")
        return
    
    # 调试数据库
    debug_database(db_path)

if __name__ == "__main__":
    main()