#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多线程内容分类处理脚本
根据内容标题和描述，自动分类到预定义的二级分类体系
"""

import sqlite3
import threading
import queue
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, List, Dict, Optional
import jieba
import jieba.analyse
from contextlib import contextmanager

# 预定义的分类体系
CATEGORY_SYSTEM = {
    1: {
        'name': '生活方式',
        'subcategories': {
            1: {'name': '日常生活', 'keywords': ['生活', '日常', '习惯', '作息', '居家', '家务', '洗衣', '清洁', '整理']},
            2: {'name': '美食烹饪', 'keywords': ['美食', '烹饪', '菜谱', '做菜', '食物', '餐厅', '吃饭', '料理', '食材', '味道']},
            3: {'name': '健康养生', 'keywords': ['健康', '养生', '锻炼', '运动', '医疗', '保健', '营养', '减肥', '健身']},
            4: {'name': '购物消费', 'keywords': ['购物', '消费', '买', '商品', '价格', '优惠', '折扣', '商场', '网购']},
            5: {'name': '居住租房', 'keywords': ['租房', '房子', '住房', '装修', '家居', '搬家', '房租', '小区', '物业']}
        }
    },
    2: {
        'name': '娱乐文化',
        'subcategories': {
            1: {'name': '影视娱乐', 'keywords': ['电影', '电视', '综艺', '明星', '演员', '导演', '剧情', '影院', '追剧']},
            2: {'name': '游戏动漫', 'keywords': ['游戏', '动漫', '漫画', '手游', '网游', '主机', '动画', '二次元', '角色']},
            3: {'name': '音乐艺术', 'keywords': ['音乐', '歌曲', '歌手', '演唱会', '艺术', '绘画', '摄影', '舞蹈', '乐器']},
            4: {'name': '文学历史', 'keywords': ['文学', '历史', '书籍', '小说', '诗歌', '古代', '文化', '传统', '典故']},
            5: {'name': '网络文化', 'keywords': ['网络', '梗', '表情包', '弹幕', '直播', '网红', '短视频', '社交媒体', '流行']}
        }
    },
    3: {
        'name': '旅游出行',
        'subcategories': {
            1: {'name': '旅游攻略', 'keywords': ['旅游', '攻略', '景点', '路线', '住宿', '酒店', '民宿', '旅行', '度假']},
            2: {'name': '交通出行', 'keywords': ['交通', '出行', '地铁', '公交', '打车', '开车', '停车', '路况', '导航']},
            3: {'name': '城市探索', 'keywords': ['城市', '探索', '街道', '建筑', '商圈', '夜生活', '本地', '发现']},
            4: {'name': '户外运动', 'keywords': ['户外', '登山', '徒步', '露营', '钓鱼', '骑行', '跑步', '运动', '健身']},
            5: {'name': '景点体验', 'keywords': ['景点', '体验', '游玩', '参观', '门票', '拍照', '风景', '名胜', '古迹']}
        }
    },
    4: {
        'name': '教育职场',
        'subcategories': {
            1: {'name': '学习教育', 'keywords': ['学习', '教育', '学校', '课程', '知识', '培训', '辅导', '考试', '成绩']},
            2: {'name': '职场工作', 'keywords': ['工作', '职场', '公司', '同事', '老板', '薪资', '加班', '会议', '项目']},
            3: {'name': '毕业就业', 'keywords': ['毕业', '就业', '找工作', '面试', '简历', '求职', '招聘', '实习', '校招']},
            4: {'name': '技能培训', 'keywords': ['技能', '培训', '学习', '提升', '证书', '课程', '教程', '实操', '经验']},
            5: {'name': '考试认证', 'keywords': ['考试', '认证', '证书', '资格', '备考', '刷题', '成绩', '通过', '资质']}
        }
    },
    5: {
        'name': '科技数码',
        'subcategories': {
            1: {'name': '数码产品', 'keywords': ['手机', '电脑', '数码', '产品', '配置', '性能', '品牌', '型号', '评测']},
            2: {'name': '软件应用', 'keywords': ['软件', '应用', 'APP', '程序', '工具', '系统', '操作', '功能', '使用']},
            3: {'name': '科学知识', 'keywords': ['科学', '知识', '原理', '实验', '发现', '研究', '理论', '技术', '创新']},
            4: {'name': '技术分享', 'keywords': ['技术', '分享', '教程', '代码', '编程', '开发', '算法', '方法', '经验']},
            5: {'name': '人工智能', 'keywords': ['人工智能', 'AI', '机器学习', '深度学习', '算法', '模型', '智能', '自动化']}
        }
    },
    6: {
        'name': '社会话题',
        'subcategories': {
            1: {'name': '时事新闻', 'keywords': ['新闻', '时事', '热点', '事件', '报道', '社会', '国内', '国际', '政治']},
            2: {'name': '社会现象', 'keywords': ['社会', '现象', '问题', '讨论', '观点', '争议', '话题', '趋势', '变化']},
            3: {'name': '法律维权', 'keywords': ['法律', '维权', '律师', '法规', '权利', '纠纷', '起诉', '合同', '法院']},
            4: {'name': '经济房产', 'keywords': ['经济', '房产', '房价', '投资', '理财', '股票', '市场', '金融', '贷款']},
            5: {'name': '政策制度', 'keywords': ['政策', '制度', '规定', '法规', '政府', '公告', '通知', '改革', '措施']}
        }
    },
    7: {
        'name': '情感社交',
        'subcategories': {
            1: {'name': '情感关系', 'keywords': ['情感', '关系', '恋爱', '男女', '感情', '分手', '表白', '约会', '婚姻']},
            2: {'name': '家庭亲情', 'keywords': ['家庭', '亲情', '父母', '孩子', '亲戚', '家人', '陪伴', '照顾', '教育']},
            3: {'name': '友情社交', 'keywords': ['友情', '社交', '朋友', '聚会', '交友', '圈子', '人际', '沟通', '相处']},
            4: {'name': '个人成长', 'keywords': ['成长', '个人', '提升', '改变', '进步', '反思', '目标', '规划', '自我']},
            5: {'name': '心理健康', 'keywords': ['心理', '健康', '情绪', '压力', '焦虑', '抑郁', '心情', '治疗', '咨询']}
        }
    },
    8: {
        'name': '兴趣爱好',
        'subcategories': {
            1: {'name': '运动健身', 'keywords': ['运动', '健身', '锻炼', '跑步', '游泳', '瑜伽', '器械', '肌肉', '体型']},
            2: {'name': '钓鱼户外', 'keywords': ['钓鱼', '户外', '野外', '自然', '探险', '露营', '徒步', '登山', '冒险']},
            3: {'name': '手工创作', 'keywords': ['手工', '创作', 'DIY', '制作', '工艺', '设计', '艺术', '创意', '作品']},
            4: {'name': '收藏展示', 'keywords': ['收藏', '展示', '古董', '文物', '藏品', '鉴定', '价值', '爱好', '珍藏']},
            5: {'name': '宠物动物', 'keywords': ['宠物', '动物', '猫', '狗', '养宠', '萌宠', '饲养', '照顾', '训练']}
        }
    },
    9: {
        'name': '地域文化',
        'subcategories': {
            1: {'name': '城市生活', 'keywords': ['城市', '生活', '都市', '市区', '社区', '邻里', '便民', '服务', '设施']},
            2: {'name': '地方特色', 'keywords': ['地方', '特色', '特产', '风味', '传统', '民俗', '文化', '习俗', '节日']},
            3: {'name': '方言文化', 'keywords': ['方言', '文化', '语言', '口音', '土话', '俚语', '地方话', '传统', '习惯']},
            4: {'name': '区域对比', 'keywords': ['区域', '对比', '比较', '差异', '南北', '东西', '地区', '差别', '特点']},
            5: {'name': '乡村生活', 'keywords': ['乡村', '生活', '农村', '田园', '农业', '种植', '养殖', '淳朴', '自然']}
        }
    },
    10: {
        'name': '其他',
        'subcategories': {
            1: {'name': '搞笑幽默', 'keywords': ['搞笑', '幽默', '笑话', '段子', '有趣', '好笑', '逗', '娱乐', '轻松']},
            2: {'name': '奇闻趣事', 'keywords': ['奇闻', '趣事', '奇怪', '有趣', '神奇', '罕见', '特殊', '惊奇', '新奇']},
            3: {'name': '实用技巧', 'keywords': ['技巧', '实用', '方法', '窍门', '小贴士', '妙招', '经验', '诀窍', '攻略']},
            4: {'name': '随感杂谈', 'keywords': ['随感', '杂谈', '感想', '想法', '闲聊', '话题', '讨论', '观点', '看法']},
            5: {'name': '未分类', 'keywords': ['其他', '未分类', '杂项', '不明', '无法', '分类', '混合', '综合', '多样']}
        }
    }
}

class ContentClassifier:
    def __init__(self, db_path: str, table_name: str = 'explore_data'):
        self.db_path = db_path
        self.table_name = table_name
        self.category_system = CATEGORY_SYSTEM
        self.db_lock = threading.Lock()  # 添加数据库锁
        
        # 初始化jieba分词
        jieba.initialize()
        
        # 构建关键词索引
        self.keyword_index = self._build_keyword_index()
        
    @contextmanager
    def get_db_connection(self):
        """安全的数据库连接上下文管理器"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")  # 启用WAL模式
            conn.execute("PRAGMA busy_timeout=30000")  # 设置忙等待超时
            try:
                yield conn
            finally:
                conn.close()
        
    def _build_keyword_index(self) -> Dict[str, List[Tuple[int, int]]]:
        """构建关键词到分类的索引"""
        keyword_index = {}
        
        for cat1_id, cat1_info in self.category_system.items():
            for cat2_id, cat2_info in cat1_info['subcategories'].items():
                for keyword in cat2_info['keywords']:
                    if keyword not in keyword_index:
                        keyword_index[keyword] = []
                    keyword_index[keyword].append((cat1_id, cat2_id))
        
        return keyword_index
    
    def _extract_features(self, title: str, content: str) -> List[str]:
        """提取文本特征"""
        # 合并标题和内容
        text = f"{title} {content}" if content else title
        
        # 使用jieba进行分词和关键词提取
        keywords = jieba.analyse.extract_tags(text, topK=20, withWeight=False)
        
        # 添加原文中的关键词
        for keyword in self.keyword_index.keys():
            if keyword in text:
                keywords.append(keyword)
        
        return list(set(keywords))
    
    def _calculate_category_score(self, features: List[str]) -> Dict[Tuple[int, int], float]:
        """计算每个分类的匹配分数"""
        scores = {}
        
        for feature in features:
            if feature in self.keyword_index:
                for cat1_id, cat2_id in self.keyword_index[feature]:
                    key = (cat1_id, cat2_id)
                    if key not in scores:
                        scores[key] = 0
                    scores[key] += 1
        
        return scores
    
    def classify_content(self, title: str, content: str = "") -> Tuple[int, int, str, str]:
        """对单个内容进行分类"""
        # 提取特征
        features = self._extract_features(title, content)
        
        # 计算分类分数
        scores = self._calculate_category_score(features)
        
        if not scores:
            # 如果没有匹配的关键词，归类为"其他-未分类"
            return 10, 5, "其他", "未分类"
        
        # 找到分数最高的分类
        best_category = max(scores.items(), key=lambda x: x[1])
        cat1_id, cat2_id = best_category[0]
        
        cat1_name = self.category_system[cat1_id]['name']
        cat2_name = self.category_system[cat1_id]['subcategories'][cat2_id]['name']
        
        return cat1_id, cat2_id, cat1_name, cat2_name
    
    def get_table_structure(self) -> List[str]:
        """获取表结构"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns = [row[1] for row in cursor.fetchall()]
        return columns
    
    def add_category_columns(self):
        """添加分类列到表中"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 检查列是否已存在
            columns = self.get_table_structure()
            
            if '类别1' not in columns:
                cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别1 TEXT")
            if '类别2' not in columns:
                cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别2 TEXT")
            if '类别1_ID' not in columns:
                cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别1_ID INTEGER")
            if '类别2_ID' not in columns:
                cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别2_ID INTEGER")
            
            conn.commit()
    
    def get_unprocessed_records(self, batch_size: int = 1000) -> List[Tuple]:
        """获取未处理的记录"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 获取所有未分类的记录
            query = f"""
            SELECT rowid, 作品标题, 作品标签, 作品ID
            FROM {self.table_name} 
            WHERE 类别1 IS NULL OR 类别1 = ''
            LIMIT {batch_size}
            """
            
            cursor.execute(query)
            records = cursor.fetchall()
        
        return records
    
    def update_record_category(self, rowid: int, cat1_id: int, cat2_id: int, cat1_name: str, cat2_name: str):
        """更新单条记录的分类"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"""
                    UPDATE {self.table_name} 
                    SET 类别1_ID = ?, 类别2_ID = ?, 类别1 = ?, 类别2 = ?
                    WHERE rowid = ?
                    """, (cat1_id, cat2_id, cat1_name, cat2_name, rowid))
                    conn.commit()
                return  # 成功执行，退出
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # 指数退避
                    continue
                else:
                    raise
    
    def batch_update_categories(self, updates: List[Tuple[int, int, int, str, str]]):
        """批量更新分类 - 更高效的方法"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.executemany(f"""
                    UPDATE {self.table_name} 
                    SET 类别1_ID = ?, 类别2_ID = ?, 类别1 = ?, 类别2 = ?
                    WHERE rowid = ?
                    """, [(cat1_id, cat2_id, cat1_name, cat2_name, rowid) 
                          for rowid, cat1_id, cat2_id, cat1_name, cat2_name in updates])
                    conn.commit()
                return  # 成功执行，退出
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))  # 指数退避
                    continue
                else:
                    raise
    
    def process_batch(self, records: List[Tuple]) -> int:
        """处理一批记录 - 使用批量更新优化"""
        processed_count = 0
        updates = []
        
        for record in records:
            rowid, title, tags, work_id = record
            
            # 对内容进行分类
            cat1_id, cat2_id, cat1_name, cat2_name = self.classify_content(
                title or "", tags or ""
            )
            
            # 收集更新数据
            updates.append((rowid, cat1_id, cat2_id, cat1_name, cat2_name))
            processed_count += 1
            
            # 每50条记录批量更新一次
            if len(updates) >= 50:
                self.batch_update_categories(updates)
                updates = []
                
                if processed_count % 100 == 0:
                    print(f"线程 {threading.current_thread().name} 已处理 {processed_count} 条记录")
        
        # 处理剩余的更新
        if updates:
            self.batch_update_categories(updates)
        
        return processed_count
    
    def process_all_records(self, max_workers: int = 2, batch_size: int = 500):
        """多线程处理所有记录 - 降低并发度"""
        print("开始处理数据库记录...")
        
        # 添加分类列
        self.add_category_columns()
        
        total_processed = 0
        start_time = time.time()
        
        # 降低线程数量，减少数据库锁冲突
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                # 获取未处理的记录
                records = self.get_unprocessed_records(batch_size)
                
                if not records:
                    break
                
                # 将记录分批分配给线程
                batch_per_thread = len(records) // max_workers + 1
                batches = [records[i:i + batch_per_thread] 
                          for i in range(0, len(records), batch_per_thread)]
                
                # 提交任务
                futures = [executor.submit(self.process_batch, batch) for batch in batches if batch]
                
                # 等待完成
                for future in as_completed(futures):
                    count = future.result()
                    total_processed += count
                
                print(f"当前批次处理完成，总计处理 {total_processed} 条记录")
        
        end_time = time.time()
        print(f"处理完成！总计处理 {total_processed} 条记录，耗时 {end_time - start_time:.2f} 秒")
    
    def get_category_statistics(self) -> Dict:
        """获取分类统计信息"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"""
            SELECT 类别1, 类别2, COUNT(*) as count
            FROM {self.table_name}
            WHERE 类别1 IS NOT NULL
            GROUP BY 类别1, 类别2
            ORDER BY count DESC
            """)
            
            results = cursor.fetchall()
        
        stats = {}
        for cat1, cat2, count in results:
            if cat1 not in stats:
                stats[cat1] = {}
            stats[cat1][cat2] = count
        
        return stats
    
    def print_statistics(self):
        """打印分类统计信息"""
        stats = self.get_category_statistics()
        
        print("\n=== 分类统计结果 ===")
        total = 0
        for cat1, subcats in stats.items():
            cat1_total = sum(subcats.values())
            total += cat1_total
            print(f"\n{cat1} (总计: {cat1_total})")
            for cat2, count in subcats.items():
                print(f"  {cat2}: {count}")
        
        print(f"\n总计处理记录: {total}")


def main():
    # 数据库路径 - 请根据实际情况修改
    db_path = "/mnt/d/xiaohongshu/XHS-Downloader_V2.5_Windows_X64/_internal/Download/ExploreData.db"
    table_name = "explore_data"
    
    # 创建分类器实例
    classifier = ContentClassifier(db_path, table_name)
    
    # 开始处理
    # 使用2个线程，每批处理500条记录，降低并发度
    classifier.process_all_records(max_workers=2, batch_size=500)
    
    # 打印统计信息
    classifier.print_statistics()


if __name__ == "__main__":
    main()