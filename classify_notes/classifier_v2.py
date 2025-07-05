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
            1: {'name': '日常生活', 'keywords': ['日常', '习惯', '作息', '居家', '家务', '洗衣', '清洁', '整理']},
            2: {'name': '美食烹饪', 'keywords': ['美食', '烹饪', '菜谱', '做菜', '食物', '餐厅', '吃饭', '料理', '食材', '味道']},
            3: {'name': '购物消费', 'keywords': ['购物', '消费', '买', '商品', '价格', '优惠', '折扣', '商场', '网购']},
            4: {'name': '居住装修', 'keywords': ['租房', '房子', '住房', '装修', '家居', '搬家', '房租', '小区', '物业']},
            5: {'name': '宠物养护', 'keywords': ['宠物', '动物', '猫', '狗', '养宠', '萌宠', '饲养', '照顾', '训练']}
        }
    },
    2: {
        'name': '娱乐文化',
        'subcategories': {
            1: {'name': '影视娱乐', 'keywords': ['电影', '电视', '综艺', '明星', '演员', '导演', '剧情', '影院', '追剧']},
            2: {'name': '游戏动漫', 'keywords': ['游戏', '动漫', '漫画', '手游', '网游', '主机', '动画', '二次元', '角色']},
            3: {'name': '音乐表演', 'keywords': ['音乐', '歌曲', '歌手', '演唱会', '唱歌', 'ktv', '演唱', '乐器演奏', '声乐']},
            4: {'name': '网络文化', 'keywords': ['梗', '表情包', '弹幕', '直播', '网红', '短视频', '社交媒体', '流行']},
            5: {'name': '创意表演', 'keywords': ['舞蹈', '跳舞', '中国舞', '古典舞', 'kpop', '街舞', 'cos', 'cosplay', '角色扮演']}
        }
    },
    3: {
        'name': '旅游出行',
        'subcategories': {
            1: {'name': '旅游攻略', 'keywords': ['旅游', '攻略', '景点', '路线', '住宿', '酒店', '民宿', '旅行', '度假']},
            2: {'name': '公共交通', 'keywords': ['地铁', '公交', '高铁', '火车', '航班', '机场', '车站', '班次', '票务']},
            3: {'name': '城市探索', 'keywords': ['城市', '探索', '街道', '建筑', '商圈', '夜生活', '本地']},
            4: {'name': '户外活动', 'keywords': ['户外', '登山', '徒步', '露营', '钓鱼', '骑行', '野外', '探险']},
            5: {'name': '景点体验', 'keywords': ['景点', '体验', '游玩', '参观', '门票', '拍照', '风景', '名胜', '古迹']}
        }
    },
    4: {
        'name': '教育学习',
        'subcategories': {
            1: {'name': '基础教育', 'keywords': ['学校', '小学', '中学', '高中', '学生', '老师', '课程', '成绩', '作业']},
            2: {'name': '高等教育', 'keywords': ['大学', '学院', '专业', '毕业', '学位', '导师', '论文', '学术研究']},
            3: {'name': '考试备考', 'keywords': ['考试', '备考', '复习', '刷题', '考研', '高考', '公务员', '资格考试']},
            4: {'name': '语言学习', 'keywords': ['英语', '雅思', '托福', '外语', '口语', '翻译', '语言交流', '多语种']},
            5: {'name': '在线教育', 'keywords': ['网课', '在线学习', '知识分享', '教学视频', '学习平台', '慕课']}
        }
    },
    5: {
        'name': '职场发展',
        'subcategories': {
            1: {'name': '求职就业', 'keywords': ['找工作', '面试', '简历', '求职', '招聘', '实习', '校招', '跳槽']},
            2: {'name': '职场生活', 'keywords': ['工作', '职场', '公司', '同事', '老板', '薪资', '加班', '会议', '项目']},
            3: {'name': '技能提升', 'keywords': ['技能培训', '职业技能', '证书', '资格认证', '专业能力', '职业规划']},
            4: {'name': '创业经营', 'keywords': ['创业', '创业故事', '初创企业', '商业模式', '市场策略', '团队管理']},
            5: {'name': '行业分析', 'keywords': ['行业趋势', '市场研究', '商业观察', '企业分析', '职业发展']}
        }
    },
    6: {
        'name': '科技数码',
        'subcategories': {
            1: {'name': '数码设备', 'keywords': ['手机', '电脑', '数码', '产品', '配置', '性能', '品牌', '型号', '评测']},
            2: {'name': '软件应用', 'keywords': ['软件', '应用', 'APP', '程序', '工具', '系统', '操作', '功能', '使用']},
            3: {'name': '编程开发', 'keywords': ['编程', '代码', '开发', '算法', '程序设计', '软件开发', '技术实现']},
            4: {'name': '人工智能', 'keywords': ['人工智能', 'AI', '机器学习', '深度学习', '模型', '智能化', '自动化']},
            5: {'name': '科学知识', 'keywords': ['科学', '原理', '实验', '发现', '研究', '理论', '创新', '科普']}
        }
    },
    7: {
        'name': '医疗健康',
        'subcategories': {
            1: {'name': '疾病治疗', 'keywords': ['疾病', '治疗', '医生', '医院', '药物', '手术', '病症', '诊断']},
            2: {'name': '健康养生', 'keywords': ['养生', '保健', '营养', '减肥', '健康生活', '预防', '调理']},
            3: {'name': '心理健康', 'keywords': ['心理', '情绪', '压力', '焦虑', '抑郁', '心情', '心理治疗', '心理咨询']},
            4: {'name': '急救安全', 'keywords': ['急救', '救援', '应急处理', '医疗急救', '生命安全', '紧急情况']},
            5: {'name': '专科医疗', 'keywords': ['眼科', '干眼症', '视力', '外伤', '创伤', '疤痕', '专科治疗']}
        }
    },
    8: {
        'name': '交通驾驶',
        'subcategories': {
            1: {'name': '驾照考试', 'keywords': ['学车', '考驾照', '驾校', '教练', '科目二', '科目三', '科目四', '练车']},
            2: {'name': '驾驶技能', 'keywords': ['安全驾驶', '防御性驾驶', '新手司机', '行车技巧', '驾驶经验']},
            3: {'name': '车辆保养', 'keywords': ['汽车保养', '车载用品', '汽车维修', '保养知识', '车辆维护']},
            4: {'name': '交通规则', 'keywords': ['交通规则', '交通法规', '违章', '罚款', '扣分', '交通标识']},
            5: {'name': '电动出行', 'keywords': ['电动车', '小电驴', '摩托车', '电动汽车', '新能源车', '充电']}
        }
    },
    9: {
        'name': '运动健身',
        'subcategories': {
            1: {'name': '健身锻炼', 'keywords': ['健身', '锻炼', '器械', '肌肉', '体型', '力量训练', '健身房']},
            2: {'name': '有氧运动', 'keywords': ['跑步', '游泳', '有氧', '减脂', '心肺', '耐力', '马拉松']},
            3: {'name': '瑜伽舞蹈', 'keywords': ['瑜伽', '普拉提', '舞蹈', '柔韧性', '体态', '身体协调']},
            4: {'name': '球类运动', 'keywords': ['篮球', '足球', '网球', '乒乓球', '羽毛球', '球类', '团队运动']},
            5: {'name': '运动康复', 'keywords': ['运动康复', '运动损伤', '康复训练', '运动医学', '体能恢复']}
        }
    },
    10: {
        'name': '情感社交',
        'subcategories': {
            1: {'name': '恋爱关系', 'keywords': ['恋爱', '情感', '男女', '感情', '分手', '表白', '约会', '恋人']},
            2: {'name': '婚姻家庭', 'keywords': ['婚姻', '结婚', '家庭', '夫妻', '婆媳', '家人关系', '家庭和谐']},
            3: {'name': '亲子教育', 'keywords': ['父母', '孩子', '亲子', '教育孩子', '育儿', '家庭教育', '成长']},
            4: {'name': '友情社交', 'keywords': ['友情', '朋友', '聚会', '交友', '圈子', '人际关系', '社交']},
            5: {'name': '个人成长', 'keywords': ['个人成长', '自我提升', '改变', '进步', '反思', '目标', '人生规划']}
        }
    },
    11: {
        'name': '兴趣爱好',
        'subcategories': {
            1: {'name': '手工制作', 'keywords': ['手工', '创作', 'DIY', '制作', '工艺', '设计', '创意作品']},
            2: {'name': '收藏鉴赏', 'keywords': ['收藏', '古董', '文物', '藏品', '鉴定', '价值', '珍藏', '收藏品']},
            3: {'name': '摄影艺术', 'keywords': ['摄影', '拍摄', '相机', '构图', '后期', '摄影技巧', '艺术摄影']},
            4: {'name': '书法绘画', 'keywords': ['书法', '绘画', '国画', '油画', '素描', '艺术创作', '美术']},
            5: {'name': '园艺植物', 'keywords': ['园艺', '植物', '花卉', '种植', '养花', '绿植', '花园']}
            # 加一个类别：美女
            ,6: {'name': '美女', 'keywords': ['美女', '漂亮', '身材', '美背', '长腿', '性感']}

        }
    },
    12: {
        'name': '文化艺术',
        'subcategories': {
            1: {'name': '传统文化', 'keywords': ['传统文化', '非遗', '传统技艺', '文化传承', '民族文化', '古典文化']},
            2: {'name': '文学阅读', 'keywords': ['文学', '书籍', '小说', '诗歌', '读书', '书评', '阅读', '文学作品']},
            3: {'name': '历史文化', 'keywords': ['历史', '古代', '典故', '文化遗产', '考古', '历史事件', '文化背景']},
            4: {'name': '博物展览', 'keywords': ['博物馆', '展览', '文物', '艺术展', '文化展示', '参观学习']},
            5: {'name': '节庆民俗', 'keywords': ['传统节日', '民俗', '节庆', '习俗', '庆典', '文化活动', '民间文化']}
        }
    },
    13: {
        'name': '法律维权',
        'subcategories': {
            1: {'name': '消费维权', 'keywords': ['消费维权', '投诉', '退费', '消费者权益', '商家纠纷', '服务质量']},
            2: {'name': '合同纠纷', 'keywords': ['合同', '协议', '违约', '法律条款', '合同纠纷', '法律责任']},
            3: {'name': '诈骗防范', 'keywords': ['诈骗', '反诈', '骗子', '电信诈骗', '网络诈骗', '防骗知识']},
            4: {'name': '法律咨询', 'keywords': ['法律', '律师', '法规', '权利', '法律程序', '法律援助']},
            5: {'name': '刑事民事', 'keywords': ['起诉', '法庭', '诉讼', '判决', '案件', '法律制裁']}
        }
    },
    14: {
        'name': '金融理财',
        'subcategories': {
            1: {'name': '投资理财', 'keywords': ['投资', '理财', '股票', '基金', '收益', '财富管理', '资产配置']},
            2: {'name': '银行服务', 'keywords': ['银行', '信用卡', '贷款', '存款', '利率', '银行业务']},
            3: {'name': '保险保障', 'keywords': ['保险', '保障', '理赔', '保险产品', '风险管理', '保费']},
            4: {'name': '房产投资', 'keywords': ['房产', '房价', '买房', '房地产', '房产投资', '房贷']},
            5: {'name': '经济分析', 'keywords': ['经济', '财经', '宏观经济', '金融市场', '经济趋势', '市场分析']}
        }
    },
    15: {
        'name': '社会话题',
        'subcategories': {
            1: {'name': '时事新闻', 'keywords': ['新闻', '时事', '热点', '事件', '报道', '社会新闻', '国内外']},
            2: {'name': '社会现象', 'keywords': ['社会现象', '社会问题', '争议', '话题', '趋势', '社会观察']},
            3: {'name': '政策制度', 'keywords': ['政策', '制度', '规定', '法规', '政府', '公告', '改革措施']},
            4: {'name': '公共服务', 'keywords': ['公共服务', '便民服务', '政务服务', '社区服务', '民生']},
            5: {'name': '环境生态', 'keywords': ['环保', '环境保护', '生态', '气候变化', '可持续发展', '绿色生活']}
        }
    },
    16: {
        'name': '安全防护',
        'subcategories': {
            1: {'name': '交通安全', 'keywords': ['交通安全', '交通事故', '道路安全', '行车安全', '交通违法']},
            2: {'name': '消防安全', 'keywords': ['消防', '火灾', '灭火', '安全通道', '防火', '消防知识']},
            3: {'name': '工作安全', 'keywords': ['工地安全', '施工安全', '安全帽', '高空作业', '职业安全']},
            4: {'name': '自然灾害', 'keywords': ['地震', '洪水', '台风', '自然灾害', '应急避险', '灾害防范']},
            5: {'name': '水域安全', 'keywords': ['溺水', '游泳安全', '水上安全', '救生', '防溺水']}
        }
    },
    17: {
        'name': '地域文化',
        'subcategories': {
            1: {'name': '城市生活', 'keywords': ['都市', '市区', '社区', '邻里', '城市文化', '都市生活']},
            2: {'name': '乡村生活', 'keywords': ['乡村', '农村', '田园', '农业', '种植', '养殖', '乡村文化']},
            3: {'name': '地方特色', 'keywords': ['地方特色', '特产', '风味', '地方文化', '区域特点']},
            4: {'name': '方言文化', 'keywords': ['方言', '语言', '口音', '土话', '俚语', '地方话']},
            5: {'name': '区域比较', 'keywords': ['区域对比', '地区差异', '南北差异', '东西差别', '地域特点']}
        }
    },
    18: {
        'name': '国际视野',
        'subcategories': {
            1: {'name': '留学生活', 'keywords': ['留学', '海外留学', '留学生', '国外大学', '留学申请', '海外学习']},
            2: {'name': '移民签证', 'keywords': ['移民', '签证', '绿卡', '永居', '移民申请', '移民政策']},
            3: {'name': '海外工作', 'keywords': ['海外工作', '国外就业', '海外职场', '外企', '跨国公司']},
            4: {'name': '国际交流', 'keywords': ['国际交流', '跨文化', '文化差异', '国际合作', '多元文化']},
            5: {'name': '国际资讯', 'keywords': ['国际新闻', '国际关系', '外交', '全球化', '国际动态']}
        }
    },
    19: {
        'name': '生活技巧',
        'subcategories': {
            1: {'name': '实用技巧', 'keywords': ['生活技巧', '实用窍门', '小贴士', '妙招', '诀窍', '生活攻略']},
            2: {'name': '搞笑幽默', 'keywords': ['搞笑', '幽默', '笑话', '段子', '逗乐', '娱乐', '轻松']},
            3: {'name': '奇闻趣事', 'keywords': ['奇闻', '趣事', '罕见', '惊奇', '新奇', '有趣']},
            4: {'name': '随感杂谈', 'keywords': ['随感', '杂谈', '感想', '想法', '闲聊', '随笔']},
            5: {'name': '其他杂项', 'keywords': ['其他', '未分类', '杂项', '无法归类', '综合']}
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
        print("正在检查和添加分类列...")
        return True
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 检查列是否已存在
                columns = self.get_table_structure()
                print(f"当前表列: {columns}")
                
                changes_made = False
                if '类别1' not in columns:
                    cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别1 TEXT")
                    changes_made = True
                    print("添加了 类别1 列")
                if '类别2' not in columns:
                    cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别2 TEXT")
                    changes_made = True
                    print("添加了 类别2 列")
                if '类别1_ID' not in columns:
                    cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别1_ID INTEGER")
                    changes_made = True
                    print("添加了 类别1_ID 列")
                if '类别2_ID' not in columns:
                    cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN 类别2_ID INTEGER")
                    changes_made = True
                    print("添加了 类别2_ID 列")
                
                if changes_made:
                    conn.commit()
                    print("分类列添加完成")
                else:
                    print("分类列已存在，无需添加")
        except Exception as e:
            print(f"添加分类列时出错: {e}")
            raise
    
    def get_unprocessed_records(self, batch_size: int = 1000) -> List[Tuple]:
        """获取未处理的记录"""
        print(f"正在获取未处理的记录，批大小: {batch_size}")
        try:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 先检查表中总记录数
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                total_count = cursor.fetchone()[0]
                print(f"表中总记录数: {total_count}")
                
                # 检查未分类记录数
                cursor.execute(f"""
                SELECT COUNT(*) FROM {self.table_name} 
                WHERE 类别1 IS NULL OR 类别1 = ''
                """)
                unprocessed_count = cursor.fetchone()[0]
                print(f"未分类记录数: {unprocessed_count}")
                
                if unprocessed_count == 0:
                    print("所有记录都已分类完成")
                    return []
                
                # 获取所有未分类的记录
                query = f"""
                SELECT rowid, 作品标题, 作品标签, 作品ID
                FROM {self.table_name} 
                WHERE 类别1 IS NULL OR 类别1 = ''
                LIMIT {batch_size}
                """
                
                cursor.execute(query)
                records = cursor.fetchall()
                print(f"本批次获取到 {len(records)} 条记录")
                
                # 显示前几条记录的内容作为调试
                if records:
                    print("前3条记录示例:")
                    for i, record in enumerate(records[:3]):
                        rowid, title, tags, work_id = record
                        print(f"  {i+1}. rowid={rowid}, 标题='{title}', 标签='{tags}'")
        
            return records
        except Exception as e:
            print(f"获取未处理记录时出错: {e}")
            raise
    
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
    
    def process_all_records(self, max_workers: int = 1, batch_size: int = 100):
        """多线程处理所有记录 - 使用更保守的参数"""
        print("开始处理数据库记录...")
        
        # 添加分类列
        self.add_category_columns()
        
        total_processed = 0
        start_time = time.time()
        batch_count = 0
        
        # 使用更保守的线程设置
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                batch_count += 1
                print(f"\n=== 处理第 {batch_count} 批 ===")
                
                # 获取未处理的记录
                records = self.get_unprocessed_records(batch_size)
                
                if not records:
                    print("没有更多未处理的记录")
                    break
                
                # 单线程或小批量多线程处理
                if max_workers == 1:
                    # 单线程处理
                    count = self.process_batch(records)
                    total_processed += count
                else:
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
                
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                print(f"第 {batch_count} 批处理完成，总计处理 {total_processed} 条记录")
                print(f"处理速度: {rate:.1f} 条/秒，耗时: {elapsed:.1f} 秒")
                
                # 每处理几批记录就休息一下，避免过度占用资源
                if batch_count % 5 == 0:
                    print("短暂休息 1 秒...")
                    time.sleep(1)
        
        end_time = time.time()
        total_time = end_time - start_time
        avg_rate = total_processed / total_time if total_time > 0 else 0
        print(f"\n🎉 处理完成！")
        print(f"总计处理 {total_processed} 条记录")
        print(f"总耗时 {total_time:.1f} 秒")
        print(f"平均速度 {avg_rate:.1f} 条/秒")
    
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
    # 从调试信息得到的正确数据库路径
    db_path = "/mnt/d/xiaohongshu/XHS-Downloader_V2.5_Windows_X64/_internal/Download/ExploreData.db"
    table_name = "explore_data"
    
    print(f"数据库路径: {db_path}")
    print(f"表名: {table_name}")
    
    # 检查数据库文件是否存在
    import os
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件不存在: {db_path}")
        return
    
    # 创建分类器实例
    print("正在初始化分类器...")
    try:
        classifier = ContentClassifier(db_path, table_name)
        print("分类器初始化完成")
        
        # 测试数据库连接
        print("正在测试数据库连接...")
        with classifier.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print(f"数据库中的表: {[table[0] for table in tables]}")
            
            if table_name not in [table[0] for table in tables]:
                print(f"错误: 表 '{table_name}' 不存在")
                return
        
        # 开始处理 - 使用单线程，小批量处理确保稳定性
        print("开始处理分类任务...")
        classifier.process_all_records(max_workers=1, batch_size=100)
        
        # 打印统计信息
        classifier.print_statistics()
        
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()