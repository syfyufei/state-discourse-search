"""
国家话语数据集 - 搜索索引构建脚本
State Discourse Dataset - Search Index Builder

运行此脚本，将全部数据预处理为前端可用的 JSON 搜索索引。
索引只包含文本片段（每条最多200字），不包含完整原文，保护数据。

使用方法:
    cd StateDiscourseDataset
    pip install pandas openpyxl
    python search/build_index.py

完成后，用浏览器打开 search/index.html 即可使用搜索功能。
"""

import os
import glob
import json
import re
import hashlib
from pathlib import Path
import pandas as pd

DATA_ROOT = Path(__file__).parent.parent / 'data'
OUTPUT_PATH = Path(__file__).parent / 'search_index.json'

# 每条记录保留的最大字符数（保护完整原文）
SNIPPET_LENGTH = 120
# 文本最短长度（过滤标题行、空行等噪音）
MIN_TEXT_LENGTH = 40
# 每个文件最多保留的记录数（均匀采样，保证每个文件都有代表性片段）
MAX_ROWS_PER_FILE = 200


def make_snippet(text, max_len=SNIPPET_LENGTH):
    """截取文本片段，去除多余空白"""
    text = re.sub(r'\s+', ' ', str(text)).strip()
    if len(text) > max_len:
        text = text[:max_len] + '…'
    return text


def make_id(text):
    """生成短 ID"""
    return hashlib.md5(text.encode('utf-8', errors='replace')).hexdigest()[:8]


records = []


def add_csv_records(path, category, text_col, title_col=None, date_col=None):
    """从 CSV 文件添加记录"""
    try:
        df = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
        for _, row in df.iterrows():
            text = str(row.get(text_col, ''))
            if not text or text == 'nan' or len(text) < MIN_TEXT_LENGTH:
                continue
            title = str(row.get(title_col, '')) if title_col else ''
            date = str(row.get(date_col, '')) if date_col else ''
            records.append({
                'id': make_id(text[:50]),
                'category': category,
                'title': title[:100],
                'date': date[:20],
                'source': os.path.basename(path),
                'snippet': make_snippet(text),
            })
        print(f"  [{category}] {os.path.basename(path)}: {len(df)} 条")
    except Exception as e:
        print(f"  跳过 {path}: {e}")


def add_excel_records(path, category, text_col_idx=0, name_col_idx=1, date_col_idx=None):
    """从 Excel 文件添加记录（均匀采样，最多 MAX_ROWS_PER_FILE 条）"""
    try:
        df = pd.read_excel(path, header=None, engine='openpyxl')
        filename = os.path.basename(path)

        # 过滤有效行
        valid = []
        for _, row in df.iterrows():
            if len(row) <= text_col_idx:
                continue
            text = str(row.iloc[text_col_idx])
            if not text or text == 'nan' or len(text) < MIN_TEXT_LENGTH:
                continue
            valid.append(row)

        # 均匀采样
        if len(valid) > MAX_ROWS_PER_FILE:
            step = len(valid) / MAX_ROWS_PER_FILE
            valid = [valid[int(i * step)] for i in range(MAX_ROWS_PER_FILE)]

        for row in valid:
            text = str(row.iloc[text_col_idx])
            name = str(row.iloc[name_col_idx]) if len(row) > name_col_idx else filename
            date = str(row.iloc[date_col_idx]) if (date_col_idx and len(row) > date_col_idx) else ''
            records.append({
                'id': make_id(text[:50] + filename),
                'category': category,
                'title': (name[:100] if name != 'nan' else filename),
                'date': date[:20] if date != 'nan' else '',
                'source': filename,
                'snippet': make_snippet(text),
            })
        print(f"  [{category}] {filename}: {len(valid)} 条")
    except Exception as e:
        print(f"  跳过 {path}: {e}")


print("正在构建搜索索引...")
print("=" * 50)

# ── 1. CSV 文件 ──────────────────────────────────
print("\n[1/9] 习近平系列重要讲话数据库")
xi_csv = DATA_ROOT / '习近平系列重要讲话数据库.csv'
if xi_csv.exists():
    add_csv_records(xi_csv, '习近平讲话', text_col='中文', title_col='标题', date_col='发布时间')
else:
    print(f"  未找到文件：{xi_csv}")

print("\n[2/9] 人民网政绩观")
rmrb_csv = DATA_ROOT / '人民网_政绩观.csv'
if rmrb_csv.exists():
    add_csv_records(rmrb_csv, '人民网政绩观', text_col='full_text', title_col='title', date_col='date')
else:
    print(f"  未找到文件：{rmrb_csv}")

# ── 2. Excel 文件：按实际目录结构映射 ────────────

# 领导人著作：多个独立目录
print("\n[3/9] 领导人著作选集")
leader_dirs = ['毛泽东选集', '刘少奇选集', '朱德选集', '周恩来选集', '陈云文选',
               '邓小平文选', '江泽民文选', '习近平谈治国理政', '习近平总书记论述摘编']
for d in leader_dirs:
    p = DATA_ROOT / d
    if p.exists():
        for f in sorted(glob.glob(str(p / '**' / '*.xlsx'), recursive=True)):
            add_excel_records(f, '领导人著作')

# 党代会：目录名前缀为"党代会-"
print("\n[4/9] 全国党代会文件")
for d in sorted(DATA_ROOT.iterdir()):
    if d.is_dir() and d.name.startswith('党代会'):
        for f in sorted(glob.glob(str(d / '**' / '*.xlsx'), recursive=True)):
            add_excel_records(f, '党代会')

# 两会：目录名后缀为"两会"
print("\n[5/9] 全国两会文件")
for d in sorted(DATA_ROOT.iterdir()):
    if d.is_dir() and d.name.endswith('两会'):
        for f in sorted(glob.glob(str(d / '**' / '*.xlsx'), recursive=True)):
            add_excel_records(f, '两会')

# 白皮书
print("\n[6/9] 政府白皮书")
wp_dir = DATA_ROOT / '政府白皮书'
if wp_dir.exists():
    for f in sorted(glob.glob(str(wp_dir / '**' / '*.xlsx'), recursive=True)):
        add_excel_records(f, '白皮书')

# 党规党史与五年规划
print("\n[7/9] 党规党史与五年规划")
for d in ['党规', '党史', '五年规划']:
    p = DATA_ROOT / d
    if p.exists():
        for f in sorted(glob.glob(str(p / '**' / '*.xlsx'), recursive=True)):
            add_excel_records(f, '党规党史')

# 意识形态：根目录下的散落 xlsx 文件
print("\n[8/9] 意识形态文件")
ideology_files = ['科学发展观 句对照.xlsx',
                  '中共中央　国务院关于完整准确全面贯彻新发展理念做好碳达峰碳中和工作的意见.xlsx',
                  '2030年前碳达峰行动方案.xlsx']
for fname in ideology_files:
    f = DATA_ROOT / fname
    if f.exists():
        add_excel_records(str(f), '意识形态')

# 求是
print("\n[9/9] 求是杂志")
qiushi_dir = DATA_ROOT / '《求是》'
if qiushi_dir.exists():
    for f in sorted(glob.glob(str(qiushi_dir / '**' / '*.xlsx'), recursive=True)):
        add_excel_records(f, '求是')

# ── 输出 ─────────────────────────────────────────
print("\n" + "=" * 50)
print(f"索引构建完成：共 {len(records)} 条记录")

# 去重（按 id）
seen = set()
unique_records = []
for r in records:
    if r['id'] not in seen:
        seen.add(r['id'])
        unique_records.append(r)

print(f"去重后：{len(unique_records)} 条记录")

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
    json.dump(unique_records, f, ensure_ascii=False, indent=None, separators=(',', ':'))

size_kb = OUTPUT_PATH.stat().st_size / 1024
print(f"已保存至：{OUTPUT_PATH}（{size_kb:.0f} KB）")
print("\n现在可以用浏览器打开 search/index.html 使用搜索功能。")
