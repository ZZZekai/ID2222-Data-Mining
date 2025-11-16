from pyspark.sql import SparkSession
from itertools import combinations
import time

# 1. 初始化 Spark Session 和 Context
spark = SparkSession.builder \
    .appName("AprioriSpark") \
    .getOrCreate()
sc = spark.sparkContext # 获取 SparkContext

# 设定参数
TRANSACTION_FILE = 'T10I4D100K.dat'
MIN_SUPPORT = 0.01  # 最小支持度 s

# 存储所有频繁项集的字典
all_frequent_itemsets = {}

def load_data(file_path):
    """加载交易数据并返回 RDD 和总交易数"""
    print("开始加载数据...")
    
    # RDD 操作：
    transactions_rdd = sc.textFile(file_path) \
                         .filter(lambda line: line.strip()) \
                         .map(lambda line: tuple(sorted(line.strip().split())))
    
    # 为什么需要 .cache()？
    # 因为 A-Priori 是迭代算法，每一轮都需要重新扫描所有交易。
    # 缓存能将 RDD 存储在内存中，加快后续迭代的速度。
    transactions_rdd.cache()
    
    num_transactions = transactions_rdd.count()
    print(f"数据加载完成。总交易数: {num_transactions}")
    
    # 计算最小支持计数 (s * 总交易数)
    min_support_count = num_transactions * MIN_SUPPORT
    print(f"最小支持计数 (min_support_count): {min_support_count:.2f}")

    return transactions_rdd, num_transactions, min_support_count

# 主执行区调用
transactions_rdd, num_trans, min_support_count = load_data(TRANSACTION_FILE)

# 交易 RDD 格式示例: 
# [('Apple', 'Banana'), ('Bread', 'Milk', 'Eggs'), ...]

# k = 1 阶段
k = 1

# C1 生成与计数：
# 1. flatMap: 将 RDD 中的每个交易 (tuple of items) 拆分成单独的项目。
#    示例: ('A', 'B') -> [('A', 1), ('B', 1)]
# 2. reduceByKey: 汇总每个项目的计数。
#    示例: [('A', 1), ('A', 1), ...] -> [('A', 2), ...]
Ck_rdd = transactions_rdd.flatMap(lambda t: [(item, 1) for item in t]) \
                         .reduceByKey(lambda a, b: a + b) 

# Ck_rdd 格式: [('Milk', 500), ('Bread', 350), ...]

# 筛选 L1：
Lk_rdd = Ck_rdd.filter(lambda x: x[1] >= min_support_count)

# Lk_rdd (仍是 Spark RDD) 格式: [('Milk', 500), ('Bread', 350), ...]

# Lk_rdd 需要被收集到 Driver 端进行下一轮的连接和剪枝操作。
# collectAsMap() 将 RDD 转换为 Python 字典 {项集: 计数}
L1_dict = Lk_rdd.collectAsMap()

# 转换为 frozenset 键，便于存储和下一轮使用
# frozenset 是不可变集合，可以作为字典的键。
L1_frozenset = {frozenset([k]): v for k, v in L1_dict.items()}

# 存储结果并为下一轮准备
all_frequent_itemsets[k] = L1_frozenset
Lk_minus_1 = L1_frozenset # Lk-1 用于下一轮连接
k += 1

print(f"迭代 1 完成。找到 {len(L1_frozenset)} 个频繁 1-项集。")


# 循环开始，从 k=2 开始
while Lk_minus_1: 

    print(f"\n--- 迭代 {k} 开始 ---")
    
    # 1. 广播 Lk-1
    # 将上一轮的频繁项集广播到所有 Worker 节点，用于高效的剪枝
    Lk_minus_1_set = set(Lk_minus_1.keys()) # 只需要项集本身，不需要计数
    broadcast_Lk_minus_1 = sc.broadcast(Lk_minus_1_set)

    # 2. 定义 generate_and_prune 函数 (在 Worker 节点上执行)
    def generate_and_prune(transaction, k_prev_frequent_set):
        """
        在 Worker 节点对每个交易执行：生成 k-项集，并进行 A-Priori 剪枝。
        transaction: 单个交易 (tuple)
        k_prev_frequent_set: 广播的 Lk-1 集合
        """
        if len(transaction) < k:
            return [] # 交易太小，无法生成 k-项集
        
        candidates = []
        # 生成该交易中所有的 k-组合 (候选 k-项集)
        for itemset in combinations(transaction, k):
            itemset_frozenset = frozenset(itemset) # 使用 frozenset
            
            # A-Priori 剪枝核心逻辑：
            # 检查 itemset 的所有 (k-1) 子集是否都在 Lk-1 中。
            is_candidate = True
            for subset in combinations(itemset, k - 1):
                if frozenset(subset) not in k_prev_frequent_set:
                    is_candidate = False # 找到了一个非频繁的子集 -> 剪枝！
                    break
            
            if is_candidate:
                # 只有通过剪枝检查的项集才作为候选集 Ck
                candidates.append((itemset, 1)) 
                
        return candidates

    # ... (下一步是应用这个函数并继续计数)

    # 3. 分布式生成 Ck RDD (应用 generate_and_prune)
    # RDD 格式: [((item_a, item_b, ...), 1), ...]
    Ck_rdd = transactions_rdd.flatMap(
        lambda t: generate_and_prune(t, broadcast_Lk_minus_1.value)
    )

    # 4. 汇总计数
    # 使用 reduceByKey 汇总 Worker 节点上的项集计数
    count_rdd = Ck_rdd.reduceByKey(lambda a, b: a + b)

    # 5. 筛选 Lk (应用最小支持计数)
    Lk_rdd = count_rdd.filter(lambda x: x[1] >= min_support_count)
    
    # 6. 收集结果到 Driver 端
    Lk_dict = Lk_rdd.collectAsMap()
    
    # 7. 检查终止条件
    if not Lk_dict:
        print(f"迭代 {k} 发现 Lk 为空，算法终止。")
        break

    # 8. 存储并为下一轮准备
    Lk_frozenset = {frozenset(k): v for k, v in Lk_dict.items()}
    all_frequent_itemsets[k] = Lk_frozenset
    
    Lk_minus_1 = Lk_frozenset 
    print(f"迭代 {k} 完成。找到 {len(Lk_frozenset)} 个频繁 {k}-项集。")
    
    k += 1 # 准备下一轮 k+1



# 阶段四：结果展示与 Spark 停止
if num_trans > 0:
    # 使用在迭代过程中填充的 all_frequent_itemsets 字典
    frequent_sets_by_size = all_frequent_itemsets

    print("\n" + "="*80)
    print(f"--- 最终频繁项集结果 (最小支持度 s={MIN_SUPPORT*100}%) ---")
    print("="*80)
    
    total_count = sum(len(v) for v in frequent_sets_by_size.values())
    print(f"总共找到 {total_count} 个频繁项集。")
    
    # 按照项集大小 (k) 排序并打印
    for k, Lk in sorted(frequent_sets_by_size.items()):
        print(f"\n### {k}-项集 (Frequent {k}-Itemsets): 共 {len(Lk)} 个")
        
        # 将 Lk (项集: 计数) 转换为列表并按计数降序排序
        sorted_Lk = sorted(Lk.items(), key=lambda item: item[1], reverse=True)
        
        # 打印所有项集或仅打印前几项以保持简洁
        for itemset, count in sorted_Lk:
            support = count / num_trans
            # 使用 set 格式化输出项集，支持度保留四位小数
            print(f"  项集: {set(itemset)}, 支持度: {support:.4f} ({count} 次)")
        


# 阶段五：关联规则生成 (可选任务)
# 设定最小置信度 c
MIN_CONFIDENCE = 0.7 # 假设为 70%，请根据需要修改

def generate_association_rules(all_frequent_itemsets, min_confidence):
    """从频繁项集中生成关联规则"""
    
    # 规则列表: [(antecedent, consequent, support, confidence, lift)]
    rules = []
    
    # 只需要考虑 k >= 2 的频繁项集
    # items_dict: {k: {frozenset(itemset): count}}
    
    # 遍历所有频繁项集 (从 2-项集开始)
    for k in sorted(all_frequent_itemsets.keys()):
        if k < 2:
            continue
            
        Lk = all_frequent_itemsets[k] # 频繁 k-项集及其计数
        
        for itemset, support_count_I in Lk.items(): # I = X U Y
            
            # 遍历项集 I 的所有非空真子集作为前提 (X)
            # 子集长度从 1 到 k-1
            for i in range(1, k):
                for antecedent_tuple in combinations(itemset, i):
                    X = frozenset(antecedent_tuple) # 前提 X
                    Y = itemset - X                 # 结果 Y (I - X)
                    
                    # 规则: X -> Y
                    
                    # 1. 获取前提 X 的支持计数 Count(X)
                    # X 一定是 Lj 中的一个项集 (j = i)
                    support_count_X = all_frequent_itemsets[len(X)].get(X)
                    
                    if support_count_X is None:
                        # 理论上，如果 I 是频繁的，则其子集 X 必须是频繁的
                        # 这种情况不应该发生，除非数据或逻辑有误
                        continue
                        
                    # 2. 计算置信度
                    confidence = support_count_I / support_count_X
                    
                    # 3. 应用最小置信度筛选
                    if confidence >= min_confidence:
                        
                        # 4. (可选) 计算 Lift
                        # Lift(X -> Y) = Confidence(X -> Y) / Support(Y)
                        # Support(Y) = Count(Y) / num_trans
                        support_count_Y = all_frequent_itemsets[len(Y)].get(Y)
                        support_Y = support_count_Y / num_trans
                        
                        lift = confidence / support_Y if support_Y != 0 else 0
                        
                        rules.append({
                            'antecedents': X,
                            'consequents': Y,
                            'support_count': support_count_I,
                            'confidence': confidence,
                            'lift': lift
                        })
    return rules

# --- 在主执行区调用和展示 ---

print("\n" + "="*80)
print(f"--- 关联规则生成结果 (最小置信度 c={MIN_CONFIDENCE*100}%) ---")
print("="*80)

association_rules_list = generate_association_rules(all_frequent_itemsets, MIN_CONFIDENCE)

if association_rules_list:
    # 按照 Lift (提升度) 降序排序
    association_rules_list.sort(key=lambda r: r['lift'], reverse=True)
    
    for rule in association_rules_list:
        support_ratio = rule['support_count'] / num_trans # 转换回支持度比例
        
        # 格式化输出
        print(
            f"规则: {set(rule['antecedents'])} -> {set(rule['consequents'])} | "
            f"支持度: {support_ratio:.4f} | 置信度: {rule['confidence']:.4f} | "
            f"提升度: {rule['lift']:.4f}"
        )
else:
    print("没有发现满足最小置信度要求的关联规则。")


# 停止 Spark Session (释放资源)
sc.stop()