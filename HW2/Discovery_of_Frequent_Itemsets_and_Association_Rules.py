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

# 存储所有频繁项集的字典: {k: Lk}
all_frequent_itemsets = {}

def load_data(file_path):
    """加载交易数据并返回 RDD 和总交易数"""
    print("--- Starting data loading ---")
    
    # RDD 操作：
    transactions_rdd = sc.textFile(file_path) \
                         .filter(lambda line: line.strip()) \
                         .map(lambda line: tuple(sorted(line.strip().split())))
    
    # 缓存 RDD 以便多次迭代使用 (A-Priori 迭代的核心优化)
    transactions_rdd.cache()
    
    num_transactions = transactions_rdd.count()
    print(f"Data loaded successfully. Total transactions: {num_transactions}")
    
    # 计算最小支持计数 (s * 总交易数)
    min_support_count = num_transactions * MIN_SUPPORT
    print(f"Minimum support count (min_support_count): {min_support_count:.2f}")

    return transactions_rdd, num_transactions, min_support_count

# 主执行区：数据加载
transactions_rdd, num_trans, min_support_count = load_data(TRANSACTION_FILE)

# 阶段 I：频繁项集挖掘 (A-Priori 算法)

# k = 1 阶段
k = 1

# C1 生成与计数：
# 1. flatMap: 将 RDD 中的每个交易拆分成 (item, 1) 键值对。
# 2. reduceByKey: 汇总每个项目的计数 (分布式支持计数)。
Ck_rdd = transactions_rdd.flatMap(lambda t: [(item, 1) for item in t]) \
                         .reduceByKey(lambda a, b: a + b) 

# 筛选 L1：保留支持计数 >= 最小支持计数的项集
Lk_rdd = Ck_rdd.filter(lambda x: x[1] >= min_support_count)

# Action: 收集 L1 结果到 Driver 端 (转换为 Python 字典)
L1_dict = Lk_rdd.collectAsMap()

# 转换为 frozenset 键，便于存储和下一轮使用
L1_frozenset = {frozenset([k]): v for k, v in L1_dict.items()}

# 存储 L1 并设置 Lk-1 用于下一轮连接
all_frequent_itemsets[k] = L1_frozenset
Lk_minus_1 = L1_frozenset # Lk-1 用于下一轮剪枝
k += 1

print(f"Iteration 1 complete. Found {len(L1_frozenset)} frequent 1-itemsets.")


# 迭代循环开始 (从 k=2 开始)
while Lk_minus_1: 

    print(f"\n--- Iteration {k} starts ---")
    
    # 1. 广播 Lk-1：将上一轮的频繁项集广播到所有 Worker 节点，用于高效剪枝
    Lk_minus_1_set = set(Lk_minus_1.keys()) 
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
            itemset_frozenset = frozenset(itemset) 
            
            # A-Priori 剪枝核心逻辑：检查 itemset 的所有 (k-1) 子集是否都在 Lk-1 中。
            is_candidate = True
            for subset in combinations(itemset, k - 1):
                if frozenset(subset) not in k_prev_frequent_set:
                    is_candidate = False # 找到了一个非频繁的子集 -> 剪枝！
                    break
            
            if is_candidate:
                # 只有通过剪枝检查的项集才作为候选集 Ck
                candidates.append((itemset, 1)) 
                
        return candidates

    # 3. 分布式 Ck 生成 (使用 flatMap)
    Ck_rdd = transactions_rdd.flatMap(
        lambda t: generate_and_prune(t, broadcast_Lk_minus_1.value)
    )

    # 4. 汇总计数
    # 使用 reduceByKey 汇总 Worker 节点上的项集计数
    count_rdd = Ck_rdd.reduceByKey(lambda a, b: a + b)

    # 5. 筛选 Lk (应用最小支持计数)
    Lk_rdd = count_rdd.filter(lambda x: x[1] >= min_support_count)
    
    # 6. Action: 收集结果到 Driver
    Lk_dict = Lk_rdd.collectAsMap()
    
    # 7. 检查终止条件
    if not Lk_dict:
        print(f"Iteration {k} found Lk is empty. Algorithm terminates.")
        break

    # 8. 存储 Lk 并为下一轮做准备
    Lk_frozenset = {frozenset(k): v for k, v in Lk_dict.items()}
    all_frequent_itemsets[k] = Lk_frozenset
    
    Lk_minus_1 = Lk_frozenset 
    print(f"Iteration {k} complete. Found {len(Lk_frozenset)} frequent {k}-itemsets.")
    
    k += 1 # 准备下一轮 k+1


# 阶段四：结果展示
if num_trans > 0:
    frequent_sets_by_size = all_frequent_itemsets

    print("\n" + "="*80)
    print(f"--- FINAL FREQUENT ITEMSETS (Min Support s={MIN_SUPPORT*100}%) ---")
    print("="*80)
    
    total_count = sum(len(v) for v in frequent_sets_by_size.values())
    print(f"Total frequent itemsets found: {total_count}.")
    
    # 按照项集大小 (k) 排序并打印
    for k, Lk in sorted(frequent_sets_by_size.items()):
        print(f"\n### {k}-Itemsets (Frequent {k}-Itemsets): Total {len(Lk)} sets")
        
        # 按支持度计数降序排序
        sorted_Lk = sorted(Lk.items(), key=lambda item: item[1], reverse=True)
        
        # 打印结果
        for itemset, count in sorted_Lk:
            support = count / num_trans
            # 格式化输出
            print(f"  Itemset: {set(itemset)}, Support: {support:.4f} ({count} counts)")
        


# 阶段五：关联规则生成 (可选任务 - 优化版，使用置信度剪枝)
# 设定最小置信度 c
MIN_CONFIDENCE = 0.7 

def generate_association_rules_optimized(all_frequent_itemsets, num_trans, min_confidence):
    """
    从频繁项集中生成关联规则 X -> Y，并使用置信度单调性原理进行剪枝。
    rules = [
        {
            'antecedents': X, 'consequents': Y, 'support_count': count, 
            'confidence': conf, 'lift': lift
        }, 
        ...
    ]
    """
    
    rules = []
    
    # 只考虑 k >= 2 的频繁项集
    for k in sorted(all_frequent_itemsets.keys()):
        if k < 2:
            continue
            
        Lk = all_frequent_itemsets[k] 
        
        # 遍历每个频繁 k-项集 I
        for itemset, support_count_I in Lk.items(): 
            
            # 初始化：H_k_results 存储当前项集 I 中所有合格的规则结果 Y (consequents)
            # 初始时，Y 是所有长度为 1 的子集。
            H_k_results = [frozenset([item]) for item in itemset]
            
            # 迭代生成规则：结果 Y 的长度从 m=1 递增到 k-1
            for m in range(1, k):
                
                next_H_k_results = set()
                
                # 遍历当前长度 m 的合格结果 Y
                for Y in H_k_results:
                    X = itemset - Y # 对应的规则前提 X
                    
                    # 1. 获取前提 X 的支持计数 Count(X)
                    support_count_X = all_frequent_itemsets[len(X)].get(X)
                    
                    # 2. 计算置信度 Confidence = Count(I) / Count(X)
                    # 因为 I 是频繁的，所以 X 也是频繁的，Count(X) > 0
                    confidence = support_count_I / support_count_X
                    
                    # 3. 应用最小置信度筛选
                    if confidence >= min_confidence:
                        
                        # **规则合格：** 立即加入结果列表
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
                        
                        # 4. 如果规则合格，则尝试生成下一层更长的结果 Y' (即 m+1)
                        # Y' 是通过在 Y 中增加一个 X 中的元素得到的
                        if m < k - 1:
                            for item in X: # 用 X 中的元素来扩展 Y
                                Y_prime = Y | frozenset([item])
                                next_H_k_results.add(Y_prime)
                                
                    # 规则不合格 (剪枝)：
                    # 如果 Confidence(X -> Y) 不合格，则任何 X' -> Y' 的规则都不再从 Y 衍生
                    # 我们通过不将 Y 加入 next_H_k_results 来实现剪枝。

                # 更新下一轮要检查的结果 Y (Y' 成为新的 Y)
                H_k_results = next_H_k_results
                
                # 如果当前层没有合格的 Y，直接终止对当前 itemset 的规则生成
                if not H_k_results:
                    break
            
    return rules

# --- 在主执行区调用和展示 ---

print("\n" + "="*80)
print(f"--- ASSOCIATION RULE GENERATION (Min Confidence c={MIN_CONFIDENCE*100}%) ---")
print("="*80)

# *** 替换函数调用：使用优化后的版本 ***
association_rules_list = generate_association_rules_optimized(all_frequent_itemsets, num_trans, MIN_CONFIDENCE)

if association_rules_list:
    # 按照 Lift (提升度) 降序排序
    association_rules_list.sort(key=lambda r: r['lift'], reverse=True)
    
    for rule in association_rules_list:
        support_ratio = rule['support_count'] / num_trans # 转换回支持度比例
        
        # 格式化输出
        print(
            f"Rule: {set(rule['antecedents'])} -> {set(rule['consequents'])} | "
            f"Support: {support_ratio:.4f} | Confidence: {rule['confidence']:.4f} | "
            f"Lift: {rule['lift']:.4f}"
        )
else:
    print("No association rules found satisfying the minimum confidence threshold.")


# 停止 Spark Session (释放资源)
sc.stop()