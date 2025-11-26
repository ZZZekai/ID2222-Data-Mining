import random
from collections import defaultdict
import time



# 模块 1: 水塘抽样
class ReservoirSampler:
    """
    水塘抽样器：
    维护一个固定大小 k 的样本集合。
    支持流式数据的添加，保证均匀随机性。
    """
    def __init__(self, k):
        # 1. k: 水塘（样本）的最大容量
        self.k = k
        # 2. reservoir: 实际存储样本边的地方 (对应算法中的 M)
        self.reservoir = []
        # 3. t: 看到的总边数计数器 (对应算法中的 t)
        self.t = 0

    def process_edge(self, edge):
        """
        处理一条新到达的边。
        
        参数:
            edge: 新到达的边，例如元组 (u, v)
            
        返回:
            tuple (added, removed_edge)
            - added (bool): 新边是否被加入到了水塘中
            - removed_edge (object/None): 如果发生了替换，返回被移除的那条旧边；否则返回 None
        """
        self.t += 1
        
        # 情况 A: 水塘未满
        # 只要没满，直接加入，不移除任何元素
        if len(self.reservoir) < self.k:
            self.reservoir.append(edge)
            return True, None

        # 情况 B: 水塘已满 (概率替换)
        # 计算当前时刻的采样概率 P(t) = k / t
        p = self.k / self.t
        
        # 生成一个 [0.0, 1.0) 的随机数
        if random.random() < p:
            # 命中概率：决定采纳新边，替换旧边
            
            # 1. 随机选一个索引 (0 到 k-1)
            # random.randint 包含右边界，所以使用 k-1
            idx = random.randint(0, self.k - 1)
            
            # 2. 记录即将被删除的旧边 (这是为了后续更新邻接表 D 用)
            removed_edge = self.reservoir[idx]
            
            # 3. 执行替换
            self.reservoir[idx] = edge
            
            return True, removed_edge
            
        else:
            # 未命中概率：直接丢弃新边，水塘保持不变
            return False, None

# # ReservoirSampler 代码自测部分
# if __name__ == "__main__":
#     # 设定容量为 3
#     sampler = ReservoirSampler(k=3)
    
#     # 模拟流数据：输入 10 条边
#     stream_edges = [f"edge_{i}" for i in range(1, 11)]
    
#     print(f"Initializing Reservoir with capacity k={sampler.k}")
#     print("-" * 75)
    
#     for edge in stream_edges:
#         # 调用处理函数，并获取反馈
#         added, removed = sampler.process_edge(edge)
        
#         status = ""
#         if added and removed is None:
#             status = "Directly Added (Not Full)"
#         elif added and removed is not None:
#             status = f"Replaced {removed}"
#         else:
#             status = "Discarded"
            
#         # 英文输出
#         print(f"t={sampler.t:2} | New: {edge:<12} | Action: {status:<25} | M: {sampler.reservoir}")

# 模块 2: 图结构 D
class SimpleGraph:
    """
    图结构 D：
    维护样本集 M 中所有节点的邻接关系。
    用于快速查找共同邻居以发现三角形。
    """
    def __init__(self):
        # 使用字典将节点映射到其邻居集合
        # 结构: { node_id: {neighbor_1, neighbor_2, ...} }
        self.adj = defaultdict(set)

    def add_edge(self, u, v):
        """在图 D 中添加一条无向边 (u, v)"""
        # 因为是无向图，u 的邻居有 v，v 的邻居也有 u
        self.adj[u].add(v)
        self.adj[v].add(u)

    def remove_edge(self, u, v):
        """从图 D 中移除一条边 (u, v)"""
        # 使用 discard 而不是 remove，防止边不存在时报错
        if u in self.adj:
            self.adj[u].discard(v)
            # 如果该节点没有邻居了，可以选择删除键以节省内存（可选）
            if not self.adj[u]:
                del self.adj[u]
                
        if v in self.adj:
            self.adj[v].discard(u)
            if not self.adj[v]:
                del self.adj[v]

    def get_common_neighbors(self, u, v):
        """
        核心功能：查找 u 和 v 的共同邻居。
        返回共同邻居的集合。
        """
        # 如果任一节点不在图中，直接返回空集合
        if u not in self.adj or v not in self.adj:
            return set()
        
        # 利用 Python 集合的求交集操作 (&)
        return self.adj[u] & self.adj[v]

    def get_neighbors(self, u):
        """获取节点 u 的所有邻居"""
        return self.adj[u]

# # SimpleGraph 代码自测部分
# if __name__ == "__main__":
#     graph = SimpleGraph()
    
#     # 模拟 TRIEST 过程
#     # 1. 现有边 (1, 2) 和 (2, 3)
#     graph.add_edge(1, 2)
#     graph.add_edge(2, 3)
#     print("Added edges (1,2) and (2,3).")
    
#     # 2. 现在来了一条新边 (1, 3)，我们要检查它能形成多少三角形
#     # 也就是检查 1 和 3 有多少共同邻居
#     common = graph.get_common_neighbors(1, 3)
#     print(f"Common neighbors between 1 and 3: {common}")
    
#     if common:
#         print(f"Triangle found! Nodes: 1-3-{list(common)[0]}")
        
#     # 3. 模拟水塘替换：移除边 (1, 2)
#     graph.remove_edge(1, 2)
#     print("Removed edge (1,2).")
    
#     # 4. 再次检查
#     common_after = graph.get_common_neighbors(1, 3)
#     print(f"Common neighbors between 1 and 3 after removal: {common_after}")


# TRIEST 主算法
class TriestBase:
    def __init__(self, k):
        self.k = k
        # 初始化两个子模块
        self.sampler = ReservoirSampler(k)
        self.graph = SimpleGraph()
        # 全局三角形估计数
        self.global_triangles_est = 0.0

    def run(self, edge_stream):
        """
        运行算法的主循环
        edge_stream: 一个包含边 (u, v) 的列表或迭代器
        """
        print(f"--- TRIEST-BASE Started (Memory k={self.k}) ---")

        for edge in edge_stream:
            u, v = edge
            
            # 当前时间 t (注意：sampler.t 会在 process_edge 后增加，这里我们预判一下)
            # 或者直接使用 t = self.sampler.t + 1
            t = self.sampler.t + 1

            # --- 步骤 1: 计数与估算 (Count & Estimate) ---
            # 在修改图结构之前，先看这条新边能和现有的样本形成多少三角形
            common_neighbors = self.graph.get_common_neighbors(u, v)
            new_triangles = len(common_neighbors)

            if new_triangles > 0:
                # 计算放大因子 (Scaling Factor)
                # 论文公式: max(1, (t-1)(t-2) / (k(k-1)))
                if t <= self.k:
                    factor = 1.0
                else:
                    numerator = (t - 1) * (t - 2)
                    denominator = self.k * (self.k - 1)
                    factor = numerator / denominator
                
                # 更新全局估计值
                self.global_triangles_est += new_triangles * factor

            # --- 步骤 2: 抽样与更新 (Sample & Update) ---
            # 交给水塘抽样器决定去留
            added, removed_edge = self.sampler.process_edge(edge)

            if added:
                # 如果有旧边被移除，必须从图结构 D 中删掉
                if removed_edge is not None:
                    ru, rv = removed_edge
                    self.graph.remove_edge(ru, rv)
                
                # 将新边加入图结构 D
                self.graph.add_edge(u, v)

            # (可选) 每隔一定步数打印日志
            if t % 1000 == 0:
                print(f"Processed t={t} edges. Estimated Triangles: {int(self.global_triangles_est)}")

        print(f"--- Finished. Final Estimated Triangles: {int(self.global_triangles_est)} ---")
        return self.global_triangles_est


# # TRIEST 测试部分
# if __name__ == "__main__":
#     # --- 基础数据：一个包含 4 个三角形的小图 ---
#     # 三角形: (1,2,3), (1,2,4), (2,3,4), (1,3,4)
#     unique_edges = [
#         (1, 2), (2, 3), (3, 1), 
#         (1, 4), (2, 4), (3, 4),
#         (1, 5), (5, 6) 
#     ]
    
#     print("=== Test 1: Exact Counting (内存充足) ===")
#     # 设定 k > 边数，此时应该是精确计数
#     # 总边数 8，设 k=10
#     triest_exact = TriestBase(k=10)
#     result_exact = triest_exact.run(unique_edges)
#     print(f"真实三角形数: 4")
#     print(f"TRIEST 计算结果: {int(result_exact)}")
#     print(f"结论: {'PASS' if int(result_exact) == 4 else 'FAIL'}")
    
#     print("\n" + "="*40 + "\n")

#     print("=== Test 2: Estimation (内存受限) ===")
#     # 为了模拟更有意义的估算，我们生成一个稍微大一点的随机图
#     # 这样统计规律更明显，而不是重复同样的数据
    
#     # 生成一个 20 个节点，约 60 条边的随机图
#     nodes = range(20)
#     big_stream = []
#     # 随机生成一些三角形结构
#     for i in range(10):
#         u, v, w = random.sample(nodes, 3)
#         big_stream.extend([(u,v), (v,w), (w,u)])
#     # 再加一些随机边
#     for i in range(30):
#         u, v = random.sample(nodes, 2)
#         big_stream.append((u,v))
        
#     random.shuffle(big_stream)
#     total_edges = len(big_stream)
    
#     # 这种情况下，我们不知道确切的三角形数量，
#     # 但我们可以先用一个大 k 跑一遍算出真值
#     print("正在计算真实值 (使用大内存)...")
#     oracle = TriestBase(k=total_edges + 1)
#     true_count = oracle.run(big_stream)
    
#     # 现在用小内存跑 (例如只存 30% 的边)
#     small_k = int(total_edges * 0.3) 
#     print(f"\n正在进行估算 (k={small_k}, stream_size={total_edges})...")
    
#     triest_est = TriestBase(k=small_k)
#     est_count = triest_est.run(big_stream)
    
#     print(f"\n真实三角形数: {int(true_count)}")
#     print(f"TRIEST 估算值: {int(est_count)}")
    
#     # 只要数量级对上了，就算算法没问题
#     error_rate = abs(est_count - true_count) / true_count if true_count > 0 else 0
#     print(f"误差率: {error_rate:.2%}")
#     if error_rate < 0.5: # 允许 50% 的波动，因为 k 比较小
#         print("结论: PASS (估算在合理范围内)")
#     else:
#         print("结论: High Variance (由于数据量小，波动大属正常现象，多跑几次试试)")

# 加载文件
def load_dataset(filename):
    """
    加载 SNAP 数据集 (每行两个整数 u v)
    """
    print(f"正在读取数据集: {filename} ...")
    edges = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                # 跳过注释行 (如果有)
                if line.startswith('#'):
                    continue
                
                parts = line.strip().split()
                if len(parts) >= 2:
                    u = int(parts[0])
                    v = int(parts[1])
                    # 排除自环 (u, u)，虽然 TRIEST 也能处理，但通常不算三角形
                    if u != v:
                        edges.append((u, v))
        print(f"读取完成。共加载 {len(edges)} 条边。")
        return edges
    except FileNotFoundError:
        print(f"错误：找不到文件 '{filename}'。请确保文件在当前目录下。")
        return []


if __name__ == "__main__":
    # 1. 数据集文件名 (请确保文件在同目录下)
    DATASET_FILE = 'facebook_combined.txt'
    
    # 2. 加载数据
    stream_edges = load_dataset(DATASET_FILE)
    
    if stream_edges:
        # 3. 模拟流：打乱数据顺序
        print("Shuffling data to simulate stream behavior...")
        random.shuffle(stream_edges)
        
        total_edges = len(stream_edges)
        
        # --- 实验 1: 获取“标准答案” (Ground Truth) ---
        print("\n" + "="*60)
        print("--- Experiment 1: Ground Truth Calculation (Baseline) ---")
        print("="*60)
        print("Calculating exact triangle count (this may take a few seconds)...")
        
        start_time = time.time()
        # k > total_edges，相当于保留所有边，算出来的是精确值
        triest_oracle = TriestBase(k=total_edges + 100) 
        true_triangle_count = triest_oracle.run(stream_edges)
        time_oracle = time.time() - start_time
        
        print(f"True Triangle Count: {int(true_triangle_count)}")
        print(f"Time Taken: {time_oracle:.4f} seconds")
        
        # --- 实验 2: TRIEST 估算 (内存受限) ---
        print("\n" + "="*60)
        print("--- Experiment 2: TRIEST Estimation (Memory Restricted) ---")
        print("="*60)
        
        # 👇 在这里调整你的参数 (k) 👇
        # 设置采样比例，例如 0.10 代表 10% 的内存
        sample_ratio = 0.10 
        k_memory = int(total_edges * sample_ratio)
        
        print(f"Current Config: k = {k_memory} (Ratio: {sample_ratio:.1%})")
        print("Running estimation...")
        
        start_time = time.time()
        # 重新初始化一个内存较小的实例
        triest_est = TriestBase(k=k_memory)
        # 注意：这里我们传入同一个打乱后的流，保证实验公平性
        # (但在真实流中数据只能读一次，这里为了对比效果我们复用了列表)
        estimated_count = triest_est.run(stream_edges)
        time_est = time.time() - start_time
        
        # --- 结果对比分析 ---
        print("\n" + "="*60)
        print("📊 Final Result Analysis")
        print("="*60)
        print(f"Dataset:        {DATASET_FILE}")
        print(f"Total Edges:    {total_edges}")
        print(f"Ground Truth:   {int(true_triangle_count)}")
        print(f"Estimate:       {int(estimated_count)}")
        
        # 计算误差
        if true_triangle_count > 0:
            error = abs(estimated_count - true_triangle_count)
            error_rate = error / true_triangle_count
            print(f"Absolute Error: {int(error)}")
            print(f"Relative Error Rate: {error_rate:.2%}")
        
        print(f"Speedup: {time_oracle / time_est:.2f}x")
        
        # 简单判定
        if error_rate < 0.10:
            print("Conclusion: ✅ Excellent (Error < 10%)")
        elif error_rate < 0.20:
            print("Conclusion: 🆗 Good (Error < 20%)")
        else:
            print("Conclusion: ⚠️ High Variance (Try increasing k)")


