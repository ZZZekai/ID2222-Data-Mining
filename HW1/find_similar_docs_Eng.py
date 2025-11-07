import re
import zlib
import random
import itertools
import math
import time  # 确保 time 库被导入

# # --- 第 0 步：定义我们的文档（语料库） ---
# # 我们使用一个字典 (dictionary) 来存储，
# # key 是文档 ID (如 'doc1'), value 是文档的原始文本。
# corpus = {
#     'doc1': "The quick brown fox jumps over the lazy dog.",
#     'doc2': "A quick brown fox jumps over the lazy dog!", # 和 doc1 非常像
#     'doc3': "The quick brown fox jumps over the lazy cat.", # 有点像
#     'doc4': "Python is a great programming language.",
#     'doc5': "Data science is a field that uses Python, a great programming language.", # 和 doc4 相关
#     'doc6': "How much wood would a woodchuck chuck if a woodchuck could chuck wood?",
#     'doc7': "This is a completely different document about web development and javascript."
# }

# --- 第 1 步：Shingling 类 ---

class Shingling:
    """
    这个类负责：
    1. 接收一篇原始文档。
    2. 将其清理干净。
    3. 创建 k-shingles (k-分片)。
    4. 将每个 shingle 哈希成一个 32 位整数。
    5. 返回所有不重复的哈希整数集合 (set)。
    """
    
    def __init__(self, k=10):
        """
        初始化 Shingler。
        
        参数:
            k (int): 每个 shingle 的长度 (例如，10 个字符)。
        """
        # 我们在这里做一个简单的检查，确保 k 是一个正数
        if k <= 0:
            raise ValueError("Shingle length k must be positive.")
        self.k = k

    def create_shingles(self, document_text):
        """
        从单个文档字符串创建唯一的、哈希过的 k-shingles 集合。
        """
        
        # 1. 清理文本：
        #    a. 全部转为小写
        #    b. re.sub(r'[^\w\s]', '', ...) 会去掉所有非字母/数字/空格的字符 (即标点符号)
        #    c. re.sub(r'\s+', ' ', ...) 会把多个空格（如 "a   b"）压缩成一个（"a b"）
        cleaned_text = document_text.lower()
        cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text)
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip() # .strip() 去掉首尾空格
        
        # 2. 创建 shingles 并哈希
        
        # 这是我们将要返回的集合
        shingles_set = set()
        
        # 如果文档比 k 还短，它就没有任何 shingle
        if len(cleaned_text) < self.k:
            return shingles_set
            
        # 使用“滑动窗口”遍历清理过的文本
        # range 会从 0 走到 (总长度 - k + 1)
        for i in range(len(cleaned_text) - self.k + 1):
            
            # a. 切片：从 i 到 i+k 提取子字符串
            shingle = cleaned_text[i:i+self.k]
            
            # b. 哈希：
            #    我们必须先用 .encode('utf-8') 把字符串变成字节(bytes)
            #    因为哈希函数是针对字节而不是字符串操作的
            hashed_shingle = zlib.crc32(shingle.encode('utf-8'))
            
            # c. 添加入集合 (set 会自动处理重复)
            shingles_set.add(hashed_shingle)
            
        # 3. 返回最终的“指纹”
        return shingles_set
    
    # --- 测试我们的 Shingling 类 ---


# --- 第 2 步：比较集合 (Jaccard 相似度) ---

def compute_jaccard(set1, set2):
    """
    计算两个集合的 Jaccard 相似度。
    
    参数:
        set1 (set): 第一个 shingle 集合。
        set2 (set): 第二个 shingle 集合。
        
    返回:
        float: Jaccard 相似度 (0.0 到 1.0 之间)。
    """
    
    # 1. 计算交集 (两个集合中共同拥有的元素)
    #    在 Python 中，set1 & set2 就能得到交集
    intersection = set1.intersection(set2)
    
    # 2. 计算并集 (两个集合中所有的元素，去掉重复)
    #    在 Python 中，set1 | set2 就能得到并集
    union = set1.union(set2)
    
    # 3. 计算 Jaccard 相似度
    
    # 我们要处理一个特殊情况：如果两个集合都是空的，
    # 那么并集的大小为 0，会导致除零错误。
    # 在这种情况下，我们定义它们是 100% 相似的。
    if not union:
        return 1.0
        
    return len(intersection) / len(union)

# --- 第 3 步：MinHashing 类 ---

class MinHashing:
    """
    这个类负责从一个 shingle 集合创建一个 MinHash 签名。
    """
    
    def __init__(self, num_hashes=100):
        """
        初始化 MinHasher。
        
        参数:
            num_hashes (int): 我们希望签名有多长 (即我们要创建多少个哈希函数)。
        """
        if num_hashes <= 0:
            raise ValueError("Number of hashes must be positive.")
        self.num_hashes = num_hashes
        
        # 我们需要一个大质数 p，用于哈希函数 h(x) = (a*x + b) % p
        # 我们的 shingle 哈希值是 32 位的，所以我们选一个比 2^32 大的质数。
        # 4294967311 是一个常用的质数 (2^32 + 15)
        self.prime = 4294967311
        
        # 我们需要为 self.num_hashes 个哈希函数生成 (a, b) 参数
        # 我们把这些参数存储在一个列表 self.hash_params 中
        self.hash_params = []
        
        # 为了让结果可复现（每次运行都一样），我们设置一个随机种子
        # 这在调试时非常有用！
        random.seed(42) 
        
        for _ in range(self.num_hashes):
            # 'a' 必须是 1 到 p-1 之间的随机数
            a = random.randint(1, self.prime - 1)
            # 'b' 必须是 0 到 p-1 之间的随机数
            b = random.randint(0, self.prime - 1)
            self.hash_params.append((a, b)) # (a, b) 就是一个哈希函数

    def create_signature(self, shingle_set):
        """
        为给定的 shingle 集合创建 MinHash 签名。
        
        参数:
            shingle_set (set): 来自 Shingling 类的哈希整数集合。
            
        返回:
            list: 一个长度为 self.num_hashes 的 MinHash 签名。
        """
        
        # 1. 初始化签名列表，所有值都设为“无穷大”
        #    因为我们要找最小值，所以初始值必须是最大的。
        signature = [float('inf')] * self.num_hashes
        
        # 如果 shingle 集合是空的，就返回这个 "无穷大" 签名
        if not shingle_set:
            return signature

        # 2. 遍历集合中的 *每一个* shingle 哈希值
        for shingle_hash in shingle_set:
            
            # 3. 对于 *每一个* shingle，我们用所有 100 个哈希函数去计算它
            for i in range(self.num_hashes):
                
                # 获取第 i 个哈希函数的 (a, b) 参数
                a, b = self.hash_params[i]
                
                # 应用哈希函数: h(x) = (a*x + b) % p
                hash_val = (a * shingle_hash + b) % self.prime
                
                # 4. 检查这个结果是否是 *迄今为止* 最小的
                #    如果是，就更新签名中第 i 个位置的值
                if hash_val < signature[i]:
                    signature[i] = hash_val
                    
        # 5. 遍历完所有 shingle 后，这个列表就是最终的 MinHash 签名
        return signature
    
# --- 第 4 步：比较签名 (估算 Jaccard 相似度) ---

def estimate_similarity(sig1, sig2):
    """
    估算两个 MinHash 签名的 Jaccard 相似度。
    
    参数:
        sig1 (list): 第一个 MinHash 签名。
        sig2 (list): 第二个 MinHash 签名。
        
    返回:
        float: 估算的相似度 (0.0 到 1.0 之间)。
    """
    
    # 1. 确保两个签名的长度一致
    if len(sig1) != len(sig2):
        raise ValueError("Signatures must be of the same length.")
    
    # 2. 检查签名是否为空
    if not sig1:
        # 两个空签名是 100% 相似的
        return 1.0
        
    # 3. 遍历签名，计算有多少个位置的值是相等的
    matches = 0
    for i in range(len(sig1)):
        if sig1[i] == sig2[i]:
            matches += 1
            
    # 4. 相似度 = 相等的数量 / 签名总长度
    return matches / len(sig1)

# --- 第 5 步：LSH (局部敏感哈希) 类 ---

class LSH:
    """
    这个类实现了 LSH "banding" 技术，
    用于从大量的 MinHash 签名中快速找出候选的相似对。
    """
    
    def __init__(self, num_hashes, threshold):
        """
        初始化 LSH。
        
        参数:
            num_hashes (int): MinHash 签名的长度 (n)。
            threshold (float): 我们期望的 Jaccard 相似度阈值 (s)。
        """
        self.num_hashes = num_hashes
        self.threshold = threshold
        
        # --- 自动计算最佳的 b 和 r ---
        # (这部分是可选的，但非常酷)
        # 我们要找到 b 和 r (b * r = num_hashes)
        # 使得阈值 t ≈ (1/b)^(1/r) 尽可能接近我们想要的 threshold
        
        best_b = 0
        best_r = 0
        min_diff = float('inf')

        # 遍历所有可能的 r (r 必须能被 num_hashes 整除)
        for r_candidate in range(1, self.num_hashes + 1):
            if self.num_hashes % r_candidate == 0:
                b_candidate = self.num_hashes // r_candidate
                
                # 计算这个 (b, r) 组合所近似的阈值
                approx_t = (1 / b_candidate) ** (1 / r_candidate)
                
                # 看看这个阈值和我们目标的差距
                diff = abs(approx_t - self.threshold)
                
                if diff < min_diff:
                    min_diff = diff
                    best_b = b_candidate
                    best_r = r_candidate
        
        if best_b == 0: # 备用方案
            best_b = self.num_hashes
            best_r = 1
            
        self.b = best_b
        self.r = best_r
        
    def find_candidates(self, signatures_map):
        """
        从 MinHash 签名中找出所有“候选对”。
        
        参数:
            signatures_map (dict): 一个字典, {doc_id: signature_list}
            
        返回:
            set: 一个包含所有候选对 (doc_id1, doc_id2) 元组 (tuple) 的集合。
        """
        
        print(f"\n[LSH] Running LSH with b={self.b} bands and r={self.r} rows...")
        
        # 这是我们最终要返回的集合 (set 会自动处理重复的候选对)
        candidate_pairs = set()
        
        # 1. 遍历每一个“条带”(band)
        for i in range(self.b):
            
            # 2. 为 *这个* 条带创建一个新的“桶”集合 (字典)
            #    注意：每个条带的桶是独立的！
            buckets = {}
            
            # 3. 遍历 *所有* 文档的签名
            for doc_id, signature in signatures_map.items():
                
                # a. 从完整签名中“切”出这个条带的数据
                start = i * self.r
                end = start + self.r
                band_data = signature[start:end]
                
                # b. 我们需要把这个条带 (一个列表) 哈希成一个值，才能放进桶里
                #    最简单的方法是把它转成一个 tuple (元组)，因为 tuple 是可哈希的
                #    为了更稳定，我们像以前一样用 zlib.crc32
                #    我们把 tuple 转成 str 来哈希
                band_hash = zlib.crc32(str(tuple(band_data)).encode('utf-8'))
                
                # c. 把 doc_id 扔进对应的桶里
                if band_hash not in buckets:
                    buckets[band_hash] = []
                buckets[band_hash].append(doc_id)
            
            # 4. 检查这个条带的所有桶
            for doc_id_list in buckets.values():
                # 如果一个桶里的文档数 > 1, 说明它们“碰撞”了！
                if len(doc_id_list) > 1:
                    
                    # 桶里所有文档的两两组合都是候选对
                    # itertools.combinations 会生成所有唯一的组合
                    # 我们先排序，确保 (doc1, doc2) 而不是 (doc2, doc1)
                    sorted_ids = sorted(doc_id_list)
                    for pair in itertools.combinations(sorted_ids, 2):
                        candidate_pairs.add(pair)
                        
        # 5. 遍历完所有条带后，返回所有找到的候选对
        return candidate_pairs

if __name__ == "__main__":
    
    # --- Global Parameters ---
    K_SHINGLE_LENGTH = 10
    NUM_HASHES = 100
    SIMILARITY_THRESHOLD = 0.8 

    # --- (NEW) Set the number of documents to test! ---
    # 500 will be fast (approx. 120k Jaccard comparisons)
    # 1000 will be slower (approx. 500k Jaccard comparisons)
    # 5572 is the full dataset (approx. 15.5M Jaccard comparisons)
    DOCS_TO_TEST = 10000
    
    # --- (NEW) Load UCI SMS Spam Dataset (with limit) ---
    print(f"--- Loading UCI SMS Spam Collection Dataset (limit: first {DOCS_TO_TEST} docs)... ---")
    
    corpus = {}
    dataset_filename = 'SMSSpamCollection' # Make sure this file is in the same directory
    doc_counter = 1

    try:
        with open(dataset_filename, 'r', encoding='utf-8') as f:
            for line in f:
                # --- (NEW) Check if we reached our test limit ---
                if doc_counter > DOCS_TO_TEST:
                    break # Stop reading
                
                try:
                    label, text = line.strip().split('\t', 1)
                except ValueError:
                    continue 
                
                if text.strip():
                    doc_id = f"sms_{doc_counter}"
                    corpus[doc_id] = text
                    doc_counter += 1
                    
    except FileNotFoundError:
        print(f"Error: Dataset file '{dataset_filename}' not found.")
        print("Please download the 'SMS Spam Collection' dataset from the UCI website,")
        print("unzip it, and place the 'SMSSpamCollection' file in the same directory as this script.")
        exit() 
    except IsADirectoryError:
        print(f"Error: '{dataset_filename}' is a directory, not a file.")
        print("Please ensure you have copied the *file itself* into the script directory, not the folder containing it.")
        exit()

    print(f"Loading complete! {len(corpus)} documents will be analyzed.") 
    print(f"Parameters: k={K_SHINGLE_LENGTH}, n_hashes={NUM_HASHES}, threshold={SIMILARITY_THRESHOLD}\n")

    # --- Task 1: Shingling (with timer) ---
    print("--- Task 1: Creating Shingles... ---")
    start_time_task1 = time.time()
    
    shingler = Shingling(k=K_SHINGLE_LENGTH)
    document_shingle_sets = {}
    for doc_id, text in corpus.items():
        document_shingle_sets[doc_id] = shingler.create_shingles(text)
        
    end_time_task1 = time.time()
    task1_time = end_time_task1 - start_time_task1
    print(f"Created shingle sets for {len(document_shingle_sets)} documents.")
    print(f"  (Time taken: {task1_time:.2f} seconds)\n")


    # --- Task 2: Ground Truth (Jaccard) (with timer) ---
    num_comparisons = len(corpus) * (len(corpus) - 1) // 2
    print(f"--- Task 2: Calculating 'Ground Truth' Jaccard Similarity (threshold > {SIMILARITY_THRESHOLD}) ---")
    print(f"  (Performing {num_comparisons:,} comparisons...)")
    print(f"  (This may take several minutes. Please be patient...)")
    
    start_time_task2 = time.time()
    
    all_doc_ids = sorted(list(document_shingle_sets.keys()))
    all_pairs = list(itertools.combinations(all_doc_ids, 2))
    
    true_similar_pairs = set()

    for id1, id2 in all_pairs:
        set1 = document_shingle_sets[id1]
        set2 = document_shingle_sets[id2]
        j_sim = compute_jaccard(set1, set2)
        
        if j_sim >= SIMILARITY_THRESHOLD:
            true_similar_pairs.add((id1, id2))
            
    end_time_task2 = time.time()
    task2_time = end_time_task2 - start_time_task2
    print(f"\nFound {len(true_similar_pairs)} 'true' similar pairs.")
    print(f"  (Time taken: {task2_time:.2f} seconds)\n")


    # --- Task 3: MinHashing (with timer) ---
    print("--- Task 3: Creating MinHash Signatures... ---")
    start_time_task3 = time.time()
    
    min_hasher = MinHashing(num_hashes=NUM_HASHES)
    document_signatures = {}
    for doc_id, shingle_set in document_shingle_sets.items():
        document_signatures[doc_id] = min_hasher.create_signature(shingle_set)
        
    end_time_task3 = time.time()
    task3_time = end_time_task3 - start_time_task3
    print(f"Successfully created {len(document_signatures)} signatures of length {NUM_HASHES}.")
    print(f"  (Time taken: {task3_time:.2f} seconds)\n")


    # --- Task 5: LSH (with timer) ---
    print("--- Task 5: Running LSH to find candidate pairs... ---")
    start_time_task5 = time.time()
    
    lsh = LSH(num_hashes=NUM_HASHES, threshold=SIMILARITY_THRESHOLD)
    candidate_pairs = lsh.find_candidates(document_signatures)
    
    end_time_task5 = time.time()
    task5_time = end_time_task5 - start_time_task5
    print(f"\nLSH found {len(candidate_pairs)} candidate pairs.")
    print(f"  (LSH run time: {task5_time:.2f} seconds)\n")
    
    
    # --- Final Evaluation ---
    print("--- Final Evaluation (LSH vs. Ground Truth) ---")
    print(f"Verifying {len(candidate_pairs)} LSH candidate pairs...")
    
    start_time_verify = time.time()
    
    lsh_tp = set() # True Positives
    lsh_fp = set() # False Positives
    
    for id1, id2 in candidate_pairs:
        # We only compute Jaccard on the *candidate pairs*
        set1 = document_shingle_sets[id1]
        set2 = document_shingle_sets[id2]
        j_sim = compute_jaccard(set1, set2)
        
        if j_sim >= SIMILARITY_THRESHOLD:
            lsh_tp.add((id1, id2))
        else:
            lsh_fp.add((id1, id2))
            
    end_time_verify = time.time()
    task_verify_time = end_time_verify - start_time_verify
    print(f"LSH candidate verification complete. (Time taken: {task_verify_time:.2f} seconds)\n")

    # False Negatives: Pairs that were truly similar but LSH missed
    lsh_fn = true_similar_pairs.difference(lsh_tp)
    
    print("--- LSH Performance Report ---")
    print(f"  True Positives (TP)  (LSH found, was similar): {len(lsh_tp)}")
    print(f"  False Positives (FP) (LSH found, NOT similar): {len(lsh_fp)}")
    print(f"  False Negatives (FN) (LSH missed, was similar): {len(lsh_fn)}")
    
    # Recall = TP / (TP + FN)
    if (len(lsh_tp) + len(lsh_fn)) > 0:
        recall = len(lsh_tp) / (len(lsh_tp) + len(lsh_fn))
        print(f"\nRecall: {recall:.2%}")
    else:
        print("\nRecall: N/A (No true similar pairs)")

    # Precision = TP / (TP + FP)
    if len(candidate_pairs) > 0:
        precision = len(lsh_tp) / len(candidate_pairs)
        print(f"Precision: {precision:.2%}")
    else:
        print(f"Precision: N/A (No candidates found)")
        
    print("\n--- Scalability Report ---")
    
    # Brute-force strategy time = Task 2 (Jaccard comparison)
    total_time_jaccard_strategy = task2_time
    
    # LSH strategy time = Task 3 (MinHash) + Task 5 (LSH buckets) + LSH Verification
    total_time_lsh_strategy = task3_time + task5_time + task_verify_time
    
    print(f"  [Brute-Force] Total time (Task 2): {total_time_jaccard_strategy:.2f} seconds")
    print(f"  [LSH Strategy] Total time (Task 3+5+Verify): {total_time_lsh_strategy:.2f} seconds")
    
    if total_time_lsh_strategy > 0 and total_time_jaccard_strategy > 0:
        speedup = total_time_jaccard_strategy / total_time_lsh_strategy
        print(f"\n  LSH strategy was {speedup:.1f}x faster than Brute-Force.")


# if __name__ == "__main__":
    
#     # --- 全局参数 ---
#     K_SHINGLE_LENGTH = 10
#     NUM_HASHES = 100
#     SIMILARITY_THRESHOLD = 0.8 

#     # --- (新功能) 在这里设置您想测试的文档数量！ ---
#     # 500 篇会很快 (总共约 12 万次 Jaccard 比较)
#     # 1000 篇会稍慢 (总共约 50 万次 Jaccard 比较)
#     # 5572 篇是完整版 (总共约 1500 万次 Jaccard 比较)
#     DOCS_TO_TEST = 5572 
    
#     # --- (新功能) 加载 UCI SMS Spam 数据集 (带上限) ---
#     print(f"--- 正在加载 UCI SMS Spam Collection 数据集 (仅限前 {DOCS_TO_TEST} 篇)... ---")
    
#     corpus = {}
#     dataset_filename = 'SMSSpamCollection' # 确保这个文件和你的.py在同一目录
#     doc_counter = 1

#     try:
#         with open(dataset_filename, 'r', encoding='utf-8') as f:
#             for line in f:
#                 # --- (新功能) 检查是否达到了我们的测试上限 ---
#                 # 因为 doc_counter 是从 1 开始的，当它即将变成 1001 时，我们就停止
#                 if doc_counter > DOCS_TO_TEST:
#                     break # 跳出循环，停止读取更多行
                
#                 try:
#                     label, text = line.strip().split('\t', 1)
#                 except ValueError:
#                     continue 
                
#                 if text.strip():
#                     doc_id = f"sms_{doc_counter}"
#                     corpus[doc_id] = text
#                     # 只有成功添加一个文档后，才增加计数器
#                     doc_counter += 1
                    
#     except FileNotFoundError:
#         print(f"错误：找不到数据集文件 '{dataset_filename}'。")
#         print("请从 UCI 网站下载 'SMS Spam Collection' 数据集,")
#         print("解压后将 'SMSSpamCollection' 文件放在脚本同一目录下。")
#         exit() 
#     except IsADirectoryError:
#         print(f"错误：'{dataset_filename}' 是一个文件夹，不是一个文件。")
#         print("请确保您已将 *文件本身* 复制到脚本目录，而不是包含它的文件夹。")
#         exit()

#     # doc_counter 最终会是 DOCS_TO_TEST + 1，所以 len(corpus) 才是正确的文档数
#     print(f"加载完毕！共 {len(corpus)} 篇文档将用于分析。") 
#     print(f"参数: k={K_SHINGLE_LENGTH}, n_hashes={NUM_HASHES}, threshold={SIMILARITY_THRESHOLD}\n")

#     # --- 任务 1：创建 Shingles (带计时器) ---
#     print("--- 任务 1：正在创建 Shingles... ---")
#     start_time_task1 = time.time()
    
#     shingler = Shingling(k=K_SHINGLE_LENGTH)
#     document_shingle_sets = {}
#     for doc_id, text in corpus.items():
#         document_shingle_sets[doc_id] = shingler.create_shingles(text)
        
#     end_time_task1 = time.time()
#     task1_time = end_time_task1 - start_time_task1
#     print(f"为 {len(document_shingle_sets)} 篇文档创建了 Shingle 集合。")
#     print(f"  (耗时: {task1_time:.2f} 秒)\n")


#     # --- 任务 2：计算“地面实况”(Ground Truth) (带计时器) ---
#     num_comparisons = len(corpus) * (len(corpus) - 1) // 2
#     print(f"--- 任务 2：计算“真实” Jaccard 相似度 (阈值 > {SIMILARITY_THRESHOLD}) ---")
#     print(f"  (正在进行 {num_comparisons:,} 次比较...)")
    
#     start_time_task2 = time.time()
    
#     all_doc_ids = sorted(list(document_shingle_sets.keys()))
#     all_pairs = list(itertools.combinations(all_doc_ids, 2))
    
#     true_similar_pairs = set()

#     for id1, id2 in all_pairs:
#         set1 = document_shingle_sets[id1]
#         set2 = document_shingle_sets[id2]
#         j_sim = compute_jaccard(set1, set2)
        
#         if j_sim >= SIMILARITY_THRESHOLD:
#             true_similar_pairs.add((id1, id2))
            
#     end_time_task2 = time.time()
#     task2_time = end_time_task2 - start_time_task2
#     print(f"\n总共找到了 {len(true_similar_pairs)} 个“真实”相似对。")
#     print(f"  (耗时: {task2_time:.2f} 秒)\n")


#     # --- 任务 3：创建 MinHash 签名 (带计时器) ---
#     print("--- 任务 3：正在创建 MinHash 签名... ---")
#     start_time_task3 = time.time()
    
#     min_hasher = MinHashing(num_hashes=NUM_HASHES)
#     document_signatures = {}
#     for doc_id, shingle_set in document_shingle_sets.items():
#         document_signatures[doc_id] = min_hasher.create_signature(shingle_set)
        
#     end_time_task3 = time.time()
#     task3_time = end_time_task3 - start_time_task3
#     print(f"成功创建了 {len(document_signatures)} 个 {NUM_HASHES} 维的签名。")
#     print(f"  (耗时: {task3_time:.2f} 秒)\n")


#     # --- 任务 5：运行 LSH (带计时器) ---
#     print("--- 任务 5：运行 LSH 寻找候选对... ---")
#     start_time_task5 = time.time()
    
#     lsh = LSH(num_hashes=NUM_HASHES, threshold=SIMILARITY_THRESHOLD)
#     candidate_pairs = lsh.find_candidates(document_signatures)
    
#     end_time_task5 = time.time()
#     task5_time = end_time_task5 - start_time_task5
#     print(f"\nLSH 找到了 {len(candidate_pairs)} 个候选对。")
#     print(f"  (LSH 运行耗时: {task5_time:.2f} 秒)\n")
    
    
#     # --- 最终评估 ---
#     print("--- 最终评估 (LSH vs 真实 Jaccard) ---")
#     print(f"正在验证 {len(candidate_pairs)} 个 LSH 候选对...")
    
#     start_time_verify = time.time()
    
#     lsh_tp = set() 
#     lsh_fp = set() 
    
#     for id1, id2 in candidate_pairs:
#         set1 = document_shingle_sets[id1]
#         set2 = document_shingle_sets[id2]
#         j_sim = compute_jaccard(set1, set2)
        
#         if j_sim >= SIMILARITY_THRESHOLD:
#             lsh_tp.add((id1, id2))
#         else:
#             lsh_fp.add((id1, id2))
            
#     end_time_verify = time.time()
#     task_verify_time = end_time_verify - start_time_verify
#     print(f"LSH 候选对验证完毕。 (耗时: {task_verify_time:.2f} 秒)\n")

#     lsh_fn = true_similar_pairs.difference(lsh_tp)
    
#     print("--- LSH 成绩单 ---")
#     print(f"  True Positives (TP)  (LSH 找到了, 且正确): {len(lsh_tp)}")
#     print(f"  False Positives (FP) (LSH 找到了, 但错了): {len(lsh_fp)}")
#     print(f"  False Negatives (FN) (LSH 漏掉了, 但本应找到): {len(lsh_fn)}")
    
#     if (len(lsh_tp) + len(lsh_fn)) > 0:
#         recall = len(lsh_tp) / (len(lsh_tp) + len(lsh_fn))
#         print(f"\n召回率 (Recall - 查全率): {recall:.2%}")
#     else:
#         print("\n召回率 (Recall - 查全率): N/A (没有真实相似对)")

#     if len(candidate_pairs) > 0:
#         precision = len(lsh_tp) / len(candidate_pairs)
#         print(f"精确率 (Precision - 查准率): {precision:.2%}")
#     else:
#         print(f"精确率 (Precision - 查准率): N/A (没有找到候选对)")
        
#     print("\n--- 可伸缩性 (Scalability) 报告 ---")
    
#     total_time_jaccard_strategy = task2_time
#     total_time_lsh_strategy = task3_time + task5_time + task_verify_time
    
#     print(f"  [暴力法] 总耗时 (任务 2): {total_time_jaccard_strategy:.2f} 秒")
#     print(f"  [LSH 法] 总耗时 (任务 3+5+验证): {total_time_lsh_strategy:.2f} 秒")
    
#     if total_time_lsh_strategy > 0 and total_time_jaccard_strategy > 0:
#         speedup = total_time_jaccard_strategy / total_time_lsh_strategy
#         print(f"\n  LSH 策略相比暴力法，速度提升了 {speedup:.1f} 倍！")