from pyspark import SparkContext
import itertools
import time
import sys



# Provide (SON) each mapper with a single key­value pair
def single_kv(line, case):
    # Let elements of data be single item
    line = line.strip()       # strings
    entries = line.split(',') # list
    k = entries[0]
    v = entries[1]
    if case == 1:
        return k, v  # (user_id, business_id)
    else:
        return v, k  # (business_id, user_id)


def apriori_algorithm(data, support):
    single_sets = {}
    basket = []
    freq_itemsets = []

    for row in data:
        basket.append(row)
        for business_support in row[1]:  # k-v: (case 1) (user_id, business_id)
            if business_support in single_sets:
                single_sets[business_support] = single_sets[business_support] + 1
            else:
                single_sets[business_support] = 1  # {'business_id', support}

    L1_candidate = []
    for id, s in single_sets.items():
        if s >= support:
            L1_candidate.append(id)

    L1_size = len(L1_candidate)
    freq_itemsets.extend(L1_candidate)

    #   Construct C2: pop the items whose support lower than support threshold, and make them be pairs
    num = 2
    L2_candidate = {}
    while L1_size > num:
        for i in range(0, L1_size):
            for j in range(i + 1, L1_size):
                if num == 2:
                    pairs1 = set([L1_candidate[i]]).union([L1_candidate[j]])
                    C2 = frozenset(pairs1)
                else:
                    pairs2 = set(L1_candidate[i]).union(L1_candidate[j])
                    C2 = frozenset(pairs2)

                #  Construct L2(Pairs): {{'99','100','101'}:3, ...}
                if len(C2) == num:
                    if C2 in L2_candidate:
                        L2_candidate[C2] = L2_candidate[C2] + 1
                    else:
                        L2_candidate[C2] = 1
        if num == 2:
            L1_candidate = [pairs for pairs, s in L2_candidate.items() if s >= 1]
        else:
            L1_candidate = [pairs for pairs, s in L2_candidate.items() if s >= num]

        L3_list = []
        basket_items = []  # keep the frequent items only
        for k, v in basket:
            basket_items.append(v)

        for i in L1_candidate:
            appear_times = 0
            for j in basket_items:
                if i.issubset(j):
                    appear_times = appear_times + 1
                if appear_times >= support:
                    freq_itemsets.append(tuple(sorted(i)))
                    L3_list.append(tuple(sorted(i)))
                    break
        L1_candidate = L3_list
        L1_size = len(L3_list)
        num = num + 1
    return iter(freq_itemsets)


def Count_num_occurrence(data, candidate_freq_items):
    basket = []
    freq_items = []
    for row in data:
        basket.append(row)

    for item in candidate_freq_items:
        support_of_pairs = 0
        for i in basket:
            if type(item[0]) is tuple:
                if set(item[0]).issubset(i[1]):
                    support_of_pairs = support_of_pairs + 1
            else:
                if set([item[0]]).issubset(i[1]):
                    support_of_pairs = support_of_pairs + 1
        if type(item[0]) is tuple:
            temp = tuple(sorted(item[0]))
            freq_items.append((temp, support_of_pairs))
        else:
            freq_items.append((item[0], support_of_pairs))

    return iter(freq_items)


def format(candidates):
    output_dict = {1: [], }
    for i in range(len(candidates)):
        if isinstance(candidates[i][0], str):
            output_dict[1].append(candidates[i][0])
        else:
            if len(candidates[i][0]) in output_dict:
                output_dict[len(candidates[i][0])].append(candidates[i][0])
            else:
                output_dict[len(candidates[i][0])] = [candidates[i][0]]

    keys = output_dict.keys()
    maxSize = max(keys)

    output_string = ''
    for i in range(1, maxSize+1):
        if i == 1:
            temp_list = output_dict[i]
            temp_list.sort()
            for string in temp_list:
                temp = '(\''+string+'\')'
                output_string += temp+','
            output_string = output_string.rstrip(',')+'\n\n'
        else:
            temp_list = []
            for x in output_dict[i]:
                temp_list.append(sorted([x]))
            temp_list.sort()
            for string in temp_list:
                temp = str(string).lstrip('[').rstrip(']')
                output_string += temp+','
            output_string = output_string.rstrip(',')+'\n\n'
    return output_string


def To_txt(candidate_freq_itemsets, freq_Itemsets, output_file_name):
    text = "Candidates:\n" + \
        format(candidate_freq_itemsets) + \
        "Frequent Itemsets:\n" + format(freq_Itemsets)
    with open(output_file_name, "w") as fp_out:
        fp_out.write(text)


def main():
    start = time.time()

    sc = SparkContext('local[*]', 'task2')
    case_num = 1
    threshold = int(sys.argv[1]) #70
    support = int(sys.argv[2]) #50
    input_file_name = sys.argv[3] #"User_id_And_Business_id.csv"
    output_file_name = sys.argv[4] #"task2(1).txt"
    textRDD = sc.textFile(input_file_name)
    # Split Data
    textRDD = textRDD.mapPartitionsWithIndex(
        lambda index, data: itertools.islice(data, 1, None) if index == 0 else data)
    # Provide (SON) each mapper with a single key­value pair
    ##    textRDD = textRDD.map(lambda line: single_kv(line, case_num)).groupByKey().map(lambda x: (x[0], frozenset(x[1])))
    textRDD = textRDD.map(lambda line: single_kv(line, case_num)).groupByKey().map(
        lambda x: (x[0], frozenset(x[1]))).filter(lambda line: 1 if len(line[1]) > threshold else 0)
    # print("Count:", textRDD.count())
    # Number of sub-files
    num_subfile = textRDD.getNumPartitions()
    local_support = support / num_subfile
    ## First phase of SON: Candidate Search (class: list)
    candidate_freq_itemsets = textRDD.mapPartitions(
        lambda baskets: apriori_algorithm(baskets, local_support if local_support > 1 else 1)).map(
        lambda itemsets: (itemsets, 1)).reduceByKey(lambda a, b: a + b).collect()
    ## Second Phase of SON: Candidate Filtering (class: list)
    # main goal: Remove the candidates from the first phase that are false positives
    freq_Itemsets = textRDD.mapPartitions(
        lambda basket: Count_num_occurrence(basket, candidate_freq_itemsets)).reduceByKey(
        lambda a, b: a + b).filter(lambda each_support: each_support[1] >= support).collect()
    To_txt(candidate_freq_itemsets, freq_Itemsets, output_file_name)

    end = time.time()
    print("Duration:" + str(end - start))


if __name__ == "__main__":
    main()
