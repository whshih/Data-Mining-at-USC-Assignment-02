from pyspark import SparkContext
import json
import csv

def to_json(line):
 lines = line.splitlines()
 data = json.loads(lines[0])
 return data

def main():
    sc = SparkContext('local[*]','preprocess')
    review_f = sys.argv[1] #"review-002.json"
    business_f = sys.argv[2] #"business.json"
    textRDD = sc.textFile(review_f).map(to_json)
    textRDD_2 = sc.textFile(business_f).map(to_json)
    business_state_pairs = dict(textRDD_2.map(lambda key: (key["business_id"],key["state"])).collect())
    user_business_id = textRDD.filter(lambda key : business_state_pairs[key["business_id"]] == "NV").map(
        lambda key: (key["user_id"],key["business_id"])).collect()
    output_f = sys.argv[3] #user_business_id.csv
    f = open(output_f, 'w')

    with f:
        writer = csv.writer(f)
        title = ['user_id', 'business_id']
        writer.writerow(title)
        for k in user_business_id:
            writer.writerow(k)

if __name__ == "__main__":
    main()