import os
import requests
from datetime import datetime
from shutil import rmtree

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType


# crawl
files = []
folder = 'raw_data'
for alias in ['a', 'b', 'e', 'f', 'h']:
    fname = f'{alias}_lvr_land_a.csv'
    full_fname = os.path.join(folder, fname)
    if os.path.isfile(full_fname):
        files.append(full_fname)
        continue
    res = requests.get(f'https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName={fname}')
    open(full_fname, 'wb').write(res.content)
    files.append(full_fname)


# spark
spark = SparkSession \
    .builder \
    .appName('realstate') \
    .getOrCreate()

df = spark.read.csv(files, header=True)


# data cleaning functions

# create character to number comparison sheet
char_num = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, 
            '六': 6, '七': 7, '八': 8, '九': 9, '十':10 }
def word_to_number(s):
    '''
    Change 'XXX層' to integer
    '''
    s = s[:-1] # remove "層" for each entry
    if '十' in s:
        if s[0] == '十':
            value = 0
            for char in s:
                value += char_num[char]
            return value
        else:
            value = char_num[s[0]] * 10 + char_num[s[-1]]
            return value
    else:
        return char_num[s]
word_to_number_udf = F.udf(word_to_number, IntegerType())

def minguo_to_ad(s):
    '''
    Change Minguo year to AD year
    '''
    if s.isdigit():
        year = int(s[:3])
        s = str(year + 1911) + s[3:]
        ad_date = datetime.strptime(s, '%Y%m%d').date()
        return ad_date
minguo_to_ad_udf = F.udf(minguo_to_ad, DateType())


# data cleaning
df = df.filter(
        (df['總樓層數'].endswith('層')) # Make sure there aren't dirty data
        ). \
        withColumn('總樓層數', word_to_number_udf(df['總樓層數'])). \
        withColumn('交易年月日', minguo_to_ad_udf(df['交易年月日'])). \
        withColumn('縣市', F.substring('土地位置建物門牌', 1, 3))
        # Use column '土地位置建物門牌' to extract city information

final_df = df.filter(
                (df['主要用途'] == '住家用') &
                (df['建物型態'].startswith('住宅大樓')) &
                (df['總樓層數'] >= 13)
                ). \
        selectExpr('`縣市` as city', '`交易年月日` as date', '`鄉鎮市區` as district', '`建物型態` as building_state')
        # Select columns we need

# output

# Combine 'district' and 'building_state' into column 'events'
# and make data order by 'date' ascendingly
df_json = final_df. \
    withColumn(
        'events', F.struct('district', 'building_state')
        ). \
    orderBy('date')

# Group by 'city' and pack 'date' and 'events' into a list named 'time_slots'
df_json = df_json.groupBy(
    'city'
    ).agg(
        F.collect_list(
            F.struct('date', 'events')
        ).alias('time_slots')
    ).repartition(2, 'city')

# Split data ramdomly, and save files locally with certain filename
output_dir = 'result'
if os.path.isdir(output_dir):
    rmtree(output_dir)
df_json.write.json(output_dir)
# a, b = df_json.randomSplit([1.0, 1.0])

for i in range(df_json.rdd.getNumPartitions()):
    os.system(f'cat {output_dir}/part-0000{i}*.json > result-part{i+1}.json')

os.system(f'rm -rf {output_dir}')