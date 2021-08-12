import requests
from datetime import datetime
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType


# crawl
files = []
for alias in ['a', 'b', 'e', 'f', 'h']:
    fname = f'{alias}_lvr_land_a.csv'
    res = requests.get(f'https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName={fname}')
    open(fname, 'wb').write(res.content)
    files.append(fname)


# spark
spark = SparkSession \
    .builder \
    .appName('realstate') \
    .getOrCreate()

df = spark.read.csv(files, header=True)


# data cleaning functions
char_num = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, 
            '六': 6, '七': 7, '八': 8, '九': 9, '十':10 }
def word_to_number(s):
    s = s[:-1]
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

def minguo_to_ad(s):
    if s.isdigit():
        year = int(s[:3])
        s = str(year + 1911) + s[3:]
        ad_date = datetime.strptime(s, '%Y%m%d').date()
        return ad_date

word_to_number_udf = F.udf(word_to_number, IntegerType())
minguo_to_ad_udf = F.udf(minguo_to_ad, DateType())


# data cleaning
df = df.filter(
        (df['總樓層數'].endswith('層'))
        ). \
        withColumn('總樓層數', word_to_number_udf(df['總樓層數'])). \
        withColumn('交易年月日', minguo_to_ad_udf(df['交易年月日'])). \
        withColumn('縣市', F.substring('土地位置建物門牌', 1, 3))

final_df = df.filter(
                (df['主要用途'] == '住家用') &
                (df['建物型態'].startswith('住宅大樓')) &
                (df['總樓層數'] >= 13)
                ). \
        selectExpr('`縣市` as city', '`交易年月日` as date', '`鄉鎮市區` as district', '`建物型態` as building_state')

final_df.show()
print(final_df.count())


# output
df_json = final_df. \
    withColumn(
        'events', F.struct('district', 'building_state')
        ). \
    orderBy('date')

df_json = df_json.groupBy(
    'city'
    ).agg(
        F.collect_list(
            F.struct('date', 'events')
        ).alias('time_slots')
    )

a, b = df_json.randomSplit([1.0, 1.0])
for i, e in enumerate([a, b]):
    e.coalesce(1).write.json('result')
    os.system(f'cat result/part-*.json > result-part{i+1}.json')
    os.system('rm -rf result')