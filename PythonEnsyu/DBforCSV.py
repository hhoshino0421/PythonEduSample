# モジュール読み込み
import psycopg2
import os
import sys
import time
import datetime

# 定数定義
# データベース接続情報
DATABASE_HOST = "192.168.8.106"
DATABASE_PORT = 5432
DATABASE_NAME = "pytestdb"
DATABASE_USER = "pytest"
DATABASE_PASSWORD = "pytest0"

# CSVファイル名
JR_EAST_FILE_NAME = "2016_JREast.csv"
TOKYO_METRO_FILE_NAME = "2016_TokyoMetro.csv"

# 社員番号
SYAIN_ID = "00053378"

# 年度
NENDO_2016 = "2016"


# 使い方表示
def usage():
    print("使い方")
    print(" python DBforCSV.py")
    print("")


# データベースデータ存在確認処理(JR東日本)
def check_exist_db_data_jreast():

    pass

# データベースデータ存在確認処理(東京メトロ)
def check_exist_db_data_tokyometro():

    pass


# JR東日本利用者数取得用SQL文作成
def make_sql_jr_east():
    sql_str = ""

    return sql_str


# 東京メトロ利用者数取得用SQL分作成
def make_sql_tokyo_metro():
    sql_str = ""

    return sql_str


# JR東日本利用者数 CSV作成処理
def jr_east_makecsv(db_connection):

    pass

# 東京メトロ利用者数  CSV作成処理
def tokyo_metro_makecsv():

    pass


# メイン処理関数
def main():

    start_datetime = datetime.datetime.today()
    print("処理を開始しました: " + start_datetime.strftime("%Y/%m/%d %H:%M:%S"))

    # 処理開始時刻を保持
    start = time.time()

    # データベースコネクション作成
    with psycopg2.connect(host=DATABASE_HOST,
                          user=DATABASE_USER,
                          password=DATABASE_PASSWORD,
                          dbname=DATABASE_NAME,
                          port=DATABASE_PORT,
                          ) as db_connection:

        # データ存在確認(JR東日本)
        if check_exist_db_data_jreast():
            # データが存在する
            # JR東日本の処理を実行
            if not jr_east_makecsv(db_connection):
                # 異常終了
                return False


        if check_exist_db_data_tokyometro():
            # データが存在する
            # 東京メトロの処理を実行
            if not tokyo_metro_makecsv(db_connection):
                # 異常終了
                return False

    # 正常終了
    return True


# プログラムのエントリポイント
if __name__ == "__main__":

    # メイン処理
    if not main():
        # 異常終了
        end_datetime = datetime.datetime.today()
        print("異常終了しました: " + end_datetime.strftime("%Y/%m/%d %H:%M:%S"))
        sys.exit(-1)

    end_datetime = datetime.datetime.today()
    print("正常終了しました: " + end_datetime.strftime("%Y/%m/%d %H:%M:%S"))

    # 正常終了
    sys.exit(0)