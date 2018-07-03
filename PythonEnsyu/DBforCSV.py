# モジュール読み込み
import psycopg2
import psycopg2.extras
import sys
import time
import datetime
import math

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


# JR東日本 データ構造体
class JreastUser:
    def __init__(self):
        self.ranking_no = ''
        self.station_name = ''
        self.non_commuter = 0
        self.commuter = 0
        self.total_user = 0
        self.over_year = 0.0
        self.deviation = ""


# 東京メトロ データ構造体
class TokyoMetroUser:
    def __init__(self):
        self.ranking_no = ''
        self.line_name = ''
        self.station_name = ''
        self.total_user = 0
        self.over_year = 0.0
        self.deviation = ""


# 使い方表示
def usage():
    print("使い方")
    print(" python DBforCSV.py")
    print("")


# データ存在チェック(共通処理)
def check_exist_db_data(sql_str, db_connection):

    exist_data_count = 0

    with db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

        cursor.execute(sql_str, (SYAIN_ID,))

        dbdata = cursor.fetchall()

        if dbdata is None:
            exist_data_count = 0

        else:

            if len(dbdata) <= 0:
                exist_data_count = 0

            else:
                for rows in dbdata:
                    exist_data_count = rows["data_cnt"]

                if exist_data_count is None:
                    exist_data_count = 0

    if exist_data_count <= 0:
        # 該当データなし
        return False

    # 該当データあり
    # 正常終了
    return True


# データベースデータ存在確認処理(JR東日本)
def check_exist_db_data_jreast(db_connection):

    sql_str = "SELECT \
                  COUNT(*) data_cnt\
                FROM \
                  jr_passenger \
                WHERE \
                  syain_id = %s"

    return check_exist_db_data(sql_str, db_connection)


# データベースデータ存在確認処理(東京メトロ)
def check_exist_db_data_tokyometro(db_connection):

    sql_str = "SELECT \
                  COUNT(*) data_cnt\
                FROM \
                  metro_passenger \
                WHERE \
                  syain_id = %s"

    return check_exist_db_data(sql_str, db_connection)


# JR東日本利用者数取得用SQL文作成
def make_sql_jr_east():

    sql_str = "SELECT \
                  ranking_no \
                  , station_name \
                  , non_commuter \
                  , commuter \
                  , total_user \
                  , over_year \
                FROM \
                  jr_passenger \
                WHERE \
                  syain_id = %s \
                ORDER BY \
                  nendo \
                  ,ranking_no"

    return sql_str


# 東京メトロ利用者数取得用SQL分作成
def make_sql_tokyo_metro():

    sql_str = "SELECT \
                  ranking_no \
                  , line_name \
                  , station_name \
                  , total_user \
                  , over_year \
                FROM \
                  metro_passenger \
                WHERE \
                  syain_id = %s \
                ORDER BY \
                  nendo \
                  ,ranking_no"

    return sql_str


# JR東日本利用者数 計算処理
def jr_east_calc(jr_east_user_list, data_count):

    # 変数初期化
    total_user = 0
    total_deviation = 0.0

    # 合計人数計算
    for rec in jr_east_user_list:
        total_user = total_user + rec.total_user

    # 平均人数計算
    average = total_user / data_count

    # 偏差を計算
    for rec in jr_east_user_list:
        deviation = rec.total_user - average
        total_deviation = total_deviation + pow(deviation, 2)

    # 分散を計算
    dispersion = total_deviation / data_count

    # 標準偏差を計算
    standard_deviation = math.sqrt(dispersion)

    # 各偏差値を計算
    for rec in jr_east_user_list:
        deviation_value = (10 * (rec.total_user - average) / standard_deviation) + 50
        rec.deviation ='{:.2f}'.format(deviation_value)

    return total_user, average, standard_deviation, dispersion


# 東京メトロ利用者数 計算処理
def tokyo_metro_calc(tokyo_metro_user_list, data_count):

    # 変数初期化
    total_user = 0
    total_deviation = 0.0

    # 合計人数計算
    for rec in tokyo_metro_user_list:
        total_user = total_user + rec.total_user

    # 平均人数計算
    average = total_user / data_count

    # 偏差を計算
    for rec in tokyo_metro_user_list:
        deviation = rec.total_user - average
        total_deviation = total_deviation + pow(deviation, 2)

    # 分散を計算
    dispersion = total_deviation / data_count

    # 標準偏差を計算
    standard_deviation = math.sqrt(dispersion)

    # 各偏差値を計算
    for rec in tokyo_metro_user_list:
        deviation_value = (10 * (rec.total_user - average) / standard_deviation) + 50
        rec.deviation = '{:.2f}'.format(deviation_value)

    return total_user, average, standard_deviation, dispersion


# JR東日本利用者 CSV出力処理
def jr_east_csv_output(total_user, average, standard_deviation, dispersion, jr_east_user_list):

    # ヘッダ
    header = "合計(Total):" + str(total_user) \
             + " ,平均(Average):" + '{:.2f}'.format(average) \
             + " ,標準偏差(Standard_deviation):" + '{:.2f}'.format(standard_deviation) \
             + " ,分散(dispersion):" + '{:.2f}'.format(dispersion)

    # 見出し
    heading_line = "順位,駅名,定期外利用者,定期利用者,利用者合計,前年比,偏差値"

    with open(JR_EAST_FILE_NAME, 'w') as file_obj:
        file_obj.write(header)
        file_obj.write('\n')
        file_obj.write('\n')

        file_obj.write(heading_line)
        file_obj.write('\n')

        for rec in jr_east_user_list:
            detail = str(rec.ranking_no) \
                     + "," + rec.station_name \
                     + "," + str(rec.non_commuter) \
                     + "," + str(rec.commuter) \
                     + "," + str(rec.total_user) \
                     + "," + str(rec.over_year) \
                     + "," + str(rec.deviation)

            file_obj.write(detail)
            file_obj.write('\n')

    # 正常終了
    return True


# 東京メトロ利用者 CSV出力処理
def tokyo_metro_csv_output(total_user, average, standard_deviation, dispersion, tokyo_metro_user_list):

    # ヘッダ
    header = "合計(Total):" + str(total_user) \
             + " ,平均(Average):" + '{:.2f}'.format(average) \
             + " ,標準偏差(Standard_deviation):" + '{:.2f}'.format(standard_deviation) \
             + " ,分散(dispersion):" + '{:.2f}'.format(dispersion)

    # 見出し
    heading_line = "順位,路線名,駅名,利用者合計,前年比,偏差値"

    with open(TOKYO_METRO_FILE_NAME, 'w') as file_obj:

        file_obj.write(header)
        file_obj.write('\n')
        file_obj.write('\n')

        file_obj.write(heading_line)
        file_obj.write('\n')

        for rec in tokyo_metro_user_list:
            detail = str(rec.ranking_no) \
                     + "," + rec.line_name \
                     + "," + rec.station_name \
                     + "," + str(rec.total_user) \
                     + "," + str(rec.over_year) \
                     + "," + str(rec.deviation)

            file_obj.write(detail)
            file_obj.write('\n')

    # 正常終了
    return True


# JR東日本利用者数 CSV作成処理
def jr_east_makecsv(db_connection):

    # JR東日本利用者取得用SQL文を取得
    sql_str = make_sql_jr_east()

    with db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

        cursor.execute(sql_str, (SYAIN_ID,))

        jr_east_user_rec = cursor.fetchall()

        jr_east_user_list = []
        data_count = 0

        # データ取得
        for rows in jr_east_user_rec:
            jr_data = JreastUser()
            jr_data.ranking_no = rows["ranking_no"]
            jr_data.station_name = rows["station_name"]
            jr_data.non_commuter = rows["non_commuter"]
            jr_data.commuter = rows["commuter"]
            jr_data.total_user = rows["total_user"]
            jr_data.over_year = rows["over_year"]

            jr_east_user_list.append(jr_data)
            data_count = data_count + 1

        # 計算処理
        total_user = 0
        average = 0.0
        standard_deviation = 0.0
        dispersion = 0.0
        total_user, average, standard_deviation, dispersion = jr_east_calc(jr_east_user_list, data_count)


        # CSV出力
        ret = jr_east_csv_output(total_user, average, standard_deviation, dispersion, jr_east_user_list)

        if not ret :
            # 異常終了
            return False

    # 正常終了
    return True


# 東京メトロ利用者数  CSV作成処理
def tokyo_metro_makecsv(db_connection):

    # 東京メトロ利用者取得用SQL文を取得
    sql_str = make_sql_tokyo_metro()

    with db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

        cursor.execute(sql_str, (SYAIN_ID,))

        tokyo_metro_user_rec = cursor.fetchall()

        tokyo_metro_user_list = []
        data_count = 0

        # データ取得
        for rows in tokyo_metro_user_rec:
            metro_data = TokyoMetroUser()
            metro_data.ranking_no = rows["ranking_no"]
            metro_data.line_name = rows["line_name"]
            metro_data.station_name = rows["station_name"]
            metro_data.total_user = rows["total_user"]
            metro_data.over_year = rows["over_year"]

            tokyo_metro_user_list.append(metro_data)
            data_count = data_count + 1

        # 計算処理
        total_user = 0
        average = 0.0
        standard_deviation = 0.0
        dispersion = 0.0
        total_user, average, standard_deviation, dispersion = tokyo_metro_calc(tokyo_metro_user_list, data_count)

        # CSV出力
        ret = tokyo_metro_csv_output(total_user, average, standard_deviation, dispersion, tokyo_metro_user_list)

        if not ret:
            # 異常終了
            return False

        # 正常終了
    return True


# メイン処理関数
def main():

    start_datetime = datetime.datetime.today()
    print("処理を開始しました: " + start_datetime.strftime("%Y/%m/%d %H:%M:%S"))

    # データベースコネクション作成
    with psycopg2.connect(host=DATABASE_HOST,
                          user=DATABASE_USER,
                          password=DATABASE_PASSWORD,
                          dbname=DATABASE_NAME,
                          port=DATABASE_PORT,
                          ) as db_connection:

        # データ存在確認(JR東日本)
        if check_exist_db_data_jreast(db_connection):
            # データが存在する
            # JR東日本の処理を実行
            if not jr_east_makecsv(db_connection):
                # 異常終了
                return False
        else:
            # データが存在しない
            print("JR東日本のデータが存在しません.")

        # データ存在チェック(東京メトロ)
        if check_exist_db_data_tokyometro(db_connection):
            # データが存在する
            # 東京メトロの処理を実行
            if not tokyo_metro_makecsv(db_connection):
                # 異常終了
                return False
        else:
            # データが存在しない
            print("東京メトロのデータが存在しません.")

    # 正常終了
    return True


# プログラムのエントリポイント
if __name__ == "__main__":

    # 処理開始時刻を保持
    start = time.time()

    # メイン処理
    if not main():
        # 異常終了
        end_datetime = datetime.datetime.today()
        print("異常終了しました: " + end_datetime.strftime("%Y/%m/%d %H:%M:%S"))
        sys.exit(-1)

    end_datetime = datetime.datetime.today()
    print("正常終了しました: " + end_datetime.strftime("%Y/%m/%d %H:%M:%S"))

    # 処理時間を計算して表示
    elapsed_time = time.time() - start
    print("処理時間:{:.5}".format(elapsed_time) + "[秒]")

    # 正常終了
    sys.exit(0)
