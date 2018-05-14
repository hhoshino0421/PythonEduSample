# モジュール読み込み
import psycopg2
import os
import sys
import time

# 定数定義
# データベース接続情報
DATABASE_HOST = "192.168.8.106"
DATABSE_PORT = 5432
DATABASE_NAME = "pytestdb"
DATABASE_USER = "pytest"
DATABASE_PASSWORD = "pytest0"

# CSVファイル名
JR_EAST_FILE_NAME = "2016年度JR東日本駅別一日平均利用者数.csv"
TOKYO_METRO_FILE_NAME = "2016年度東京メトロ駅別一日平均利用者数.csv"

# 社員番号
SYAIN_ID = "00053378"

# 年度
NENDO_2016 = "2016"


# 使い方表示
def usage():
    print("使い方")
    print(" python 00053378_Ensyu2.py")
    print("")
    print("カレントディレクトリに以下のファイルを置いてください。")
    print(JR_EAST_FILE_NAME)
    print(TOKYO_METRO_FILE_NAME)
    print("")


# CSVファイル存在確認
def check_read_file_exist():

    if not os.path.isfile(JR_EAST_FILE_NAME):
        # JR用のファイルが存在しない
        print(JR_EAST_FILE_NAME + "がカレントディレクトリに存在しません")
        # 使い方表示
        usage()
        return False

    elif not os.path.isfile(TOKYO_METRO_FILE_NAME):
        # 東京メトロ用のファイルが存在しない
        print(TOKYO_METRO_FILE_NAME + "がカレントディレクトリに存在しません")
        usage()
        return False

    # 正常終了
    return True


# 利用者データ削除処理
def all_data_delete(db_connection):

    try:
        sql_jr_str = "DELETE FROM \
                jr_passenger \
                WHERE \
                syain_id = %s"

        with db_connection.cursor() as cursor:
            cursor.execute(sql_jr_str, (SYAIN_ID,))

        sql_metro_str = "DELETE FROM \
                metro_passenger \
                WHERE \
                syain_id = %s"

        with db_connection.cursor() as cursor:
            cursor.execute(sql_metro_str, (SYAIN_ID,))

    except Exception as ee:
        print("両者データ削除処理で予期しないエラー発生")
        print(ee)
        return False

    # 正常終了
    return True


# JR東日本利用者数登録用SQL文作成
def make_sql_jr_east():

    sql_str = "INSERT INTO  jr_passenger \
                ( \
                syain_id \
                , nendo \
                , ranking_no \
                , station_name \
                , non_commuter  \
                , commuter \
                , total_user \
                , over_year \
                , insert_id \
                , insert_dt \
                , update_id \
                , update_dt \
                ) \
                VALUES \
                ( \
                %s \
                ,%s \
                ,%s \
                ,%s \
                ,%s \
                ,%s \
                ,%s \
                ,%s \
                ,%s \
                ,clock_timestamp() \
                ,%s \
                ,clock_timestamp() \
                )"

    return sql_str


# 東京メトロ利用者数登録用SQL文作成
def make_sql_tokyo_metro():

    sql_str = "INSERT INTO metro_passenger \
                ( \
                syain_id \
                , nendo \
                , ranking_no \
                , line_name \
                , station_name \
                , total_user \
                , over_year \
                , insert_id \
                , insert_dt \
                , update_id \
                , update_dt \
                ) \
                VALUES \
                ( \
                %s \
                , %s \
                , %s \
                , %s \
                , %s \
                , %s \
                , %s \
                , %s \
                , clock_timestamp() \
                , %s \
                , clock_timestamp() \
                )"

    return sql_str


# JR東日本利用者数登録処理
def jr_east_insert(db_connection):

    # SQL文作成
    sql_str = make_sql_jr_east()

    try:

        with open(JR_EAST_FILE_NAME, "r", encoding="utf_8") as jr_east_data:

            # 初回ループフラグ
            first_line_flg = True

            # 一行ずつ読み込んで処理する
            for line_data in jr_east_data:

                if first_line_flg :
                    # １行目は見出し行のためデータ登録しない
                    first_line_flg = False
                    continue

                # 行末の改行文字を除去
                line_data_str = line_data.replace("\n", "")
                # カンマ区切りでデータを分割
                item_data = line_data_str.split(',')

                # データを文字列変換処理
                ranking_no = str(item_data[0])
                station_name = str(item_data[1])

                non_commuter = str(item_data[2])
                commuter = str(item_data[3])
                total_user = str(item_data[4])
                over_year = str(item_data[5])

                if len(over_year) <= 0:
                    # 空白文字の場合はnullに変換
                    over_year = None

                with db_connection.cursor() as cursor:
                    cursor.execute(sql_str, (SYAIN_ID
                                             , NENDO_2016
                                             , ranking_no
                                             , station_name
                                             , non_commuter
                                             , commuter
                                             , total_user
                                             , over_year
                                             , SYAIN_ID
                                             , SYAIN_ID
                                            )
                                   )

    except Exception as ee:
        print("JR東日本利用者数登録処理で予期しないエラーが発生しました")
        print(ee)
        return False

    # 正常終了
    return True


# 東京メトロ利用者数登録処理
def tokyo_metro_insert(db_connection):

    # SQL文作成
    sql_str = make_sql_tokyo_metro()

    try:

        with open(TOKYO_METRO_FILE_NAME, "r", encoding="utf_8") as tokyo_metro_data:

            # 初回ループフラグ
            first_line_flg = True

            # 一行ずつ読み込んで処理する
            for line_data in tokyo_metro_data:

                if first_line_flg :
                    # １行目は見出し行のためデータ登録しない
                    first_line_flg = False
                    continue

                # 行末の改行文字を除去
                line_data_str = line_data.replace("\n", "")
                # カンマ区切りでデータを分割
                item_data = line_data_str.split(',')

                # データを文字列変換処理
                ranking_no = str(item_data[0])
                rosen_name = str(item_data[1])
                station_name = str(item_data[2])
                total_user = str(item_data[3])
                over_year = str(item_data[4])

                if len(over_year) <= 0:
                    # 空白文字の場合はnullに変換
                    over_year = None

                with db_connection.cursor() as cursor:
                    cursor.execute(sql_str, (SYAIN_ID
                                             , NENDO_2016
                                             , ranking_no
                                             , rosen_name
                                             , station_name
                                             , total_user
                                             , over_year
                                             , SYAIN_ID
                                             , SYAIN_ID
                                            )
                                   )

    except Exception as ee:
        print("東京メトロ利用者数登録処理で予期しないエラーが発生しました")
        print(ee)
        return False

    # 正常終了
    return True


# メイン処理関数
def main():

    print("処理を開始しました")

    # 処理開始時刻を保持
    start = time.time()

    # データベースコネクション作成
    with psycopg2.connect(host=DATABASE_HOST,
                          user=DATABASE_USER,
                          password=DATABASE_PASSWORD,
                          dbname=DATABASE_NAME,
                          port=DATABSE_PORT,
                          ) as db_connection:

        # 利用者データ削除処理
        ret = all_data_delete(db_connection)

        if not ret:
            # 異常終了
            print("データ削除処理でエラーが発生しました。")
            # ロールバック処理
            db_connection.rollback()
            print("ロールバック処理を実行しました")
            # 異常終了を通知
            return False

        # JR利用者登録
        ret = jr_east_insert(db_connection)

        if not ret:
            # 異常終了
            print("JR利用者数登録処理でエラーが発生しました。")
            # ロールバック処理
            db_connection.rollback()
            print("ロールバック処理を実行しました")
            # 異常終了を通知
            return False

        # 東京メトロ利用者登録
        ret = tokyo_metro_insert(db_connection)

        if not ret:
            # 異常終了
            print("東京メトロ利用者数登録処理でエラーが発生しました。")
            # ロールバック処理
            db_connection.rollback()
            # 異常終了を通知
            return False

        # コミット処理
        db_connection.commit()
        print("コミット処理完了")

    elapsed_time = time.time() - start
    print("処理時間:{0}".format(elapsed_time) + "[sec]")

    # 正常終了
    return True


# プログラムのエントリポイント
if __name__ == "__main__":

    # ファイル存在確認
    if not check_read_file_exist():
        # ファイルが存在しない
        # 異常修了
        sys.exit(-1)

    # メイン処理
    if not main():
        # 異常終了
        print("異常終了しました")
        sys.exit(-1)

    print("正常終了しました")

    # 正常終了
    sys.exit(0)


