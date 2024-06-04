"""
-----------------------------------------------------------------------------------------
Created Date: 2024/04
version =1.0.0

Lambda API:画像判定処理終了後、任意のDBへ追加または更新する。

Input:gyomudatano:string, yomikomino:string, uketsukeno:string, userid:string
Output:True ->Success
       False->Failure
-----------------------------------------------------------------------------------------
"""

"""
MEMO: テスト環境ではSQL実行ログに関してデータを取得できる場合、ロガーで出力したほうがわかりやすい
"""
import os
from datetime import datetime
from dateutil import tz
from database import psqlDBWrapper
from get_logging import get_logging
import logging
import math
from ssm_param_store import get_paramater

# Define
SSM_PARAM_DB_PREFIX = '/sgs/database/'
APINAME = "SKIC05008E010"
FAILUR_STATUS = "9" 

def lambda_handler(event, context):
    try:
        logging = get_logging()
        object_name = event['object_name']
        # file_name = event['file_name']
        # folder_name = event['folder_name']
        uketsukeno = event['uketsukeno']
        userid = event['userid']
        gyomudatano = event['gyomudatano']
        yomikomino = event['yomikomino']
        # uketsukeno = event["toan_relation_data"][0]["uketsukeno"]
        
        folder_name = os.path.dirname(object_name)
        file_name = os.path.basename(object_name)

        selected_info = [{
                            "sentaku_bunrui1": None,
                            "sentaku_select1": None,
                            "sentaku_bunrui2": None,
                            "sentaku_select2": None
                        }] # 選択情報
        slected_komoku_info = [{
                            "sentaku_bunrui1": None,
                            "sentaku_select1": None,
                            "sentaku_bunrui2": None,
                            "sentaku_select2": None
                        }] # 選択項目情報
        gamentype = [] # 画面タイプ

        # DB接続
        psqlDBConn = psqlDBWrapper(
            get_paramater(SSM_PARAM_DB_PREFIX + 'host', True),
            get_paramater(SSM_PARAM_DB_PREFIX + 'db_name'),
            get_paramater(SSM_PARAM_DB_PREFIX + 'username'),
            get_paramater(SSM_PARAM_DB_PREFIX + 'password', True)
        )

        # トランザクション開始
        res = psqlDBConn.Open()
        psqlDBConn._connection.autocommit = False
        if not res:
            logging.error(
                f"[{APINAME}][ERROR]Connect to postgreSQL was failed. ObjectName: {object_name}")
        # トランザクション開始直後の日時を取得
        date_format='%Y-%m-%d %H:%M:%S.%f'
        transaction_start_date_time = datetime.now().strftime(date_format)[:23]

        """ -----------------イメージ取込情報更新-------------------"""
        # update: イメージ取り込み情報更新処理
        update_image_import_info(gyomudatano, uketsukeno, transaction_start_date_time, userid, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [UPDATE] function name: update_image_import_info ObjectName: {object_name}")

        """ -----------------採点対象項目抽出処理-------------------"""
        # get:領域モード情報取得
        ryoiki_mode = get_ryoiki_mode(gyomudatano, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_ryoiki_mode ObjectName: {object_name}")
        # print("領域モード情報取得",ryoiki_mode)
        if not isinstance(ryoiki_mode, bool) and len(ryoiki_mode) == 1:
            if ryoiki_mode[0]['ryoikimode'] is True:
                # get:領域選択問題情報取得
                ryoiki_info = get_ryoiki_info(gyomudatano, psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_ryoiki_info ObjectName: {object_name}")
                # print("領域選択問題情報取得",ryoiki_info)
                if len(ryoiki_info) >= 1:
                    selected_info = [
                        {
                            "sentaku_bunrui1": info["ryoikibunrui"],
                            "sentaku_select1": info["ryoikiselect"],
                            "sentaku_bunrui2": None,
                            "sentaku_select2": None
                        } for info in ryoiki_info
                    ]
                else:
                    trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "2-2-1")    
                    logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
                    logging.error(f"[{APINAME}][ERROR]There is no item of acquire area mode information. ObjectName: {object_name}")
                    err_process_number = "2:2-1"
                    logging.error(f"[{APINAME}][ERROR]{err_process_number}")
                    err_process(object_name, folder_name, file_name)
                    return False, "2:2-1"
            else:
                # get:項目情報取得
                koumoku_info = get_koumoku_info(yomikomino, psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_koumoku_info ObjectName: {object_name}")
                # print("項目情報取得",koumoku_info)
                if len(koumoku_info) > 0:
                    slected_komoku_info =[
                        {
                            "komokuno": info["komokuno"],
                            "sentaku_bunrui1": info["sentaku_bunrui1"],
                            "sentaku_select1": info["sentaku_select1"],
                            "sentaku_bunrui2": info["sentaku_bunrui2"],
                            "sentaku_select2": info["sentaku_select2"]
                        } for info in koumoku_info
                    ]
                    for info in slected_komoku_info:
                        if info["sentaku_bunrui1"] is None:
                            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "3-3-1")    
                            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
                            logging.error(f"[{APINAME}][ERROR]There is no item of item definition management. ObjectName: {object_name}")
                            logging.error(f"[{APINAME}][ERROR]3:3-1")
                            err_process(object_name, folder_name, file_name)
                            return False, "3:3-1"
                        
                        record_no = math.ceil(info["komokuno"] / 100)
                        column_no = (info["komokuno"] -1) % 100 + 1

                        # get:選択項目回答情報取得
                        kaitou_tokuten_info = get_kaitou_tokuten_info(gyomudatano, uketsukeno, record_no, column_no, psqlDBConn)
                        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_kaitou_tokuten_info ObjectName: {object_name}")
                        # print("選択項目回答情報取得",kaitou_tokuten_info)
                        kaito_key = "kaito_" + str(column_no) # 値を動的に変更するためのキー

                        if len(kaitou_tokuten_info) == 1:
                            # 4-5-7
                            # get:項目定義管理情報取得
                            koumoku_info = get_koumoku_info2(yomikomino, info["komokuno"], kaitou_tokuten_info[0][kaito_key], psqlDBConn)
                            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_koumoku_info2 ObjectName: {object_name}")
                            # print("項目定義管理情報取得",koumoku_info)
                            selected_info = generate_selected_info(koumoku_info, kaitou_tokuten_info)
                            # print("selected_info",selected_info)
                        else:
                            ocr_kaitou_tokuten_info = get_ocr_kaitou_tokuten_info(gyomudatano, uketsukeno, record_no, column_no, psqlDBConn)
                            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_ocr_kaitou_tokuten_info ObjectName: {object_name}")
                            # print("OCR解答得点情報取得",ocr_kaitou_tokuten_info)
                            if len(ocr_kaitou_tokuten_info) == 1:
                                # 4-5-7 ~ 4-5-10 
                                # get:項目定義管理情報取得
                                koumoku_info = get_koumoku_info2(yomikomino, info["komokuno"], ocr_kaitou_tokuten_info[0][kaito_key], psqlDBConn)
                                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_koumoku_info2 ObjectName: {object_name}")
                                # print("項目定義管理情報取得",koumoku_info)
                                selected_info = generate_selected_info(koumoku_info, ocr_kaitou_tokuten_info)
                                # print("selected_info",selected_info)
                            else:
                                trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "4:4-5-7")
                                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
                                logging.error(f"[{APINAME}][ERROR]Item Definition Management Information List is not 1. ObjectName: {object_name}")
                                logging.error(f"[{APINAME}][ERROR]4:4-5-7")
                                err_process(object_name, folder_name, file_name)
                                return False, "4:4-5-7"
        else:
            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "2:2-3-1")
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]Area mode information acquisition list is not 1. ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]2:2-3-1")
            err_process(object_name, folder_name, file_name)
            return False, "2:2-3-1"
        
        """ -----------------問題種別一覧取得処理-------------------"""
        # get:問題種別一覧取得
        system_info = get_system_info(psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_system_info ObjectName: {object_name}")
        # print("問題種別一覧取得",system_info)
        if system_info is None:
            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "5:1-1")
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]Failed to retrieve list of problem types. {object_name}")
            logging.error(f"[{APINAME}][ERROR]5:1-1")
            err_process(object_name, folder_name, file_name)
            return False, "5:1-1"
        
        """ -----------------解答項目情報作成-------------------"""
        # get:業務データ階層情報取得
        gyomudata_hierarchy = get_gyomudata_hierarchy(gyomudatano, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_gyomudata_hierarchy ObjectName: {object_name}")
        # print("業務データ階層情報取得",gyomudata_hierarchy)
        if gyomudata_hierarchy is None or len(gyomudata_hierarchy) == 0:
            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "6:1-1")
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]Failed to retrieve gyomudata hierarchy. {object_name}")
            logging.error(f"[{APINAME}][ERROR]6:1-1")
            err_process(object_name, folder_name, file_name)
            return False, "6:1-1"
        
        if len(gyomudata_hierarchy) == 1:
            training_mode_of_trn_batchtree = gyomudata_hierarchy[0]["trainingmode"]
        else:
            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "6:1-2")
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]There is not a single mode classification for business data hierarchy information.. ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]1:1-2-2")
            err_process(object_name, folder_name, file_name)
            return False, "6:1-2-2"
        
        datano_list = [info["datano"] for info in system_info]
        # get:非選択問題集計対象項目定義管理情報取得
        non_selected_problem_aggregation_target_info_list = get_non_selected_problem_aggregation_target_info_bulk(yomikomino, datano_list, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_non_selected_problem_aggregation_target_info_bulk ObjectName: {object_name}")
        # print("非選択問題集計対象項目定義管理情報取得",non_selected_problem_aggregation_target_info_list)
        # get:選択問題集計対象項目定義管理情報取得
        selected_problem_aggregation_target_info_list = get_selected_problem_aggregation_target_info_bulk(yomikomino, datano_list, selected_info, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method :[GET] function name: get_selected_problem_aggregation_target_info_bulk ObjectName: {object_name}")
        # print("選択問題集計対象項目定義管理情報取得",selected_problem_aggregation_target_info_list)     
        # Combine the two lists
        combined_info_list = non_selected_problem_aggregation_target_info_list + selected_problem_aggregation_target_info_list
        if len(combined_info_list) > 0:
            for info in combined_info_list:
                # 自動認識チェック
                if training_mode_of_trn_batchtree != 0 and (info["komokukind"] == 3 and info["shorikind"] == 18 and (info["saitenkind"] == 6 or info["saitenkind"] == 7)):
                    komoku_aggregation = 0
                    if info["verifytargetflg"] is False or info["ocrtargetflg"] is True:
                        komoku_aggregation += 1
                    else:
                        komoku_aggregation += 2

                    if komoku_aggregation > 0:
                        # insert:解答項目情報作成
                        insert_kaitou_koumoku_info(gyomudatano, uketsukeno, info["mondaikind"], komoku_aggregation, transaction_start_date_time, userid, psqlDBConn)
                        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[INSERT] function name: insert_kaitou_koumoku_info ObjectName: {object_name}")
                # if training_mode_of_trn_batchtree != 0 or not (info["komokukind"] == 3 and info["shorikind"] == 18 and (info["saitenkind"] == 6 or info["saitenkind"] == 7)):
                #     if info["verifytargetflg"] is False or info["ocrtargetflg"] is True:
                #         komoku_aggregation += 1
                #     else:
                #         komoku_aggregation += 2
                #     if komoku_aggregation > 0:
                #         # insert:解答項目情報作成
                #         insert_kaitou_koumoku_info(gyomudatano, uketsukeno, info["mondaikind"], komoku_aggregation, transaction_start_date_time, userid, psqlDBConn)
                #         logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[INSERT] function name: insert_kaitou_koumoku_info ObjectName: {object_name}")
                #         komoku_aggregation = 0
        
        """ -----------------解答作文減点情報更新-------------------"""
        # get:項目定義管理情報画面タイプ抽出
        gamentype = [8]
        komoku_screen_type_8 = get_komoku_definition_management_screen_type_extraction(yomikomino, gamentype, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[GET] function name: get_komoku_definition_management_screen_type_extraction ObjectName: {object_name}")
        # print("項目定義管理情報画面タイプ抽出[8]",komoku_screen_type_8)
        if len(komoku_screen_type_8) > 0:
        # MEMO: 1-2の判定は不要なためコードでは表記していない(komoku_screen_typeが空の時forループに入らないため)
            for info in komoku_screen_type_8:
                # delete:解答作文減点情報削除
                delete_sakubun_genten_info(gyomudatano, uketsukeno, info["komokuno"], psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [DELETE] function name: delete_sakubun_genten_info ObjectName: {object_name}")
                #MEMO: 1-3-2の判定は不要なためコードでは表記していない
                if info["outtargetkind"] != 0:
                    # insert:解答作文減点情報作成
                    insert_sakubun_genten_info(gyomudatano, uketsukeno, info["komokuno"], transaction_start_date_time, userid, psqlDBConn)
                    logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[INSERT] function name: insert_sakubun_genten_info ObjectName: {object_name}")

        """ -----------------解答英作文減点情報更新-------------------"""
        # MEMO: SQL交番12,15はgamentype変数をもうけ、1つに省略
        # get:項目定義管理情報画面タイプ抽出
        gamentype = [9]
        komoku_screen_type_9 = get_komoku_definition_management_screen_type_extraction(yomikomino, gamentype, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[GET] function name: get_komoku_definition_management_screen_type_extraction ObjectName: {object_name}")
        # print("項目定義管理情報画面タイプ抽出[9]",komoku_screen_type_9)
        if len(komoku_screen_type_9) > 0:
            # MEMO: 1-2の判定は不要なためコードでは表記していない(komoku_screen_typeが空の時forループに入らないため)
            for info in komoku_screen_type_9:
                # delete:解答英作文減点情報削除
                delete_eisakubun_genten_info(gyomudatano, uketsukeno, info["komokuno"], psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[DELETE] function name: delete_eisakubun_genten_info ObjectName: {object_name}")
                #MEMO: 1-3-2の判定は不要なためコードでは表記していない
                if info["outtargetkind"] != 0:
                    # insert:解答英作文減点情報作成
                    insert_eisakubun_genten_info(gyomudatano, uketsukeno, info["komokuno"], transaction_start_date_time, userid, psqlDBConn)
                    logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method:[INSERT] function name: insert_eisakubun_genten_info ObjectName: {object_name}")

        """ -----------------解答減点情報更新-------------------"""
        # delete:解答減点情報削除
        delete_genten_info(gyomudatano, uketsukeno, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [DELETE] function name: delete_genten_info ObjectName: {object_name}")

        # get:解答減点情報取得
        # MEMO: get_komoku_definition_management_infoでrecnoも取得するように変更
        komoku_definition_management_info = get_komoku_definition_management_info(yomikomino, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_komoku_definition_management_info ObjectName: {object_name}")
        # print("解答減点情報取得",komoku_definition_management_info)
        if len(komoku_definition_management_info) > 0:
            for info in komoku_definition_management_info:
                # insert:解答減点情報作成
                # MEMO: insert_genten_infoでrecnoもインサートするように変更
                insert_genten_info(gyomudatano, uketsukeno, info["komokuno"], transaction_start_date_time, userid, psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: insert_genten_info ObjectName: {object_name}")

        """ -----------------解答複数正誤設問減点情報-------------------"""
        # MEMO: SQL交番12,15,21はgamentype変数をもうけ、1つに省略
        # get:項目定義管理情報画面タイプ抽出
        gamentype = [10,11]
        komoku_screen_type_10_11 = get_komoku_definition_management_screen_type_extraction(yomikomino, gamentype, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_komoku_definition_management_screen_type_extraction ObjectName: {object_name}")

        # print("項目定義管理情報画面タイプ抽出[10,11]",komoku_screen_type_10_11)
        if len(komoku_screen_type_10_11) > 0:
            for info in komoku_screen_type_10_11:
                # delete:解答複数正誤設問減点情報削除
                delete_kaitou_fukususeigimon_genten_info(gyomudatano, uketsukeno, info["komokuno"], psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [DELETE] function name: delete_kaitou_fukususeigimon_genten_info ObjectName: {object_name}")
                #MEMO: 1-3-2の判定は不要なためコードでは表記していない
                if info["outtargetkind"] != 0:
                    # insert:解答英作文減点情報作成
                    insert_kaitou_fukususeigimon_genten_info(gyomudatano, uketsukeno, info["komokuno"], transaction_start_date_time, userid, psqlDBConn)
                    logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: insert_kaitou_fukususeigimon_genten_info ObjectName: {object_name}")

        """-----------------画像アップロード情報更新-------------------"""
        # update:画像アップロード情報取得
        image_upload_info = get_image_upload_info(folder_name, file_name, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_image_upload_info ObjectName: {object_name}")
        # print("画像アップロード情報取得",image_upload_info)
        if(len(image_upload_info) == 1):
            if(image_upload_info[0]["status"] != "9"):
                update_image_upload_info(folder_name, file_name, transaction_start_date_time, userid, psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [UPDATE] function name: update_image_upload_info ObjectName: {object_name}")
                # print("イメージ取り込み情報更新",update_image)
        else:
            trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, "11:2-2")
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: trn_post_batch_error_insert ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]Image upload information is not a single record. ObjectName: {object_name}")
            logging.error(f"[{APINAME}][ERROR]2-2")
            err_process(object_name, folder_name, file_name)
            return False, "11:2-2"

        """-----------------業務ツリー中間管理情報作成-------------------"""
        process_gyomu_data(gyomudatano, psqlDBConn, transaction_start_date_time, object_name)

        """----------------未採点一覧業務データ中間管理情報更新-------------------"""
        # update:未採点一覧業務データ中間管理情報更新
        update_untouched_list_gyomu_data_mid_management_info(gyomudatano, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [UPDATE] function name: update_untouched_list_gyomu_data_mid_management_info ObjectName: {object_name}")
        # print("未採点一覧業務データ中間管理情報更新",untouched_list_gyomu_data_mid_management_info)
        """----------------解答得点情報無回答更新-------------------"""

        targets_trn_mukaitohanteiresult = get_trn_mukaitohanteiresult(uketsukeno, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_trn_mukaitohanteiresult ObjectName: {object_name} count: {len(targets_trn_mukaitohanteiresult)}")
        for mukaito in targets_trn_mukaitohanteiresult:
            record_no = math.ceil(mukaito["komokuno"] / 100)
            column_no = (mukaito["komokuno"] - 1) % 100 + 1
            update_trn_ansscore_mukaito(gyomudatano, uketsukeno, record_no, column_no, psqlDBConn)    
            logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [UPDATE] function name: update_trn_ansscore_mukaito ObjectName: {object_name}")

        """-----------------トランザクションの終了-------------------"""
        # コミットトランザクション
        psqlDBConn.Commit()
        psqlDBConn.Close()
        return True      
    except Exception as e:
        if 'psqlDBConn' in locals():
            psqlDBConn.Rollback()
        logging.error(f"[{APINAME}][ERROR]{e}")
        logging.error(f"[{APINAME}][ERROR]exception")
        return False, "exception"

# 項目定義管理情報取得を選択項目情報への代入する関数
def generate_selected_info(koumoku_info, kaitou_tokuten_info):
    selected_info = []
    if len(koumoku_info) == 1:
        selected_info = [
            {
                "sentaku_bunrui1": info["sentaku_bunrui1"],
                "sentaku_select1": info["sentaku_select1"],
                "sentaku_bunrui2": None,
                "sentaku_select2": None
            } for info in koumoku_info
        ]
    else:
        for info in koumoku_info:
            if info["sentaku_select2"] != None:
                selected_info.append(
                    {
                        "sentaku_bunrui1": info["sentaku_bunrui1"],
                        "sentaku_select1": info["sentaku_select1"],
                        "sentaku_bunrui2": info["sentaku_bunrui2"],
                        "sentaku_select2": kaitou_tokuten_info[0]
                    }
                )
            else:
                selected_info.append(
                    {
                        "sentaku_bunrui1": info["sentaku_bunrui1"],
                        "sentaku_select1": kaitou_tokuten_info[0],
                        "sentaku_bunrui2": None,
                        "sentaku_select2": None
                    }
                )
    return selected_info


def process_gyomu_data(gyomudatano, psqlDBConn, transaction_start_date_time, object_name):
    """
    # 業務ツリー中間管理情報作成
    """
    target_gyomudatano = gyomudatano
    while True:
        gyomu_tree_info = get_gyomu_tree_info(target_gyomudatano, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_gyomu_tree_info ObjectName: {object_name}")
        # print("業務ツリー中間管理情報", gyomu_tree_info)

        if gyomu_tree_info:
            break

        gomu_data_hierarchy_info = get_gyomu_data_hierarchy_info(target_gyomudatano, psqlDBConn)
        logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [GET] function name: get_gyomu_data_hierarchy_info ObjectName: {object_name}")
        # print("業務データ階層情報取得", gomu_data_hierarchy_info)

        if not gomu_data_hierarchy_info:
            break

        for info in gomu_data_hierarchy_info:
            target_gyomudatano = info["oyagyomudatano"]
            if info["gyomudatano"] is not None and info["oyagyomudatano"] is not None:
                insert_gyomu_tree_chukan_kanri_info(info["gyomudatano"], info["oyagyomudatano"], transaction_start_date_time, psqlDBConn)
                logging.info(f"[{APINAME}][INFO]SQL execution succeeded! Method: [INSERT] function name: insert_gyomu_tree_chukan_kanri_info ObjectName: {object_name}")

"""
    MEMO [SQL取得データ共通処理]
    データベースライブラリに依存しているため、必要ならリストのリストに変換する
    result = [(row,) for row in result]
"""

""" 
    更新テーブル: イメージ取込情報(trn_imageget)
    概要: イメージ取込情報更新
"""
def update_image_import_info(gyomudatano, uketsukeno, transaction_start_date_time, userid, psqlDBConn):
    try:
        query = f"""
        UPDATE trn_imageget
        SET
            imagefin = 3,
            upddate = %s,
            upduserid = %s
        WHERE
            gyomudatano = %s
            AND
            uketsukeno = %s
        """
        params = (transaction_start_date_time, userid, gyomudatano, uketsukeno)
        updated_rows = psqlDBConn.Update(query, params)
        return updated_rows is not None and updated_rows > 0
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire item information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 01")
        psqlDBConn.Rollback()
        return False
    
    
"""
    取得テーブル: 業務一覧マスタ(mst_gyomulist)
    概要: 領域モード情報取得
"""
def get_ryoiki_mode(gyomudatano, psqlDBConn):    
    try:
        query = f"""
        SELECT mst_gyomulist.ryoikimode
        FROM trn_batchtree
        INNER JOIN mst_gyomulist
        ON trn_batchtree.gyomukind = mst_gyomulist.gyomukind
        WHERE trn_batchtree.gyomudatano = %s
        """
        params = (gyomudatano,)
        result = psqlDBConn.Select(query, params, isAll=True)
        if result is None:
            logging.error(f"[{APINAME}][ERROR]Failed to execute select query.")
            return False
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire area mode information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 02")
        psqlDBConn.Rollback()
        return False
"""
    取得テーブル: 領域選択問題情報(trn_ryoikiselect)
    概要: 領域選択問題情報取得
"""
def get_ryoiki_info(gyomudatano, psqlDBConn):
    try:
        query = f"""
        SELECT
            trn_ryoikiselect.ryoikibunrui,
            trn_ryoikiselect.ryoikiselect
        FROM
            trn_ryoikiselect
        WHERE
            trn_ryoikiselect.gyomudatano = %s
        """
        params = (gyomudatano,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire information on area selection problem. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 03")
        psqlDBConn.Rollback()
        return False
    
"""
    取得テーブル: 項目定義管理(mst_komoku)
    概要: 項目定義管理取得
"""
def get_koumoku_info(yomikomino, psqlDBConn):
    try:
        query = f"""
        SELECT
            mst_komoku.komokuno,						
            mst_komoku.sentaku_bunrui1,								
            mst_komoku.sentaku_select1,								
            mst_komoku.sentaku_bunrui2,								
            mst_komoku.sentaku_select2								
        FROM
            mst_komoku
        WHERE
            mst_komoku.yomikomino = %s
            AND mst_komoku.komokukind = 4
        """
        params = (yomikomino,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire item definition management. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 04")
        psqlDBConn.Rollback()
        return False
    
"""
    取得テーブル: 解答得点情報(trn_ansscore)
    概要: 解答得点情報取得
"""
def get_kaitou_tokuten_info(gyomudatano, uketsukeno, record_no, column_no, psqlDBConn):
    try:
        query = f"""
        SELECT
            trn_ansscore.kaito_{column_no}
        FROM
            trn_ansscore
        WHERE
            trn_ansscore.gyomudatano = %s
            AND trn_ansscore.uketsukeno = %s
            AND trn_ansscore.recno = %s
            AND trn_ansscore.kaito_{column_no} IS NOT NULL
            AND TRIM(trn_ansscore.kaito_{column_no}) <> ''
        """
        params = (gyomudatano, uketsukeno, record_no)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire answer score information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 05")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: OCR解答得点情報[OCR読込結果一時保存用](trn_ocrscore)
    概要: OCR解答得点情報取得
"""
def get_ocr_kaitou_tokuten_info(gyomudatano, uketsukeno, record_no, column_no, psqlDBConn):
    try:
        query = f"""
        SELECT
            trn_ocrscore.kaito_{column_no}
        FROM
            trn_ocrscore
        WHERE
            trn_ocrscore.gyomudatano = %s
            AND trn_ocrscore.uketsukeno = %s
            AND trn_ocrscore.recno = %s
        """
        params=(gyomudatano, uketsukeno, record_no)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire OCR answer score information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 06")
        psqlDBConn.Rollback()
        return False
    
"""
    取得テーブル: 項目定義管理(mst_komoku)
    概要: 選択条件取得
"""
def get_koumoku_info2(yomikomino,komokuno,kaitou_info, psqlDBConn):
    try:
        query = f"""
        SELECT
            mst_komoku.komokuno,				
            mst_komoku.sentaku_bunrui1,								
            mst_komoku.sentaku_select1,								
            mst_komoku.sentaku_bunrui2,								
            mst_komoku.sentaku_select2								
        FROM
            mst_komoku
        WHERE
            mst_komoku.yomikomino = %s
            AND mst_komoku.sentaku_groupno = %s
            AND mst_komoku.sentaku_hantei = %s
            AND mst_komoku.komokukind = 5
        """
        params = (yomikomino, komokuno, kaitou_info)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire item definition management. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 07")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: システム情報管理(mst_syscontrol)
    概要: 問題種別一覧取得
"""
def get_system_info(psqlDBConn):    
    try:
        query = f"""
        SELECT
            mst_syscontrol.datacode,
            mst_syscontrol.datano,
            mst_syscontrol.seqdata,
            mst_syscontrol.mojidata
        FROM
            mst_syscontrol
        WHERE
            mst_syscontrol.datacode = 201
        """
        result = psqlDBConn.Select(query, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to retrieve list of problem types. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 08")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 業務データ階層情報(trn_batchtree)
    概要: モード区分取得
"""
def get_gyomudata_hierarchy(gyomudatano, psqlDBConn):    
    try:
        query = f"""
        SELECT
            trn_batchtree.trainingmode
        FROM
            trn_batchtree
        WHERE
            trn_batchtree.gyomudatano = %s
            AND
            trn_batchtree.trainingmode IS NOT NULL
        """
        params = (gyomudatano,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to retrieve gyomudata hierarchy. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 09")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 項目定義管理(mst_komoku)、採点定義管理(mst_saiten)
    概要: 非選択問題集計対象項目定義管理情報取得
"""
def get_non_selected_problem_aggregation_target_info(yomikomino, mondaikind, psqlDBConn):    
    try:
        query = f"""
       SELECT
	        mst_komoku.komokuno,
	        mst_komoku.mondaikind,
	        mst_komoku.verifytargetflg,
	        mst_komoku.ocrtargetflg,
	        mst_komoku.komokukind,
	        mst_komoku.shorikind,
	        mst_saiten.saitenkind
        FROM
	        mst_komoku
	        LEFT JOIN
        	mst_saiten
	    ON
		    mst_komoku.yomikomino = mst_saiten.yomikomino
		AND
		    mst_komoku.komokuno = mst_saiten.komokuno
        WHERE
	        mst_komoku.yomikomino = %s
	    AND
	        mst_komoku.mondaikind = %s
	    AND
	        mst_komoku.sentaku_bunrui1 IS NULL
        """
        params = (yomikomino, mondaikind)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire non-selected problem tally target item definition management information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 10")
        psqlDBConn.Rollback()
        return False
    

def get_non_selected_problem_aggregation_target_info_bulk(yomikomino, datano_list, psqlDBConn):
    try:
        query = f"""
        SELECT
            mst_komoku.komokuno,
            mst_komoku.mondaikind,
            mst_komoku.verifytargetflg,
            mst_komoku.ocrtargetflg,
            mst_komoku.komokukind,
            mst_komoku.shorikind,
            mst_saiten.saitenkind
        FROM
            mst_komoku
        LEFT JOIN
            mst_saiten
        ON
            mst_komoku.yomikomino = mst_saiten.yomikomino
            AND
            mst_komoku.komokuno = mst_saiten.komokuno
        WHERE
            mst_komoku.yomikomino = %s
            AND
            mst_komoku.mondaikind = ANY(%s)
            AND
            (
                mst_komoku.sentaku_bunrui1 IS NULL
                OR (
                    mst_komoku.sentaku_bunrui1 IS NULL 
                    AND mst_komoku.sentaku_select1 IS NULL 
                    AND mst_komoku.sentaku_bunrui2 IS NULL 
                    AND mst_komoku.sentaku_select2 IS NULL
                )
            )
        """
        params = (yomikomino, datano_list)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to retrieve komoku and saiten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 10")
        psqlDBConn.Rollback()
        return False


"""
    更新テーブル: 項目定義管理(mst_komoku)、採点定義管理(Mst_Saiten)
    概要: 選択問題集計対象項目定義管理情報取得
"""
def get_selected_problem_aggregation_target_info(yomikomino, mondaikind, sentaku_bunrui1, sentaku_select1, sentaku_bunrui2, sentaku_select2, psqlDBConn):    
    try:
        query = f"""
        SELECT
    	    mst_komoku.komokuno,	
	        mst_komoku.mondaikind,
        	mst_komoku.verifytargetflg,
        	mst_komoku.ocrtargetflg,
        	mst_komoku.komokukind,
            mst_komoku.shorikind,
	        mst_saiten.saitenkind
        FROM
	        mst_komoku
	    LEFT JOIN
	        mst_saiten
	    ON													
		    mst_komoku.yomikomino = mst_saiten.yomikomino
		AND
		    mst_komoku.komokuno = mst_saiten.komokuno
        WHERE
	        mst_komoku.yomikomino = %s
	    AND
	        mst_komoku.mondaikind = %s
	    AND
	        mst_komoku.sentaku_bunrui1 = %s
	    AND
	        mst_komoku.sentaku_select1 = %s
	    AND
	        mst_komoku.sentaku_bunrui2 = %s
	    AND
	        mst_komoku.sentaku_select2 = %s
        """
        params = (yomikomino, mondaikind, sentaku_bunrui1, sentaku_select1, sentaku_bunrui2, sentaku_select2)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire selection question tally target item definition management information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 22")
        psqlDBConn.Rollback()
        return False
    
def get_selected_problem_aggregation_target_info_bulk(yomikomino, datano_list, selected_info_list, psqlDBConn):
    try:
        results = []
        for selected_info in selected_info_list:
            query = f"""
            SELECT
                mst_komoku.komokuno,
                mst_komoku.mondaikind,
                mst_komoku.verifytargetflg,
                mst_komoku.ocrtargetflg,
                mst_komoku.komokukind,
                mst_komoku.shorikind,
                mst_saiten.saitenkind
            FROM
                mst_komoku
            LEFT JOIN
                mst_saiten
            ON
                mst_komoku.yomikomino = mst_saiten.yomikomino
                AND
                mst_komoku.komokuno = mst_saiten.komokuno
            WHERE
                mst_komoku.yomikomino = %s
                AND
                mst_komoku.mondaikind = ANY(%s)
                AND
                (
                    mst_komoku.sentaku_bunrui1 = %s
                    AND mst_komoku.sentaku_select1 = %s
                    AND mst_komoku.sentaku_bunrui2 = %s
                    AND mst_komoku.sentaku_select2 = %s
                )
            """
            params = (yomikomino, datano_list, selected_info["sentaku_bunrui1"], selected_info["sentaku_select1"], selected_info["sentaku_bunrui2"], selected_info["sentaku_select2"])
            result = psqlDBConn.Select(query, params, isAll=True)
            results.extend(result)
        return results
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to retrieve komoku and saiten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 10")
        psqlDBConn.Rollback()
        return False
    
"""
    追加テーブル: 解答項目情報(Trn_AnsKomoku)
    概要: 解答項目情報追加
"""
def insert_kaitou_koumoku_info(gyomudatano, uketsukeno, mondaikind, komokucnt, transaction_start_time, userid, psqlDBConn):
    try:
        query = """
        INSERT INTO Trn_AnsKomoku(
            gyomudatano,
            uketsukeno,
            mondaikind,
            komokucnt,
            insdate,
            insuserid,
            upddate,
            upduserid
        )
        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL)
        ON CONFLICT DO NOTHING
        """
        params = (gyomudatano, uketsukeno, mondaikind, komokucnt, transaction_start_time, userid)
        psqlDBConn.Insert(query, params)
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert into Trn_AnsKomoku. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 11")
        psqlDBConn.Rollback()
        return False
    return True

"""
    取得テーブル: 項目定義管理(mst_komoku)
    概要: 項目定義管理情報画面タイプ抽出
"""
def get_komoku_definition_management_screen_type_extraction(yomikomino, gamentypes, psqlDBConn):
    try:
        query = f"""
        SELECT
            mst_komoku.komokuno,
            mst_komoku.outtargetkind
        FROM
            mst_komoku
        WHERE
            mst_komoku.yomikomino = %s
            AND mst_komoku.gamentype IN %s
        """
        params = (yomikomino, tuple(gamentypes))
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire item definition management. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 12")
        psqlDBConn.Rollback()
        return False

"""
    削除テーブル: 解答作文減点情報(trn_anssakubun)
    概要: 解答作文減点情報削除
"""
def delete_sakubun_genten_info(gyomudatano, uketsukeno, komokuno, psqlDBConn):
    try:
        query = f"""
        DELETE
        FROM
            trn_anssakubun
        WHERE
            trn_anssakubun.gyomudatano = %s
            AND trn_anssakubun.uketsukeno = %s
            AND trn_anssakubun.komokuno = %s
        """
        params = (gyomudatano, uketsukeno, komokuno)
        result = psqlDBConn.Delete(query, params)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to delete Sakubun Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 13")
        psqlDBConn.Rollback()
        return False
    
"""
    追加テーブル: '解答作文減点情報(trn_anssakubun)
    概要: 解答作文減点情報作成
"""
def insert_sakubun_genten_info(gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id, psqlDBConn):
    try:
        query = f"""
        INSERT INTO trn_anssakubun(
            gyomudatano,
            uketsukeno,
            komokuno,
            insdate,
            insuserid
        )
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id)
        psqlDBConn.Insert(query, params)
        return True
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert Sakubun Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 14")
        psqlDBConn.Rollback()
        return False

"""
    削除テーブル: 解答英作文減点情報(trn_anseisakubun)
    概要: 解答英作文減点情報削除
"""
def delete_eisakubun_genten_info(gyomudatano, uketsukeno, komokuno, psqlDBConn):
    try:
        query = f"""
        DELETE
        FROM
            trn_anseisakubun
        WHERE
            trn_anseisakubun.gyomudatano = %s
            AND trn_anseisakubun.uketsukeno = %s
            AND trn_anseisakubun.komokuno = %s
        """
        params = (gyomudatano, uketsukeno, komokuno)
        result = psqlDBConn.Delete(query, params)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to delete Sakubun Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 16")
        psqlDBConn.Rollback()
        return False
    

"""
    追加テーブル: '解答英作文減点情報(trn_anseisakubun)
    概要: 解答英作文減点情報作成
"""
def insert_eisakubun_genten_info(gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id, psqlDBConn):
    try:
        query = f"""
        INSERT INTO trn_anseisakubun(
            gyomudatano,
            uketsukeno,
            komokuno,
            insdate,
            insuserid
        )
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id)
        psqlDBConn.Insert(query, params)
        return True
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert Sakubun Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 17")
        psqlDBConn.Rollback()
        return False

"""
    削除テーブル: 解答減点情報(trn_ansgenten)
    概要: 既存解答減点情報削除
"""
def delete_genten_info(gyomudatano, uketsukeno, psqlDBConn):
    try:
        query = f"""
        DELETE
        FROM
            trn_ansgenten
        WHERE
            trn_ansgenten.gyomudatano = %s
            AND trn_ansgenten.uketsukeno = %s
        """
        params = (gyomudatano, uketsukeno)
        result = psqlDBConn.Delete(query, params)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to delete Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 18")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 項目定義管理(mst_komoku)
    概要: 項目定義管理情報抽出
"""
def get_komoku_definition_management_info(yomikomino, psqlDBConn):
    try:
        query = f"""
        SELECT
            mst_komoku.komokuno,
            mst_komoku.outtargetkind
        FROM
            mst_komoku
        WHERE
            mst_komoku.yomikomino = %s
            AND mst_komoku.gentenflg <> 0
        """
        params = (yomikomino,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire item definition management. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 19")
        psqlDBConn.Rollback()
        return False

"""
    追加テーブル: 解答減点情報(trn_ansgenten)
    概要: 解答英作文減点情報作成
"""
def insert_genten_info(gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id, psqlDBConn):
    try:
        query = f"""
        INSERT INTO trn_ansgenten(
            gyomudatano,
            uketsukeno,
            recno,
            komokuno,
            insdate,
            insuserid
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (gyomudatano, uketsukeno, 1, komokuno, transaction_start_date_time, user_id)
        psqlDBConn.Insert(query, params)
        return True
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert Sakubun Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 20")
        psqlDBConn.Rollback()
        return False

"""
    削除テーブル: 解答複数正誤設問減点情報(trn_ansmultiseigo)
    概要: 解答複数正誤設問減点情報削除
"""
def delete_kaitou_fukususeigimon_genten_info(gyomudatano, uketsukeno, komokuno, psqlDBConn):
    try:
        query = f"""
        DELETE
        FROM
            trn_ansmultiseigo
        WHERE
            trn_ansmultiseigo.gyomudatano = %s
            AND trn_ansmultiseigo.uketsukeno = %s
            AND trn_ansmultiseigo.komokuno = %s
        """
        params = (gyomudatano, uketsukeno, komokuno)
        result = psqlDBConn.Delete(query, params)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to delete Kaitou Fukususeigimon Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 23")
        psqlDBConn.Rollback()
        return False

"""
    追加テーブル: 解答複数正誤設問減点情報(trn_ansmultiseigo)
    概要: 解答複数正誤設問減点情報作成
"""
def insert_kaitou_fukususeigimon_genten_info(gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id, psqlDBConn):
    try:
        query = f"""
        INSERT INTO trn_ansmultiseigo(
            gyomudatano,
            uketsukeno,
            komokuno,
            insdate,
            insuserid
        )
        VALUES (%s, %s, %s, %s, %s)
        """
        params = (gyomudatano, uketsukeno, komokuno, transaction_start_date_time, user_id)
        psqlDBConn.Insert(query, params)
        return True
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert Kaitou Fukususeigimon Genten info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 24")
        psqlDBConn.Rollback()
        return False


"""
    取得テーブル: 画像アップロード情報(trn_uploadimginfo)
    概要: 画像アップロード情報更新
"""
def update_image_upload_info(foldername, filename, transaction_start_date_time, userid, psqlDBConn):
    try:
        query = f"""
        UPDATE trn_uploadimginfo
        SET
            status = 1,
            upddate = %s,
            upduserid = %s,
            bucketname = 'snw-test-answersheet-original'
        WHERE
            foldername = %s
            AND
            filename = %s
        """
        params = (transaction_start_date_time, userid, foldername, filename)
        updated_rows = psqlDBConn.Update(query, params)
        return updated_rows is not None and updated_rows > 0
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to update image upload info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 25")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 業務ツリー中間管理情報(Interface_TreeGyomu)
    概要: 業務ツリー中間管理情報検索
"""
def get_gyomu_tree_info(gyomudatano, psqlDBConn):
    try:
        query = f"""
        SELECT *
        FROM Interface_TreeGyomu
        WHERE Interface_TreeGyomu.childgyomudatano = %s
        """
        params = (gyomudatano,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire gyomu tree information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 32")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 業務データ階層情報(trn_batchtree)
    概要: 業務データ階層情報取得
"""
def get_gyomu_data_hierarchy_info(gyomudatano, psqlDBConn):
    try:
        query = f"""
        SELECT 
            trn_batchtree.gyomudatano,
            trn_batchtree.oyagyomudatano,
            trn_batchtree.upddate
        FROM 
            trn_batchtree
        WHERE 
            trn_batchtree.gyomudatano = %s
        """
        params = (gyomudatano,)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire gyomu data hierarchy information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 33")
        psqlDBConn.Rollback()
        return False

"""
    追加テーブル: 業務ツリー中間管理情報(Interface_TreeGyomu)
    概要: 業務ツリー中間管理情報登録
"""
def insert_gyomu_tree_chukan_kanri_info(gyomudatano, oyagyomudatano, transaction_start_date_time, psqlDBConn):
    try:
        query = f"""
        INSERT INTO Interface_TreeGyomu(
            GyomuDataNo,
            ChildGyomuDataNo,
            UpdDate
        )
        VALUES (%s, %s, %s)
        """
        params = (oyagyomudatano, gyomudatano, transaction_start_date_time)
        psqlDBConn.Insert(query, params)
        return True
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to insert Gyomu Tree Chukan Kanri info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 34")
        psqlDBConn.Rollback()
        return False

"""
    更新テーブル: 未採点一覧業務データ中間管理情報(Interface_SaitenGyomu)
    概要:  未採点一覧業務データ中間管理情報更新
"""
def update_untouched_list_gyomu_data_mid_management_info(gyomudatano, psqlDBConn):
    try:
        query = f"""
        UPDATE Interface_SaitenGyomu
        SET
            ImageFin = 3
        WHERE
            gyomudatano = %s
        """
        params = (gyomudatano,)
        updated_rows = psqlDBConn.Update(query, params)
        return updated_rows is not None and updated_rows > 0
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to update untouched list gyomu data mid management info. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 35")
        psqlDBConn.Rollback()
        return False

"""
    取得テーブル: 画像アップロード情報(trn_uploadimginfo)
    概要: 画像アップロード情報取得
"""
def get_image_upload_info(foldername,filename, psqlDBConn):
    try:
        query = f"""
        SELECT 
            trn_uploadimginfo.status
        FROM 
            trn_uploadimginfo
        WHERE 
            trn_uploadimginfo.foldername = %s
            AND
            trn_uploadimginfo.filename = %s
        """
        params = (foldername,filename)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire image upload information. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 37")
        psqlDBConn.Rollback()
        return False

"""
追加テーブル: エラー情報(trn_postbatcherror)
概要: エラーメッセージを追加
"""
def trn_post_batch_error_insert(psqlDBConn, uketsukeno, userid, object_name, err_number):
    try:
        query = f"""
        INSERT INTO
            trn_postbatcherror (
                uketsukeno,
                kaitono,
                backetname,
                foldername,
                filename,
                gyomudatano,
                recno,
                errorcode,
                yomikomino,
                komokuno,
                shuseiflag,
                insdate,
                insuserid,
                upddate,
                upduserid
            ) VALUES (%s, '0', null, null, null, null, null, '100', null, null, 0, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP, %s)
        """
        err_process_number = err_number
        params = (uketsukeno, userid, userid)
        if psqlDBConn.Insert(query, params):
            logging.info(f"[{APINAME}][SUCCESS] trn_postbatcherror table registration process was successful. ObjectName: {object_name} uketsukeno: {uketsukeno}")
            return True
        else:
            raise Exception("Insert operation failed.")
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]trn_postbatcherror table registration process was faild. ObjectName: {object_name} uketsukeno: {uketsukeno}")
        logging.error(f"[{APINAME}][ERROR] {err_process_number}")
        psqlDBConn.Rollback()
        return False

def get_trn_mukaitohanteiresult(uketsukeno, psqlDBConn):
    try:
        query = f"""
        SELECT 
            trn_mukaitohanteiresult.komokuno
        FROM 
            trn_mukaitohanteiresult
        WHERE 
            trn_mukaitohanteiresult.uketsukeno = %s
        """
        params = (uketsukeno)
        result = psqlDBConn.Select(query, params, isAll=True)
        return result
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to acquire trn_mukaitohanteiresult. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 38")
        psqlDBConn.Close()
        return False

def update_trn_ansscore_mukaito(gyomudatano, uketsukeno, recordno, columnno, psqlDBConn):
    try:
        query = f"""
        UPDATE 
            trn_ansscore
        SET
            tokuten_%s = -5 
        WHERE
            trn_ansscore.gyomudatano = %s, 
            trn_ansscore.uketsukeno = %s, 
            trn_ansscore.recordno = %s
        """
        params = (columnno, gyomudatano, uketsukeno, recordno)
        updated_rows = psqlDBConn.Update(query, params)
        return updated_rows is not None and updated_rows > 0
    except Exception as e:
        logging.info(f"[{APINAME}][ERROR]Failed to update trn_ansscore. {e}")
        logging.error(f"[{APINAME}][ERROR]SQL item number is 39")
        psqlDBConn.Close()
        return False


""" -----------------以下は例外処理関数----------------- """
def err_process(object_name, folder_name, file_name):
    try:
        psqlDBConn = psqlDBWrapper(
            get_paramater(SSM_PARAM_DB_PREFIX + 'host', True),
            get_paramater(SSM_PARAM_DB_PREFIX + 'db_name'),
            get_paramater(SSM_PARAM_DB_PREFIX + 'username'),
            get_paramater(SSM_PARAM_DB_PREFIX + 'password', True)
        )

        result = psqlDBConn.Open()
        if not result:
            logging.error(
                f"[{APINAME}][ERROR]status update Failed. ObjectName: {object_name}")
            return None

        result = psqlDBConn.Select(
            f"SELECT upluserid FROM trn_uploaduserinfo WHERE foldername = '{folder_name}';")
        if result is None:
            logging.error(
                f"[{APINAME}][ERROR]status update Failed. ObjectName: {object_name}")
            return None
        userid = result['upluserid']
        logging.info(f"[{APINAME}][INFO]Get user id: {
                     userid} ObjectName: {object_name}"
                     )

        result = psqlDBConn.Update(f"Update trn_uploadimginfo set status= '{FAILUR_STATUS}',upddate =CURRENT_TIMESTAMP,upduserid ='{
                                   userid}' where foldername='{folder_name}' and filename ='{file_name}';"
                                   )
        if result is None:
            logging.error(
                f"[{APINAME}][ERROR]status update Failed. ObjectName: {object_name}")
            return None

        logging.info(f"[{APINAME}][INFO]Image upload information table update process was successful. ObjectName: {
                     object_name} Status: {FAILUR_STATUS}"
                     )
        return None
    except Exception as e:
        logging.error(f"[{APINAME}][ERROR]{e}")
        logging.error(f"[{APINAME}][ERROR]exception")
        psqlDBConn.Rollback()
    # finally:
    #     if 'psqlDBConn' in locals():
    #         psqlDBConn.Close()
