import re
import pandas as pd
import os.pah as osp

def preprocess_requirements(x):
    x = re.sub('<BR>', '', x)
    x = re.sub("[◆【】▼※《》≪≫＊・＜＞]", "", x)
    return x

def preprocess_worktime_overtime(x):
    if "残業" not in x:
        return -1
    if "残業はありません" in x:
        return 0
    if "残業はほとんどありません" in x:
        return 0
    if "残業ほとんどありません" in x:
        return 0
    if "残業はほとんどありまん" in x:
        return 0
    if "残業はほとんどなし" in x:
        return 0
    x = x.split("残業")[1].split("<BR>")[0].split("。")[0]
    x = x.split("月")[1].split("時間")[0]
    return np.mean([int(_x) for _x in x.split("〜")])

def preprocess_worktime_off(x):
    if "休憩" not in x:
        return -1
    if "休憩はありません" in x:
        return 0
    if "休憩はほとんどありません" in x:
        return 0
    if "休憩ほとんどありません" in x:
        return 0
    if "休憩はほとんどありまん" in x:
        return 0
    if "休憩はほとんどなし" in x:
        return 0
    if "休憩はなし" in x:
        return 0
    if "休憩はありません" in x:
        return 0
    if "休憩なし" in x:
        return 0
    if "休憩は有りません" in x:
        return 0
    if "１ｈ" in x:
        return 60
    if "１Ｈ" in x:
        return 60
    x = x.split("休憩")[1].split("<BR>")[0].split("。")[0].split("分")[0].split("は")[-1].split("計")[-1]
    x = x.split("交代制で")[-1].split("交替制で")[-1].split("交替制")[-1].split("交代制")[-1]
    x = x.split("です")[0].split("）の勤務")[0]
    return np.mean([int(_x) for _x in x.split("〜")])

def encode(df, sample_df=all):
    no_use_columns = ['お仕事No.', '職場の様子', '英語力不要', '給与/交通費　給与下限', '残業なし', 'PCスキル不要', '休日休暇(金曜日)',  
                        'オフィスが禁煙・分煙', '給与/交通費　交通費', '土日祝休み', '未経験OK', 'PowerPointのスキルを活かす', '大量募集', 
                        'Excelのスキルを活かす', 'Accessのスキルを活かす', 'Wordのスキルを活かす', '休日休暇(土曜日)', '平日休みあり', 
                        '休日休暇(火曜日)', '休日休暇(月曜日)', '休日休暇(水曜日)', '休日休暇(木曜日)', '職種コード', '経験者優遇', 
                        'フラグオプション選択', '休日休暇(日曜日)', '休日休暇(祝日)', '勤務地　都道府県コード', '会社概要　業界コード', 
                        '仕事の仕方', '勤務地　市区町村コード', '期間・時間　勤務期間', '大手企業', '（派遣先）勤務先写真ファイル名', 
                        'シフト勤務', '英語力を活かす', '英語以外の語学力を活かす', '派遣形態', '週2・3日OK', '残業月20時間以上', 
                        '16時前退社OK', '10時以降出社OK', '残業月20時間未満', '1日7時間以下勤務OK', '短時間勤務OK(1日4h以内)', '服装自由', 
                        '正社員登用あり', '駅から徒歩5分以内', '外資系企業', '社員食堂あり', '勤務先公開', '週4日勤務', '車通勤OK', 
                        '制服あり', '紹介予定派遣', '学校・公的機関（官公庁）', '交通費別途支給', '派遣スタッフ活躍中', '（紹介予定）入社後の雇用形態', 
                        '勤務地　最寄駅2（駅からの交通手段）', '勤務地　最寄駅1（駅からの交通手段）', '勤務地　最寄駅1（分）', '勤務地　最寄駅2（分）', 
                        '（派遣先）配属先部署　男女比　男', '（派遣先）配属先部署　男女比　女', '（派遣先）配属先部署　人数', '（派遣先）配属先部署　平均年齢', 
                        '勤務地　最寄駅1（駅名）', '勤務地　最寄駅2（駅名）', '勤務地　最寄駅2（沿線名）', '勤務地　最寄駅1（沿線名）', 
                        '給与/交通費　給与上限', '（紹介予定）雇用形態備考', '（派遣先）配属先部署', '休日休暇　備考', '（紹介予定）休日休暇',
                        '勤務地　備考', '（紹介予定）待遇・福利厚生', '期間･時間　備考', '応募資格', '仕事内容', '期間・時間　勤務開始日', 
                        '期間・時間　勤務時間', '（派遣先）概要　勤務先名（漢字）']


    todo_columns = ['（紹介予定）入社時期', 'お仕事名', '（紹介予定）年収・給与例', 
                    'お仕事のポイント（仕事PR）', '（派遣先）職場の雰囲気', '給与/交通費　備考']
    df_ = df.copy()
    
    df_["state_workplace_label"] = df_["職場の様子"]
    df_["occupation_code_label"] = df_['職種コード']
    df_["industry_code_label"] = df_['会社概要　業界コード']
    df_["prefecture_code_label"] = df_['勤務地　都道府県コード']
    df_["town_code_label"] = df_['勤務地　市区町村コード']
    df_["option_selection_label"] = df_['フラグオプション選択']
    df_["how_to_work_label"] = df_['仕事の仕方']
    df_["working_period_label"] = df_['期間・時間　勤務期間']
    df_["employment_schedule_label"] = df_['（紹介予定）入社後の雇用形態'].fillna(-1)
    df_["location_station_2_label"] = df_['勤務地　最寄駅2（駅からの交通手段）'].fillna(-1)
    df_["location_station_1_label"] = df_['勤務地　最寄駅1（駅からの交通手段）'].fillna(-1)

    df_["location_station_2_station_name_label"] = df_["勤務地　最寄駅2（駅名）"].map(location_station_2_station_name_dict)
    df_["location_station_1_station_name_label"] = df_["勤務地　最寄駅1（駅名）"].map(location_station_1_station_name_dict)

    df_["location_line_2_line_name_label"] = df_["勤務地　最寄駅2（沿線名）"].map(location_line_2_line_name_dict)
    df_["location_line_1_line_name_label"] = df_["勤務地　最寄駅1（沿線名）"].map(location_line_1_line_name_dict)

    df_["employment_status_remarks_label"] = df_['（紹介予定）雇用形態備考'].map(employment_status_remarks_dict)
    df_["dispatching_department_label"] = df_['（派遣先）配属先部署'].map(dispatching_department_dict)
    df_["dayoff_remarks_label"] = df_['休日休暇　備考'].map(dayoff_remarks_dict)
    df_["workplace_remarks_label"] = df_['勤務地　備考'].map(workplace_remarks_dict)
    df_["diligent_name_label"] = df_['（派遣先）概要　勤務先名（漢字）'].map(diligent_name_dict)
    df_["salary_min"] = df_['給与/交通費　給与下限']

    df_["no_eng_flag"] = df_["英語力不要"]

    df_["no_overtime_flag"] = df_['残業なし']
    df_["no_pcskill_flag"] = df_['PCスキル不要']
    df_["no_smoke_flag"] = df_['オフィスが禁煙・分煙']
    df_["weekday_off_flag"] = df_['平日休みあり']
    
    df_["monday_off_flag"] = df_['休日休暇(月曜日)']
    df_["tueday_off_flag"] = df_['休日休暇(火曜日)']
    df_["wedday_off_flag"] = df_['休日休暇(水曜日)']
    df_["thrday_off_flag"] = df_['休日休暇(木曜日)']
    df_["friday_off_flag"] = df_['休日休暇(金曜日)']
    df_["sutday_off_flag"] = df_['休日休暇(土曜日)']
    df_["sunday_off_flag"] = df_['休日休暇(日曜日)']

    df_["off_sum"] =   df_["monday_off_flag"] + df_["tueday_off_flag"] + df_["wedday_off_flag"] + df_["thrday_off_flag"] + df_["friday_off_flag"] + df_["sutday_off_flag"] + df_["sunday_off_flag"]
    df_["weekend_off_flag"] = df_['土日祝休み']
    df_["holiday_off_flag"] = df_['休日休暇(祝日)']
    df_["carfare_flag"] = df_['給与/交通費　交通費']
    
    df_["inexperienced_ok_flag"] = df_['未経験OK']
    df_["PowerPoint_skill_flag"] = df_['PowerPointのスキルを活かす']
    df_["Excel_skill_flag"] = df_['Excelのスキルを活かす']
    df_["Access_skill_flag"] = df_['Accessのスキルを活かす']
    df_["Word_skill_flag"] = df_['Wordのスキルを活かす']

    df_["mass_recruit_flag"] = df_['大量募集']
    df_["mejar_company_flag"] = df_['大手企業']
    df_["picture_flag"] = df_['（派遣先）勤務先写真ファイル名'].isna()*1.0

    df_["period_remarks_flag"] = df_['期間･時間　備考'].isna()*1.0

    df_["shift_working_flag"] = df_['シフト勤務']
    df_["experienced_treat_flag"] = df_['経験者優遇']
    df_["english_skill_flag"] = df_['英語力を活かす']
    df_["foreign_lang_skill_flag"] = df_['英語以外の語学力を活かす']
    df_["dispatch_flag"] = df_['派遣形態']
    df_["2-3days_working_flag"] = df_['週2・3日OK']

    df_["overtime_morethan_20h_flag"] = df_['残業月20時間以上']
    df_["overtime_lessthan_20h_flag"] = df_['残業月20時間未満']
    df_["leave_in_16_flag"] = df_['16時前退社OK']
    df_["arrive_after_10_flag"] = df_['10時以降出社OK']
    df_["worktime_lessthan_7_flag"] = df_['1日7時間以下勤務OK']
    df_["worktime_lessthan_4_flag"] = df_['短時間勤務OK(1日4h以内)']
    df_["fulltime_assign_able_flag"] = df_['正社員登用あり']

    df_["station_in_5_flag"] = df_['駅から徒歩5分以内']
    df_["employee_caffe_flag"] = df_['社員食堂あり']
    df_["foreing_fund_flag"] = df_['外資系企業']
    df_["clothing_free_flag"] = df_['服装自由']
    df_["workplace disclosure_flag"] = df_['勤務先公開']
    df_["workdays_4_flag"] = df_['週4日勤務']
    df_["car_commute_flag"] = df_['車通勤OK']
    df_["unifotm_flag"] = df_['制服あり']
    df_["dispatching_flag"] = df_['紹介予定派遣']

    df_["schooles_publics_flag"] = df_['学校・公的機関（官公庁）']
    df_["trans_paid_flag"] = df_['交通費別途支給']
    df_["dispatching_active_flag"] = df_['派遣スタッフ活躍中']
    

    df_["location_station_2_minute_m1"] = df_['勤務地　最寄駅2（分）'].fillna(-1)
    df_["location_station_1_minute_m1"] = df_['勤務地　最寄駅1（分）'].fillna(-1)
    df_["location_station_2_minute_mean"] = df_['勤務地　最寄駅2（分）'].fillna(sample_df['勤務地　最寄駅2（分）'].mean())
    df_["location_station_1_minute_mean"] = df_['勤務地　最寄駅1（分）'].fillna(sample_df['勤務地　最寄駅1（分）'].mean())
    
    df_["dispatch_mfratio_men_m1"] = df_['（派遣先）配属先部署　男女比　男'].fillna(-1)

    df_["dispatch_mfratio_women_m1"] = df_['（派遣先）配属先部署　男女比　女'].fillna(-1)

    df_["dispatch_populatie_m1"] = df_['（派遣先）配属先部署　人数'].fillna(-1)

    df_["dispatch_age_m1"] = df_['（派遣先）配属先部署　平均年齢'].fillna(-1)

    df_["salary_limit"] = df_['給与/交通費　給与上限'].fillna(-1)

    df_["salary_range"] = (df_["salary_limit"] - df_["salary_min"]).map(lambda x: max(x, 0))

    df_["off_year"] = df_["（紹介予定）休日休暇"].map(lambda x: int(x.split('日')[1]) if type(x) == str else -1)

    df_["work_start_day"] = df_["期間・時間　勤務開始日"].map(lambda x : int(x.split('/')[0])*1e+4+int(x.split('/')[1])*1e+2+int(x.split('/')[2]))
    df_["work_start_time"] = df_["期間・時間　勤務時間"].map(lambda x : int(x.split("〜")[0].split(":")[0])*1e+2+int(x.split("〜")[0].split(":")[1]) ) 
    df_["work_end_time"] = df_["期間・時間　勤務時間"].map(lambda x : int(x.split("〜")[1].split("\u3000")[0].split(":")[0])*1e+2+int(x.split("〜")[1].split("\u3000")[0].split(":")[1]) ) 
    df_["work_diff_time"] = df_["work_end_time"] - df_["work_start_time"]

    df_["worktime_overwork"] = df_["期間・時間　勤務時間"].map(preprocess_worktime_overtime)
    df_["worktime_off"] = df_["期間・時間　勤務時間"].map(preprocess_worktime_off)

    df_welfare = tfidf_welfare.transform(df_["（紹介予定）待遇・福利厚生"].fillna("None"))
    df_welfare = pd.DataFrame(svd_welfare.transform(df_welfare))
    df_welfare.columns = [f"welfare_{c}" for c in df_welfare.columns]

    df_requirement = tfidf_requirement.transform(df_['応募資格'].fillna("None"))
    df_requirement = pd.DataFrame(svd_requirement.transform(df_requirement))
    df_requirement.columns = [f"requirement_{c}" for c in df_requirement.columns]

    df_description = tfidf_description.transform(df_['仕事内容'].fillna("None"))
    df_description = pd.DataFrame(svd_description.transform(df_description))
    df_description.columns = [f"description_{c}" for c in df_description.columns]

    df_ = pd.concat([
                     df_, 
                     df_welfare,
                     df_requirement,
                     df_description
    ], axis=1)

    df_ = df_.drop(columns=todo_columns)


    return df_.drop(columns=no_use_columns)

DATA_DIR = "./input"
PRETRAIN_DIR = "./pretrained"
OUTPUT_DIR = "./outputined"

svd_flags = pd.read_pickle(osp.join(PRETRAIN_DIR, 'svd_flags.pkl'))
location_station_2_station_name_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'location_station_2_station_name_dict.pkl'))
location_station_1_station_name_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'location_station_1_station_name_dict.pkl'))
location_line_2_line_name_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'location_line_2_line_name_dict.pkl'))
location_line_1_line_name_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'location_line_1_line_name_dict.pkl'))
employment_status_remarks_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'employment_status_remarks_dict.pkl'))
dispatching_department_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'dispatching_department_dict.pkl'))
dayoff_remarks_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'dayoff_remarks_dict.pkl'))
workplace_remarks_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'workplace_remarks_dict.pkl'))
diligent_name_dict = pd.read_pickle(osp.join(PRETRAIN_DIR, 'diligent_name_dict.pkl'))
tfidf_welfare = pd.read_pickle(osp.join(PRETRAIN_DIR, 'tfidf_welfare.pkl'))
svd_welfare = pd.read_pickle(osp.join(PRETRAIN_DIR, 'svd_welfare.pkl'))
tfidf_requirement = pd.read_pickle(osp.join(PRETRAIN_DIR, 'tfidf_requirement.pkl'))
svd_requirement = pd.read_pickle(osp.join(PRETRAIN_DIR, 'svd_requirement.pkl'))
tfidf_description = pd.read_pickle(osp.join(PRETRAIN_DIR, 'tfidf_description.pkl'))
svd_description = pd.read_pickle(osp.join(PRETRAIN_DIR, 'svd_description.pkl'))