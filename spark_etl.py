import json
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
import pandas as pd


LOAN_INPUT_PATH = 's3n://lending-club-project//raw_data/201812.csv'
OUTPUT_PATH = 's3n://lending-club-project//transformed_data/'
DEMO_DATA_PATH = 'https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=json&timezone=Europe/Berlin&lang=en'


def main():

    """process loan data and demographic data

    - read loan data from csv files from s3 bucket and transform the data, write to different buckets which is ready to load to
     different tables in a Redshift data base
    
    - get demographic data from a website, process the data and write the transformed data to a S3 bucket. This data will be loaded directly to 
     a Redshift table in the later stage.
    
    """
    
    # process loan data
    df_loan = spark.read.csv(LOAN_INPUT_PATH, header=True)
    
    process_lending_club_data(df_loan, OUTPUT_PATH)
    
    # process state demographic data
    df_demo = get_state_demographic(DEMO_DATA_PATH)
    
    df_demo.to_csv(OUTPUT_PATH + '//demographic//state_demo.csv')
    
    return None


def process_lending_club_data(df, output_path):

    """process lending-club loan data
    - get loan data and write to loan S3 bucket
    - get borrowers data and write to borrower S3 bucket
    - get credit history data and write to credit_history S3 bucket
    - get payment data and write to payment S3 bucket
    - get bad debt settlement data and write to bad debt settlement S3 bucket
    - get hardship data and write to hardship S3 bucket
  
    :param df: spark data frame which contains all information in relation to a loan
    :param output_path: str, path to s3 bucket
    """

        
    df = general_data_processing(df)
    
    process_loan_data(df, output_path)
    
    process_borrower_data(df, output_path)
    
    process_payment_data(df, output_path)
    
    process_credit_hist_data(df, output_path)
    
    process_bad_debt_settlement(df, output_path)
    
    process_hardship_data(df, output_path)
    
    
    return None
    

def general_data_processing(df):
    """preprocess the data before getting a specific subset of the data
    """
    
    df = df.withColumn('loan_id', monotonically_increasing_id())
    df = df.withColumn('borrower_id', monotonically_increasing_id())
    df = _parse_dates(df)
    df = _convert_term_to_num(df)
    
    return df

def _parse_dates(df):
    """transform the date format in the form MONTH-YEAR to timestamp format
    """
    
    date_cols = ['issue_d', 'payment_plan_start_date', 'hardship_start_date', 'hardship_end_date', 'next_pymnt_d', 'last_pymnt_d', 
                 'earliest_cr_line', 'last_credit_pull_d', 'sec_app_earliest_cr_line', 'debt_settlement_flag_date', 'settlement_date']

    month_letter_to_num = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                               'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    
    parse_date = udf(lambda x: x[-4:] + '-' + month_letter_to_num[x[:3]] + '-' + '15' if  x != None else None)
    
    for col in date_cols:
        
        df = df.withColumn(col, parse_date(df[col]))
        
    return df

def _convert_term_to_num(df):
    """convert loan terms from string to integer
    """
    
    convert_term = udf(lambda x: int(x[:-7]))
    
    df = df.withColumn('term', convert_term(df.term))
    
    return df



def process_loan_data(df, output_path):
    """get necessary loan columns data and write to a S3 bucket
    """
    
    loan_cols = ['loan_id', 'borrower_id','issue_d', 'term', 'loan_amnt', 'funded_amnt', 'funded_amnt_inv','int_rate', 'installment' ,
                 'grade', 'sub_grade', 'initial_list_status', 'application_type', 'verification_status', 
                 'verification_status_joint', 'pymnt_plan', 'url', 'desc', 'purpose', 'title',  'dti', 'dti_joint']
    
    df.select(loan_cols)\
      .write\
      .parquet(os.path.join(output_path, 'loans'),  'overwrite')
    
    return None


def process_borrower_data(df, output_path):
    """get necessary borrower columns data and write to a S3 bucket
    """
    
    borrower_cols = ['borrower_id', 'annual_inc', 'annual_inc_joint',  'emp_length', 'emp_title', 
                     'home_ownership',  'zip_code', 'addr_state']
    
    df.select(borrower_cols)\
      .write\
      .parquet(os.path.join(output_path, 'borrowers'),  'overwrite')
                     
    return None


def process_credit_hist_data(df, output_path):

    """get necessary credit history columns data and write to a S3 bucket

    :param df: loan spark data frame
    :param output_path: str, path to S3 bucket
    """
    
    credit_hist_cols = ['borrower_id', 'earliest_cr_line',  'last_credit_pull_d', 
                         'acc_now_delinq', 'acc_open_past_24mths', 'all_util', 'avg_cur_bal', 'bc_open_to_buy',
                         'bc_util', 'chargeoff_within_12_mths', 'collections_12_mths_ex_med', 'delinq_2yrs',
                         'delinq_amnt', 'il_util', 'inq_fi', 'inq_last_12m', 'inq_last_6mths',
                         'max_bal_bc', 'mo_sin_old_il_acct', 'mo_sin_old_rev_tl_op', 'mo_sin_rcnt_rev_tl_op',
                         'mo_sin_rcnt_tl', 'mort_acc', 'mths_since_last_delinq', 'mths_since_last_major_derog',
                         'mths_since_last_record', 'mths_since_rcnt_il', 'mths_since_recent_bc', 
                         'mths_since_recent_bc_dlq','mths_since_recent_inq', 'mths_since_recent_revol_delinq', 
                         'num_accts_ever_120_pd', 'num_actv_bc_tl', 'num_actv_rev_tl', 'num_bc_sats', 'num_bc_tl', 
                         'num_il_tl', 'num_op_rev_tl', 'num_rev_accts', 'num_rev_tl_bal_gt_0', 'num_sats', 
                         'num_tl_120dpd_2m', 'num_tl_30dpd', 'num_tl_90g_dpd_24m', 'num_tl_op_past_12m', 'open_acc', 
                         'open_acc_6m', 'open_il_12m', 'open_il_24m', 'open_act_il', 'open_rv_12m', 'open_rv_24m', 
                         'pct_tl_nvr_dlq', 'percent_bc_gt_75', 'policy_code', 'pub_rec', 'pub_rec_bankruptcies', 
                         'revol_bal', 'revol_util', 'tax_liens', 'tot_coll_amt', 'tot_cur_bal', 'tot_hi_cred_lim', 
                         'total_acc', 'total_bal_ex_mort', 'total_bal_il', 'total_bc_limit', 'total_cu_tl', 
                         'total_il_high_credit_limit', 'sec_app_open_act_il', 'total_rev_hi_lim', 'revol_bal_joint',
                         'sec_app_earliest_cr_line', 'sec_app_inq_last_6mths', 'sec_app_mort_acc', 'sec_app_open_acc',
                         'sec_app_revol_util', 'sec_app_num_rev_accts', 'sec_app_chargeoff_within_12_mths',
                         'sec_app_collections_12_mths_ex_med', 'sec_app_mths_since_last_major_derog']
    
    df.select(credit_hist_cols)\
      .write\
      .parquet(os.path.join(output_path, 'credit_history'),  'overwrite')
    
    return None


def process_payment_data(df, output_path):
    """get necessary payment columns data and write to a S3 bucket

    :param df: loan spark data frame
    :param output_path: str, path to S3 bucket
    """
    
    payment_cols = ['loan_id', 'loan_status', 'total_pymnt', 'last_pymnt_amnt', 'recoveries', 'collection_recovery_fee',
                    'out_prncp', 'out_prncp_inv', 'total_pymnt_inv', 'total_rec_prncp', 'total_rec_int', 
                    'total_rec_late_fee', 'next_pymnt_d', 'last_pymnt_d']
    
    df.select(payment_cols)\
      .write\
      .parquet(os.path.join(output_path, 'payment'),  'overwrite')
    
    return None


def process_bad_debt_settlement(df, output_path):

    """get necessary bad debt columns data and write to a S3 bucket

    :param df: loan spark data frame
    :param output_path: str, path to S3 bucket

    """
    
    bad_debt_cols = ['loan_id', 'debt_settlement_flag', 'debt_settlement_flag_date', 'settlement_status',
                 'settlement_date', 'settlement_amount', 'settlement_percentage', 'settlement_term']
    
    df = df.select(bad_debt_cols)\
           .filter('debt_settlement_flag=="Y"')
    
    df.write\
      .parquet(os.path.join(output_path, 'bad_debt_settlement'),  'overwrite')
    
    return None


def process_hardship_data(df, output_path):

    """get necessary hardshift columns data and write to a S3 bucket

    :param df: loan spark data frame
    :param output_path: str, path to S3 bucket
    """
    
    hardship_cols = ['loan_id', 'hardship_flag', 'hardship_type', 'hardship_reason', 'hardship_status', 
                     'deferral_term', 'hardship_amount', 'hardship_start_date', 'hardship_end_date',
                     'payment_plan_start_date', 'hardship_length', 'hardship_dpd', 'hardship_loan_status',
                     'orig_projected_additional_accrued_interest', 'hardship_payoff_balance_amount',
                     'hardship_last_payment_amount']
    
    df = df.select(hardship_cols)\
           .filter('hardship_flag == "Y"')
    
    df.write\
      .parquet(os.path.join(output_path, 'hardship'),  'overwrite')
    
    return None
    

def get_state_demographic(json_url):

    """get demographic data, transform the data and return a clean pandas data frame of demographic by state

    :param json_url: str, path to json data file 

    """
    
    df_demo = pd.read_json(json_url)
    df_demo = pd.DataFrame(df_demo.fields.tolist())
    
    # extract population by race by city
    df_race_city = df_demo[['city', 'race', 'count']]
    df_race_city = pd.pivot_table(data=df_race_city, values='count', index='city', columns='race').reset_index()
    df_race_city.columns = ['city', 'pop_american_natives', 'pop_asian', 'pop_black', 'pop_hispanic','pop_white']

    # get unique records for each city 
    df_demo_city = df_demo[['city', 'state', 'state_code', 'median_age', 'male_population', 'female_population', 
                            'count', 'number_of_veterans', 'foreign_born', 'average_household_size']]
    df_demo_city = df_demo_city.drop_duplicates()
    df_demo_city.columns = ['city', 'state_name', 'state_code', 'median_age', 'pop_male', 'pop_female', 'pop_total', 'no_vetagans', 'foreign_born', 'avg_house_size']

    # merge city demographic data with population by city and race
    df_demo_city = df_demo_city.merge(df_race_city, how='left', on='city')

    # aggregate to State level
    df_demo_state = df_demo_city.groupby(['state_code', 'state_name']).agg({'median_age': 'mean',
                                                                       'avg_house_size': 'mean',
                                                                       'pop_total': 'sum',
                                                                       'foreign_born': 'sum',
                                                                       'no_vetagans': 'sum',
                                                                       'pop_male': 'sum',
                                                                       'pop_female': 'sum',
                                                                       'pop_american_natives': 'sum',
                                                                       'pop_asian': 'sum',
                                                                       'pop_black': 'sum',
                                                                       'pop_hispanic': 'sum',
                                                                       'pop_white': 'sum'
                                                                      })
    df_demo_state = df_demo_state.reset_index()
    
    return df_demo_state


if __name__ == "__main__":

    """run main() file to execute the whole ETL process
    """
    main()
