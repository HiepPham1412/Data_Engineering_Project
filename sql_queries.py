import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOANS_BUCKET = config['S3']['LOANS']
BORROWERS_BUCKET = config['S3']['BORROWERS']
PAYMENT_BUCKET = config['S3']['PAYMENT']
CREDIT_HIST_BUCKET= config['S3']['CREDIT_HIST']
BAD_DEBT_BUCKET = config['S3']['BAD_DEBT']
HARDSHIP_BUCKET = config['S3']['HARDSHIP']
STATE_DEMO_BUCKET = config['S3']['STATE_DEMO']
ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

loans_table_drop = "DROP TABLE IF EXISTS loans"
borrowers_table_drop = "DROP TABLE IF EXISTS borrowers"
payment_table_drop = "DROP TABLE IF EXISTS payment"
credit_history_table_drop = "DROP TABLE IF EXISTS credit_history"
bad_debt_settlement_table_drop = "DROP TABLE IF EXISTS bad_debt_settlement"
hardship_table_drop = "DROP TABLE IF EXISTS hardship"
state_demo_table_drop = "DROP TABLE IF EXISTS state_demo"

# CREATE TABLES
loans_table_create= ("""
    CREATE TABLE IF NOT EXISTS loans (
                loan_id                     INT NOT NULL,
                borrower_id                 INT,
                issue_d                     TIMESTAMP,
                term                        INT,
                loan_amnt                   FLOAT,
                funded_amnt                 FLOAT,
                funded_amnt_inv             FLOAT,
                int_rate                    FLOAT,
                installment                 FLOAT,
                grade                       VARCHAR,
                sub_grade                   VARCHAR,
                initial_list_status         VARCHAR,
                application_type            VARCHAR,
                verification_status         VARCHAR,
                verification_status_joint   VARCHAR,
                pymnt_plan                  VARCHAR,
                url                         VARCHAR,
                descrb                      VARCHAR,
                purpose                     VARCHAR,
                title                       VARCHAR,
                dti                         FLOAT,
                dti_joint                   FLOAT
    );
""")

borrowers_table_create = ("""
CREATE TABLE IF NOT EXISTS borrowers (
                borrower_id         INT NOT NULL,
                annual_inc          FLOAT,
                annual_inc_joint    FLOAT,
                emp_length          VARCHAR,
                emp_title           VARCHAR,
                home_ownership      VARCHAR,
                zip_code            VARCHAR,
                addr_state          VARCHAR
    );
""")


payment_table_create = ("""
CREATE TABLE IF NOT EXISTS payment (
                loan_id                     INT,
                loan_status                 VARCHAR,
                total_pymnt                 FLOAT,
                last_pymnt_amnt             FLOAT,
                recoveries                  FLOAT,
                collection_recovery_fee     FLOAT,
                out_prncp                   FLOAT,
                out_prncp_inv               FLOAT,
                total_pymnt_inv             FLOAT,
                total_rec_prncp             FLOAT,
                total_rec_int               FLOAT,
                total_rec_late_fee          FLOAT,
                next_pymnt_d                TIMESTAMP,
                last_pymnt_d                TIMESTAMP
);
""")


credit_history_table_create = ("""
CREATE TABLE IF NOT EXISTS credit_history (
                borrower_id                             INT NOT NULL,
                earliest_cr_line                        TIMESTAMP,
                last_credit_pull_d                      TIMESTAMP,
                acc_now_delinq                          INT,
                acc_open_past_24mths                    INT,
                all_util                                FLOAT,
                avg_cur_bal                             FLOAT,
                bc_open_to_buy                          FLOAT,
                bc_util                                 FLOAT,
                chargeoff_within_12_mths                FLOAT,
                collections_12_mths_ex_med              FLOAT,
                delinq_2yrs                             FLOAT,
                delinq_amnt                             FLOAT,
                il_util                                 FLOAT,
                inq_fi                                  INT,
                inq_last_12m                            INT,
                inq_last_6mths                          INT,
                max_bal_bc                              FLOAT,
                mo_sin_old_il_acct                      INT,
                mo_sin_old_rev_tl_op                    INT,
                mo_sin_rcnt_rev_tl_op                   INT,
                mo_sin_rcnt_tl                          INT, 
                mort_acc                                INT, 
                mths_since_last_delinq                  INT, 
                mths_since_last_major_derog             INT,
                mths_since_last_record                  INT, 
                mths_since_rcnt_il                      INT, 
                mths_since_recent_bc                    INT,
                mths_since_recent_bc_dlq                INT,
                mths_since_recent_inq                   INT, 
                mths_since_recent_revol_delinq          INT,
                num_accts_ever_120_pd                   INT, 
                num_actv_bc_tl                          INT, 
                num_actv_rev_tl                         INT, 
                num_bc_sats                             INT, 
                num_bc_tl                               INT, 
                num_il_tl                               INT, 
                num_op_rev_tl                           INT, 
                num_rev_accts                           INT, 
                num_rev_tl_bal_gt_0                     INT, 
                num_sats                                INT, 
                num_tl_120dpd_2m                        INT, 
                num_tl_30dpd                            INT, 
                num_tl_90g_dpd_24m                      INT,
                num_tl_op_past_12m                      INT, 
                open_acc                                INT, 
                open_acc_6m                             INT, 
                open_il_12m                             INT, 
                open_il_24m                             INT, 
                open_act_il                             INT, 
                open_rv_12m                             INT, 
                open_rv_24m                             INT, 
                pct_tl_nvr_dlq                          INT, 
                percent_bc_gt_75                        FLOAT, 
                policy_code                             INT, 
                pub_rec                                 INT,   
                pub_rec_bankruptcies                    INT,
                revol_bal                               FLOAT, 
                revol_util                              FLOAT, 
                tax_liens                               FLOAT, 
                tot_coll_amt                            FLOAT, 
                tot_cur_bal                             FLOAT, 
                tot_hi_cred_lim                         FLOAT,
                total_acc                               FLOAT, 
                total_bal_ex_mort                       FLOAT, 
                total_bal_il                            FLOAT, 
                total_bc_limit                          FLOAT, 
                total_cu_tl                             FLOAT, 
                total_il_high_credit_limit              FLOAT, 
                sec_app_open_act_il                     INT, 
                total_rev_hi_lim                        FLOAT, 
                revol_bal_joint                         FLOAT, 
                sec_app_earliest_cr_line                TIMESTAMP, 
                sec_app_inq_last_6mths                  INT, 
                sec_app_mort_acc                        FLOAT, 
                sec_app_open_acc                        INT,
                sec_app_revol_util                      FLOAT,
                sec_app_num_rev_accts                   INT, 
                sec_app_chargeoff_within_12_mths        FLOAT,
                sec_app_collections_12_mths_ex_med      FLOAT, 
                sec_app_mths_since_last_major_derog     FLOAT
      );
""")


bad_debt_settlement_table_create = ("""
CREATE TABLE IF NOT EXISTS bad_debt_settlement (
                loan_id                     INT,
                debt_settlement_flag_date   TIMESTAMP,
                settlement_status           VARCHAR,
                settlement_date             TIMESTAMP,
                settlement_amount           FLOAT,
                settlement_percentage       FLOAT,
                settlement_term             INT
  );
""")

hardship_table_create = ("""
CREATE TABLE IF NOT EXISTS hardshift (
               loan_id                                      INT,
               hardship_type                                VARCHAR,
               hardship_reason                              VARCHAR,
               hardship_status                              VARCHAR,
               deferral_term                                INT,
               hardship_amount                              FLOAT,
               hardship_start_date                          TIMESTAMP,
               hardship_end_date                            TIMESTAMP,
               payment_plan_start_date                      TIMESTAMP,
               hardship_length                              INT,
               hardship_dpd                                 INT,
               hardship_loan_status                         VARCHAR,
               orig_projected_additional_accrued_interest   FLOAT,
               hardship_payoff_balance_amount               FLOAT,
               hardship_last_payment_amount                 FLOAT
  );
""")

state_demo_table_create = ("""
CREATE TABLE IF NOT EXISTS state_demo (
              state_code                VARCHAR NOT NULL,
              state_name                VARCHAR,
              median_age                FLOAT,
              avg_house_size            FLOAT,
              pop_total                 INT,
              foreign_born              INT,
              no_vetagans               INT,
              pop_male                  INT,
              pop_female                INT,
              pop_american_natives      INT,
              pop_asian                 INT,
              pop_black                 INT,
              pop_hispanic              INT,
              pop_white                 INT
   );
""")

# INSERT TABLES
loans_table_insert = ("""
    COPY loans 
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET;
""").format(LOANS_BUCKET, ARN)


borrowers_table_insert = ("""
    COPY borrowers 
    FROM '{}'
    iam_role '{}'
    format as PARQUET;
""").format(BORROWERS_BUCKET, ARN)


payment_table_insert = ("""
    COPY payment 
    FROM '{}'
    iam_role '{}'
    format as PARQUET;
""").format(PAYMENT_BUCKET, ARN)


credit_history_table_insert = ("""
    COPY credit_history 
    FROM '{}'
    iam_role '{}'
    format as PARQUET;
""").format(CREDIT_HIST_BUCKET, ARN)



bad_debt_settlement_table_insert = ("""
    COPY bad_debt_settlement 
    FROM '{}'
    iam_role '{}'
    format as PARQUET;;
""").format(BAD_DEBT_BUCKET, ARN)

hardship_table_insert = ("""
    COPY hardship 
    FROM '{}'
    iam_role '{}'
    format as PARQUET;
""").format(HARDSHIP_BUCKET, ARN)

state_demo_table_insert = ("""
    COPY state_demo 
    FROM '{}'
    iam_role '{}'
    format as csv;
""").format(STATE_DEMO_BUCKET, ARN)

# QUERY LISTS

create_table_queries = [loans_table_create, borrowers_table_create, payment_table_create, credit_history_table_create, 
                        bad_debt_settlement_table_create, hardship_table_create, state_demo_table_create]

drop_table_queries = [loans_table_drop, borrowers_table_drop, payment_table_drop, credit_history_table_drop, 
                      bad_debt_settlement_table_drop, hardship_table_drop, state_demo_table_drop]

insert_table_queries = [loans_table_insert, borrowers_table_insert, payment_table_insert, credit_history_table_insert, 
                        bad_debt_settlement_table_insert, hardship_table_insert]
