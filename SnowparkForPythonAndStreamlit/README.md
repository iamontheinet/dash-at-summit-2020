# Snowpark For Python -- Advertising Spend and ROI Prediction

This is the same demo that was presented during the [Snowflake Summit Opening Keynote](https://events.snowflake.com/summit/agenda/session/849836) and adapted for the demo in the keynote at Snowflake Data Cloud World Tour (DCWT)

## Prereq

* Access to Snowflake Enterprise account

## Usage

### **Step 1** -- Setup Tables, Users, Roles, and Tag-based Masking Policy

For the DCWT, we will need two users. One user with ACCOUNTADMIN role and another user with a role of a "Data Scientist", for example, DASH_DS. If you use a different role name, update code and scripts that follow accordingly.

 As ACCOUNTADMIN, create the following objects:

 ```sql
 USE ROLE ACCOUNTADMIN;
 ```

* Create table BUDGET_ALLOCATIONS_AND_ROI that holds the last six months of budget allocations and ROI

  ```sql
  CREATE or REPLACE TABLE BUDGET_ALLOCATIONS_AND_ROI (
    MONTH varchar(30),
    SEARCHENGINE integer,
    SOCIALMEDIA integer,
    VIDEO integer,
    EMAIL integer,
    ROI float
  );

  INSERT INTO BUDGET_ALLOCATIONS_AND_ROI (MONTH, SEARCHENGINE, SOCIALMEDIA, VIDEO, EMAIL, ROI)
  VALUES
  ('January',35,50,35,85,8.22),
  ('February',75,50,35,85,13.90),
  ('March',15,50,35,15,7.34),
  ('April',25,80,40,90,13.23),
  ('May',95,95,10,95,6.246),
  ('June',35,50,35,85,8.22);
  ```

* Create table CLICK_DATA from raw clickstream data hosted on publicly accessible S3 bucket

  ```sql
  CREATE or REPLACE file format jsonformat
    type = 'JSON'
    strip_outer_array = true;

  CREATE or REPLACE stage json_data_stage
    file_format = jsonformat
    url = 's3://sfquickstarts/Summit 2022 Keynote Demo/click_data/';

  CREATE or REPLACE TABLE CLICK_DATA_JSON (
    click_data VARIANT
  );

  COPY into CLICK_DATA_JSON
    from @json_data_stage
    on_error = 'skip_file';

  CREATE or REPLACE TABLE CLICK_DATA (
    AD_ID varchar(30),
    CHANNEL varchar(30),
    CLICK integer,
    COST FLOAT,
    IPADDRESS varchar(30),
    MACADDRESS varchar(30),
    TIMESTAMP integer  
  ) 
  AS SELECT CLICK_DATA:ad_id,CLICK_DATA:channel,CLICK_DATA:click,CLICK_DATA:cost,CLICK_DATA:ipaddress,CLICK_DATA:macaddress,CLICK_DATA:timestamp from CLICK_DATA_JSON;
  ```

* Create table CAMPAIGN_SPEND from data hosted on publicly accessible S3 bucket

  ```sql
  CREATE or REPLACE file format csvformat
    skip_header = 1
    type = 'CSV';

  CREATE or REPLACE stage campaign_data_stage
    file_format = csvformat
    url = 's3://sfquickstarts/Summit 2022 Keynote Demo/campaign_spend/';

  CREATE or REPLACE TABLE CAMPAIGN_SPEND (
    CAMPAIGN VARCHAR(60), 
    CHANNEL VARCHAR(60),
    DATE DATE,
    TOTAL_CLICKS NUMBER(38,0),
    TOTAL_COST NUMBER(38,0),
    ADS_SERVED NUMBER(38,0)
  );

  COPY into CAMPAIGN_SPEND
    from @campaign_data_stage;
  ```

* Create table MONTHLY_REVENUE from data hosted on publicly accessible S3 bucket

  ```sql
  CREATE or REPLACE stage monthly_revenue_data_stage
    file_format = csvformat
    url = 's3://sfquickstarts/Summit 2022 Keynote Demo/monthly_revenue/';

  CREATE or REPLACE TABLE MONTHLY_REVENUE (
    YEAR NUMBER(38,0),
    MONTH NUMBER(38,0),
    REVENUE FLOAT
  );

  COPY into MONTHLY_REVENUE
    from @monthly_revenue_data_stage;
  ```

* Create stages required for Stored Procedures, UDFs, and saving model files

  ```sql
  CREATE OR REPLACE STAGE dash_sprocs;
  CREATE OR REPLACE STAGE dash_models;
  CREATE OR REPLACE STAGE dash_udfs;
  ```

* Create new "Data Science" role DASH_DS and grant previliges

  ```sql
  CREATE OR REPLACE ROLE DASH_DS;

  GRANT ALL on WAREHOUSE <WAREHOUDE_NAME> to ROLE DASH_DS;
  GRANT ALL on DATABASE <DB_NAME> to ROLE DASH_DS;
  GRANT ALL on SCHEMA <SCHEMA_NAME> to ROLE DASH_DS;

  GRANT ALL on TABLE CLICK_DATA to ROLE DASH_DS;
  GRANT ALL on TABLE BUDGET_ALLOCATIONS_AND_ROI to ROLE DASH_DS;
  GRANT ALL on TABLE CAMPAIGN_SPEND to ROLE DASH_DS;
  GRANT ALL on TABLE MONTHLY_REVENUE to ROLE DASH_DS;

  GRANT ALL on STAGE dash_models to ROLE DASH_DS;
  GRANT ALL on STAGE dash_udfs to ROLE DASH_DS;
  GRANT ALL on STAGE dash_sprocs to ROLE DASH_DS;
  ```

* Create Tag-based Masking Policy for table CLICK_DATA

  ```sql
  CREATE OR REPLACE tag clickstream;

  CREATE OR REPLACE masking policy MASK_ADDRESS as (val string) returns string ->
    case
      when current_role() IN ('ACCOUNTADMIN') then val
      else '***MASKED***'
    end;
    
  ALTER tag clickstream set
    masking policy MASK_ADDRESS;
    
  ALTER TABLE CLICK_DATA set tag clickstream = 'tag-based policies';

  SELECT * from table (information_schema.policy_references(
    ref_entity_domain => 'TABLE',
    ref_entity_name => 'CLICK_DATA' )
  );

  -- NOTE: To test the above masking policy, run the follwing queries. When using ACCOUNTADMIN role you should see plain-text values for all the columns. When using DASH_DS role you should see "***MASKED***" values for AD_ID, CHANNEL, IPADDRESS, and MACADDRESS columns.
  USE ROLE ACCOUNTADMIN;
  SELECT * from CLICK_DATA limit 10;

  USE ROLE DASH_DS;
  SELECT * from CLICK_DATA limit 10;
  ```

* Create new user and assign role DASH_DS. You will use this user to connect to Snowflake from the Notebook. ***NOTE***: It might be better to create this user from Snowsight via Admin GUI.

  ```sql
  USE ROLE ACCOUNTADMIN;

  CREATE OR REPLACE user <USER_NAME>

  GRANT ROLE DASH_DS to USER <USER_NAME>;
  ```

### **Step 2** -- Create Conda Environment

* `pip install conda`

  * ***NOTE***: The other option is to use [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
  
* `conda create --name snowpark -c https://repo.anaconda.com/pkgs/snowflake python=3.8`

* `conda activate snowpark`

### **Step 3** -- Install Snowpark for Python and other libraries

* `pip install "snowflake-snowpark-python[pandas]"`

* `pip install ipykernel`

* `pip install scikit-learn`

### **Step 4** -- Update [connection.json](https://github.com/iamontheinet/dash-at-summit-2022/blob/main/SnowparkForPythonAndStreamlit/connection.json) with your Snowflake account details and "Data Science" role credentials

### **Step 5** -- Run through the [Jupyter notebook](https://github.com/iamontheinet/dash-at-summit-2022/blob/main/SnowparkForPythonAndStreamlit/Snowpark_For_Python.ipynb)

The notebook...

* Performs Exploratory Data Analysis (EDA)
* Creates features for training a model and writes them to a Snowflake table
* Creates a Stored Proc for training a ML model and uploads the model to a stage
* Calls the Stored Proc to train the model
* Creates a User-Defined Function (UDF) that uses the model for inference on new data points passed in as parameters
  * NOTE: This UDF is then called from the Streamlit app

### **Step 6** -- Run Streamlit app

In a terminal window, run the [Streamlit app](https://github.com/iamontheinet/dash-at-summit-2022/blob/main/SnowparkForPythonAndStreamlit/Snowpark_Streamlit_Revenue_Prediction.py) by executing `streamlit run Snowpark_Streamlit_Revenue_Prediction.py`

If all goes well, you should the following app in your browser window.

https://user-images.githubusercontent.com/1723932/175127637-9149b9f3-e12a-4acd-a271-4650c47d8e34.mp4
