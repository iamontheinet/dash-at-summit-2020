from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import sum, col
import streamlit as st
import json

st.set_page_config(
     page_title="Environment Data Atlas",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

# Create Session object
def create_session_object():
    if "snowpark_session" not in st.session_state:
        connection_parameters = json.load(open('../connection.json'))
        connection_parameters['database'] = 'ENVIRONMENT_DATA_ATLAS'
        connection_parameters['schema'] = 'ENVIRONMENT'
        session = Session.builder.configs(connection_parameters).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']

    return session

# Add header and a subheader
st.header("Knoema: Environment Data Atlas")
st.subheader("Powered by Snowpark for Python and Snowflake Data Marketplace | Made with Streamlit")

# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
def load_data(session):
    # CO2 Emissions by Country
    snow_df_co2 = session.table("ENVIRONMENT.EDGARED2019").filter(col('Indicator Name') == 'Fossil CO2 Emissions').filter(col('Type Name') == 'All Type')
    snow_df_co2 = snow_df_co2.group_by('Location Name').agg(sum('$16').alias("Total CO2 Emissions")).filter(col('Location Name') != 'World').sort('Location Name')
    
    # Forest Occupied Land Area by Country
    snow_df_land = session.table("ENVIRONMENT.\"WBWDI2019Jan\"").filter(col('Series Name') == 'Forest area (% of land area)')
    snow_df_land = snow_df_land.group_by('Country Name').agg(sum('$61').alias("Total Share of Forest Land")).sort('Country Name')
    
    # Total Municipal Waste by Country
    snow_df_waste = session.table("ENVIRONMENT.UNENVDB2018").filter(col('Variable Name') == 'Municipal waste collected')
    snow_df_waste = snow_df_waste.group_by('Location Name').agg(sum('$12').alias("Total Municipal Waste")).sort('Location Name')
    
    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_co2  = snow_df_co2.to_pandas()
    pd_df_land = snow_df_land.to_pandas() 
    pd_df_waste = snow_df_waste.to_pandas()
    
    # Use columns to display the three dataframes side-by-side along with their headers
    col1, col2, col3 = st.columns(3)
    with st.container():
        with col1:
            st.subheader('CO2 Emissions by Country')
            st.dataframe(pd_df_co2)
        with col2:
            st.subheader('Forest Occupied Land Area by Country')
            st.dataframe(pd_df_land)
        with col3:
            st.subheader('Total Municipal Waste by Country')
            st.dataframe(pd_df_waste)
    
    # Display an interactive chart to visualize CO2 Emissions by Top N Countries
    with st.container():
        st.subheader('CO2 Emissions by Top N Countries')
        with st.expander(""):
            emissions_threshold = st.number_input(label='Emissions Threshold',min_value=5000, value=20000, step=5000)
            pd_df_co2_top_n = snow_df_co2.filter(col('Total CO2 Emissions') > emissions_threshold).to_pandas()
            st.bar_chart(data=pd_df_co2_top_n.set_index('Location Name'), width=850, height=500, use_container_width=True)

if __name__ == "__main__":
    session = create_session_object()
    load_data(session)

