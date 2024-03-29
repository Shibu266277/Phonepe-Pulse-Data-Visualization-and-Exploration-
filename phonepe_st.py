import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
import psycopg2 
import requests
import json


# All DataFrame Creating

# Below Create SQL Connections
phonepe_db=psycopg2.connect(host="localhost",
                            user="postgres",
                            port=5432,
                            database="Project_Phonepe",
                            password=12345)
cursor=phonepe_db.cursor()

# Aggregated Insurance Data Farme
cursor.execute("select * FROM aggregated_insurance")
phonepe_db.commit()
table_df1=cursor.fetchall()

agg_insurance=pd.DataFrame(table_df1, columns=("STATES","YEARS","QUARTERS","TRANSACTION_TYPES",
                                                "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Aggregated Transaction Data Farme
cursor.execute("select * FROM aggregated_transaction")
phonepe_db.commit()
table_df2=cursor.fetchall()

agg_transaction=pd.DataFrame(table_df2, columns=("STATES","YEARS","QUARTERS","TRANSACTION_TYPES",
                                                    "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Aggregated User Data Farme
cursor.execute("select * FROM aggregated_user")
phonepe_db.commit()
table_df3=cursor.fetchall()

agg_user=pd.DataFrame(table_df3, columns=("STATES","YEARS","QUARTERS","BRANDS",
                                            "TRANSACTION_COUNT","PERCENTAGES"))


# Map Insurance Data Farme
cursor.execute("select * FROM map_insurance")
phonepe_db.commit()
table_df4=cursor.fetchall()

ma_insurance=pd.DataFrame(table_df4, columns=("STATES","YEARS","QUARTERS","DISTRICTS",
                                                "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Map Transaction Data Farme
cursor.execute("select * FROM map_transaction")
phonepe_db.commit()
table_df5=cursor.fetchall()

ma_transaction=pd.DataFrame(table_df5, columns=("STATES","YEARS","QUARTERS","DISTRICTS",
                                                "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Map User Data Farme
cursor.execute("select * FROM map_user")
phonepe_db.commit()
table_df6=cursor.fetchall()

ma_user=pd.DataFrame(table_df6, columns=("STATES","YEARS","QUARTERS","DISTRICTS",
                                            "REGISTEREDUSERS","APPOPENS"))


# Top Insurance Data Farme
cursor.execute("select * FROM top_insurance")
phonepe_db.commit()
table_df7=cursor.fetchall()

to_insurance=pd.DataFrame(table_df7, columns=("STATES","YEARS","QUARTERS","PINCODES",
                                              "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Top Transaction Data Farme
cursor.execute("select * FROM top_transaction")
phonepe_db.commit()
table_df8=cursor.fetchall()

to_transaction=pd.DataFrame(table_df8, columns=("STATES","YEARS","QUARTERS","PINCODES",
                                              "TRANSACTION_COUNT","TRANSACTION_AMOUNT"))


# Top User Data Farme
cursor.execute("select * FROM top_user")
phonepe_db.commit()
table_df9=cursor.fetchall()

to_user=pd.DataFrame(table_df9, columns=("STATES","YEARS","QUARTERS",
                                         "PINCODES","REGISTEREDUSERS"))


# DataFrame Call Function
# Transaction Amount and Count Code With Chart View
def TRANSACTION_COUNT_AMOUNT_Y(df,YEAR):
    SYQTCA=df[df["YEARS"] == YEAR]
    SYQTCA.reset_index(drop=True, inplace= True)

    SYQTCA_group=SYQTCA.groupby("STATES")[["TRANSACTION_COUNT","TRANSACTION_AMOUNT"]].sum()
    SYQTCA_group.reset_index(inplace= True)

    coll1,coll2=st.columns(2)
    with coll1:
        fig_amount= px.bar(SYQTCA_group, x="STATES", y="TRANSACTION_AMOUNT", title=f"{YEAR}TRANSACTION AMOUNT CHART",
                        color_discrete_sequence=px.colors.sequential.Turbo, height=600,width=600)
        st.plotly_chart(fig_amount)

    with coll2:
        fig_count= px.bar(SYQTCA_group, x="STATES", y="TRANSACTION_COUNT", title=f"{YEAR}TRANSACTION COUNT CHART",
                        color_discrete_sequence=px.colors.sequential.amp_r, height=600,width=600)
        st.plotly_chart(fig_count)

# Transaction Amount,Count & States Show With Map View
    coll1,coll2=st.columns(2)
    with coll1:  
        map_url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(map_url)
        data1=json.loads(response.content)
        states_name=[]
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()

        fig_map_1= px.choropleth(SYQTCA_group,geojson=data1,featureidkey="properties.ST_NM",
                                locations="STATES",color="TRANSACTION_AMOUNT",color_continuous_scale="Turbo",
                                range_color=(SYQTCA_group["TRANSACTION_AMOUNT"].min(),SYQTCA_group["TRANSACTION_AMOUNT"].max()),
                                hover_name="STATES", title= f"{YEAR} TRANSACTION AMOUNT", fitbounds="locations",
                                height=600,width=550)
        fig_map_1.update_geos(visible=False)
        st.plotly_chart(fig_map_1)

    with coll2:
        fig_map_2= px.choropleth(SYQTCA_group,geojson=data1,featureidkey="properties.ST_NM",
                                locations="STATES",color="TRANSACTION_COUNT",color_continuous_scale="Rainbow",
                                range_color=(SYQTCA_group["TRANSACTION_COUNT"].min(),SYQTCA_group["TRANSACTION_COUNT"].max()),
                                hover_name="STATES", title= f"{YEAR} TRANSACTION COUNT", fitbounds="locations",
                                height=600,width=550)
        fig_map_2.update_geos(visible=False)
        st.plotly_chart(fig_map_2)

    return SYQTCA

#Quarter Analaysis View With Bar Chart 
def TRANSACTION_AMOUNT_COUNT_Y_Q(df,QUARTER):
    SYQTCA=df[df["QUARTERS"] == QUARTER]
    SYQTCA.reset_index(drop=True, inplace= True)
    
    SYQTCA_group=SYQTCA.groupby("STATES")[["TRANSACTION_COUNT","TRANSACTION_AMOUNT"]].sum()
    SYQTCA_group.reset_index(inplace= True)

    coll1,coll2=st.columns(2)
    with coll1:
        fig_amount= px.bar(SYQTCA_group, x="STATES", y="TRANSACTION_AMOUNT", title=f"{SYQTCA['YEARS'].max()} YEAR {QUARTER} QUARTERS TRANSACTION AMOUNT CHART",
                        color_discrete_sequence=px.colors.sequential.PuBu_r,height=500,width=400)
        st.plotly_chart(fig_amount)

    with coll2:
        fig_count= px.bar(SYQTCA_group, x="STATES", y="TRANSACTION_COUNT", title=f"{SYQTCA['YEARS'].max()} YEAR {QUARTER} QUARTERS TRANSACTION COUNT CHART",
                        color_discrete_sequence=px.colors.sequential.Teal_r,height=500,width=400) 
        st.plotly_chart(fig_count)

    coll1,coll2=st.columns(2)
    with coll1:
        map_url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(map_url)
        data1=json.loads(response.content)
        states_name=[]
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()

        fig_map_1= px.choropleth(SYQTCA_group,geojson=data1,featureidkey="properties.ST_NM",
                        locations="STATES",color="TRANSACTION_AMOUNT",color_continuous_scale="Rainbow",
                        range_color=(SYQTCA_group["TRANSACTION_AMOUNT"].min(),SYQTCA_group["TRANSACTION_AMOUNT"].max()),
                        hover_name="STATES", title= f"{SYQTCA['YEARS'].max()} YEAR {QUARTER} QUARTERS TRANSACTION AMOUNT", fitbounds="locations",
                            height=400, width=400)
        fig_map_1.update_geos(visible=False)
        st.plotly_chart(fig_map_1)
    with coll2:
        fig_map_2= px.choropleth(SYQTCA_group,geojson=data1,featureidkey="properties.ST_NM",
                        locations="STATES",color="TRANSACTION_COUNT",color_continuous_scale="Rainbow",
                        range_color=(SYQTCA_group["TRANSACTION_COUNT"].min(),SYQTCA_group["TRANSACTION_COUNT"].max()),
                        hover_name="STATES", title= f"{SYQTCA['YEARS'].max()} YEAR {QUARTER} QUARTERS TRANSACTION COUNT", fitbounds="locations",
                            height=400, width=400)
        fig_map_2.update_geos(visible=False)
        st.plotly_chart(fig_map_2)

    return SYQTCA


def agg_trans_type(df, STATE):

    SYQTCA=agg_tran_SYQTCA_Y[agg_tran_SYQTCA_Y["STATES"] == STATE]
    SYQTCA.reset_index(drop=True, inplace= True)
    
    SYQTCA_group=SYQTCA.groupby("TRANSACTION_TYPES")[["TRANSACTION_COUNT","TRANSACTION_AMOUNT"]].sum()
    SYQTCA_group.reset_index(inplace= True)

    coll1,coll2=st.columns(2)
    with coll1:
        fig_pie_1= px.pie(data_frame=SYQTCA_group, names= "TRANSACTION_TYPES", values= "TRANSACTION_AMOUNT", width=400,
                            title=f"{STATE.upper()} TRANSACTION AMOUNT", hole= 0.2)
        st.plotly_chart(fig_pie_1)
    with coll2:
        fig_pie_2= px.pie(data_frame=SYQTCA_group, names= "TRANSACTION_TYPES", values= "TRANSACTION_COUNT", width=400,
                            title=f"{STATE.upper()} TRANSACTION COUNT", hole= 0.4)
        st.plotly_chart(fig_pie_2)


##AGGREGATED USER ANALYSIS CODE BELOW
def agg_user_plot_1(df, YEAR):
    agg_user_SYQBTP=df[df["YEARS"]==YEAR]
    agg_user_SYQBTP.reset_index(drop=True, inplace=True)

    agg_user_SYQBTP_group=pd.DataFrame(agg_user_SYQBTP.groupby("BRANDS")["TRANSACTION_COUNT"].sum())
    agg_user_SYQBTP_group.reset_index(inplace=True)

    fig_bar_1=px.bar(agg_user_SYQBTP_group, x="BRANDS", y="TRANSACTION_COUNT", title=f"{YEAR} TRANSACTION COUNT BRANDS",
                    width=500, color_discrete_sequence= px.colors.sequential.BuGn_r, hover_name= "BRANDS")
    st.plotly_chart(fig_bar_1)

    return agg_user_SYQBTP

## ##AGGREGATED USER ANALYSIS QUARTER CODE BELOW
def agg_user_plot_2(df, QUARTER):
    agg_user_SYQBTP_Q=df[df["QUARTERS"]== QUARTER]
    agg_user_SYQBTP_Q.reset_index(drop=True, inplace=True)

    agg_user_SYQBTP_Q_group=pd.DataFrame(agg_user_SYQBTP_Q.groupby("BRANDS")["TRANSACTION_COUNT"].sum())
    agg_user_SYQBTP_Q_group.reset_index(inplace=True)

    fig_bar_1=px.bar(agg_user_SYQBTP_Q_group, x="BRANDS", y="TRANSACTION_COUNT", title=f"{QUARTER}TRANSACTION COUNT BRANDS QUARTER",
                        width=500, color_discrete_sequence= px.colors.sequential.Sunsetdark_r, hover_data="BRANDS")
    st.plotly_chart(fig_bar_1)

    return agg_user_SYQBTP_Q

## ## ## AGGREGATED USER ANALYSIS WITH LINE CHART CODE BELOW
def agg_user_plot_3(df, state):
    agg_user_Y_Q_S=df[df["STATES"]== state]
    agg_user_Y_Q_S.reset_index(drop=True,inplace=True)

    fig_line_1= px.line(agg_user_Y_Q_S, x="BRANDS", y="TRANSACTION_COUNT", hover_data="PERCENTAGES",
                    title=f"{state.upper()}  BRANDS TRANSACTION COUNT PERCENTAGE",markers=True, width=600)
    st.plotly_chart(fig_line_1)


## MAP INSURANCE ANALYSIS CODE BELOW
#  TRANSACTION_AMOUNT AND DISTRICTS Chart View 
def map_insurance_SYQ_D(df, STATE):

    SYQTCA=df[df["STATES"] == STATE]
    SYQTCA.reset_index(drop=True, inplace= True)
    
    SYQTCA_group=SYQTCA.groupby("DISTRICTS")[["TRANSACTION_COUNT","TRANSACTION_AMOUNT"]].sum()
    SYQTCA_group.reset_index(inplace= True)

    coll1,coll2=st.columns(2)
    with coll1:
        fig_bar_1= px.bar(SYQTCA_group, x="TRANSACTION_AMOUNT", y="DISTRICTS", height=500, width=500,
                            title=f"{STATE.upper()} DISTRICTS TRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Sunsetdark_r)
        st.plotly_chart(fig_bar_1)

    with coll2:
        fig_bar_2= px.bar(SYQTCA_group, x="TRANSACTION_COUNT", y= "DISTRICTS", height=500, width=500,
                            title=f"{STATE.upper()} DISTRICTS TRANSACTION COUNT",
                            color_discrete_sequence= px.colors.sequential.Darkmint_r)
        st.plotly_chart(fig_bar_2)



# Creating Design Streamlit Page  

st.set_page_config(layout= "wide")
st.title("PHONEPE DATA ANALYSIS")
st.balloons()

with st.sidebar:
    select=option_menu("MENU CLASSES", ["ANALYSISTIC DATA", "TOP CHART ANALYSIS"])
    #select = st.selectbox("", ["TITLES", "ANALYTIC DATA", "TOP CHART ANALYSIS"])
    
if select== "TITLE":
    pass

elif select== "ANALYSISTIC DATA":
    tab1, tab2, tab3 = st.tabs(["AGGREGATED ANALYSISTIC", "MAP ANALYSISTIC", "TOP ANALYSISTIC"])

    with tab1:
        button_type1=st.radio("SELECT AGGRE ANALYSIS", ["INSURANCE ANALYSIS", "TRANSACTION ANALYSIS", "USER ANALYSIS"])

        if button_type1=="INSURANCE ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("CHOOSE THE YEAR",agg_insurance["YEARS"].min(),agg_insurance["YEARS"].max(),agg_insurance["YEARS"].min())
            SYQTCA_Y=TRANSACTION_COUNT_AMOUNT_Y(agg_insurance,years)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("CHOOSE THE QUARTER",SYQTCA_Y["QUARTERS"].min(),SYQTCA_Y["QUARTERS"].max(),SYQTCA_Y["QUARTERS"].min())
            TRANSACTION_AMOUNT_COUNT_Y_Q(SYQTCA_Y, QUARTER)

        elif button_type1=="TRANSACTION ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("CHOOSE THE YEAR",agg_transaction["YEARS"].min(),agg_transaction["YEARS"].max(),agg_transaction["YEARS"].min())
            agg_tran_SYQTCA_Y=TRANSACTION_COUNT_AMOUNT_Y(agg_transaction,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("CHOOSE THE STATE", agg_tran_SYQTCA_Y["STATES"].unique())
            agg_trans_type(agg_tran_SYQTCA_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("CHOOSE THE QUARTER",agg_tran_SYQTCA_Y["QUARTERS"].min(),agg_tran_SYQTCA_Y["QUARTERS"].max(),agg_tran_SYQTCA_Y["QUARTERS"].min())
            agg_tran_SYQTCA_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(agg_tran_SYQTCA_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("CHOOSE THE STATE TYPE", agg_tran_SYQTCA_Y_Q["STATES"].unique())
            agg_trans_type(agg_tran_SYQTCA_Y_Q, states)
                
        elif button_type1=="USER ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("CHOOSE THE YEAR ",agg_user["YEARS"].min(),agg_user["YEARS"].max(),agg_user["YEARS"].min())
            agg_user_Y=agg_user_plot_1(agg_user, years)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("CHOOSE THE QUARTER",agg_user_Y["QUARTERS"].min(),agg_user_Y["QUARTERS"].max(),agg_user_Y["QUARTERS"].min())
            agg_user_Y_Q= agg_user_plot_2(agg_user_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("CHOOSE THE STATE", agg_user_Y_Q["STATES"].unique())
            agg_user_plot_3(agg_user_Y_Q, states)

    
    with tab2:
        button_type2=st.radio("SELECT MAP ANALYSIS ",["MAP INSURANCE", "MAP TRANSACTION", "MAP USER"])

        if button_type2=="MAP INSURANCE":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("CHOOSE THE YEAR",ma_insurance["YEARS"].min(),ma_insurance["YEARS"].max(),
                                ma_insurance["YEARS"].min(), key="year_slider")
            map_insurance_SYQ_Y=TRANSACTION_COUNT_AMOUNT_Y(ma_insurance,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("CHOOSE THE STATE",map_insurance_SYQ_Y["STATES"].unique())
            map_insurance_SYQ_D(map_insurance_SYQ_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("CHOOSE THE QUARTER",map_insurance_SYQ_Y["QUARTERS"].min(),map_insurance_SYQ_Y["QUARTERS"].max(),map_insurance_SYQ_Y["QUARTERS"].min())
            map_insurance_SYQ_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(map_insurance_SYQ_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("CHOOSE THE STATE TYPE", map_insurance_SYQ_Y_Q["STATES"].unique())
            map_insurance_SYQ_D(map_insurance_SYQ_Y_Q, states)
                                
        
        elif button_type2=="MAP TRANSACTION":
            
            pass
        elif button_type2=="MAP USER":
            pass


    with tab3:
        button_type3=st.radio("SELECT TOP ANALYSIS",["TOP INSURANCE", "TOP TRANSACTION", "TOP USER"])

        if button_type3=="TOP INSURANCE":
            pass
        elif button_type3=="TOP TRANSACTION":
            pass
        elif button_type3=="TOP USER":
            pass
elif select == "TOP CHART ANALYSIS":
    pass

