import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
import psycopg2 
import requests
import json
import os
import mysql.connector
from sqlalchemy import create_engine
import numpy as np
import plotly.figure_factory as ff


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


## MAP USER PLOT 1
def map_user_plot_1(df, YEAR):
    map_user_y=df[df["YEARS"]==YEAR]
    map_user_y.reset_index(drop=True, inplace=True)

    map_user_y_group=map_user_y.groupby("STATES")[["REGISTEREDUSERS", "APPOPENS"]].sum()
    map_user_y_group.reset_index(inplace=True)

    fig_line_1=px.line(map_user_y_group, x="STATES", y=["REGISTEREDUSERS", "APPOPENS"],
                    title=f"{YEAR} STATES REGISTEREDUSERS APPOPENS",markers=True, width=600, height=550,
                    color_discrete_sequence= px.colors.sequential.Magma)
    st.plotly_chart(fig_line_1)

    return map_user_y

## MAP USER PLOT 2
def map_user_plot_2(df, QUARTER):
    map_user_y_Q=df[df["QUARTERS"]==QUARTER]
    map_user_y_Q.reset_index(drop=True, inplace=True)

    map_user_y_Q_group=map_user_y_Q.groupby("STATES")[["REGISTEREDUSERS", "APPOPENS"]].sum()
    map_user_y_Q_group.reset_index(inplace=True)

    fig_line_1=px.line(map_user_y_Q_group, x="STATES", y=["REGISTEREDUSERS", "APPOPENS"],
                    title=f"{df['YEARS'].min()} / {QUARTER} QUARTERS REGISTEREDUSERS APPOPENS",markers=True, width=600, height=550,
                    color_discrete_sequence=px.colors.sequential.PuBu_r)
    st.plotly_chart(fig_line_1)

    return map_user_y_Q


## MAP USER PLOT 3
def map_user_plot_3(df, STATE):
    agg_user_YQS=df[df["STATES"]== STATE]
    agg_user_YQS.reset_index(drop=True, inplace=True)

    coll1,coll2= st.columns(2)
    with coll1:
        fig_MU_pie_1 = px.pie(agg_user_YQS, values='REGISTEREDUSERS', names='DISTRICTS', title='REGISTERED USERS ANALYSIS',
                            height=400, width=500,hole= 0.1)
        st.plotly_chart(fig_MU_pie_1)

    with coll2:
        fig_MU_pie_2 = px.pie(agg_user_YQS, values='APPOPENS', names='DISTRICTS', title='APPOPENS / USERS ANALYSIS',
                            height=400, width=500,hole= 0.2)
        st.plotly_chart(fig_MU_pie_2)


## TOP INSURANCE PLOT 1
def top_insurance_plot_1(df, STATE):
    top_insur_Y=df[df["STATES"]==STATE]
    top_insur_Y.reset_index(drop=True, inplace=True)
    
    coll1,coll2= st.columns(2)
    with coll1:
        fig_top_insur_bar_1 = px.bar(top_insur_Y, x='QUARTERS', y='TRANSACTION_AMOUNT', hover_data="PINCODES", 
                                    title='TRANSACTION AMOUNT ANALYSIS',color_discrete_sequence=px.colors.sequential.PuBu_r,
                                    height=600, width=500)
        st.plotly_chart(fig_top_insur_bar_1)

    with coll2:
        fig_top_insur_bar_2 = px.bar(top_insur_Y, x='QUARTERS', y='TRANSACTION_COUNT', hover_data="PINCODES", 
                                    title='TRANSACTION COUNT ANALYSIS', color_discrete_sequence=px.colors.sequential.Sunsetdark_r,
                                    height=600, width=500)
        st.plotly_chart(fig_top_insur_bar_2)


## TOP USER PLOT 1(df, YEAR):
def top_user_plot_1(df, YEAR):
    top_user_Y=df[df["YEARS"]==YEAR]
    top_user_Y.reset_index(drop=True, inplace=True)

    top_user_Y_group=pd.DataFrame(top_user_Y.groupby(["STATES", "QUARTERS"])["REGISTEREDUSERS"].sum())
    top_user_Y_group.reset_index(inplace=True)


    fig_top_plot_1= px.bar(top_user_Y_group, x="STATES", y="REGISTEREDUSERS", color="QUARTERS", 
                        hover_name="STATES", width=600,height=500,
                        title=f"{YEAR} REGISTERED USERS",
                        color_discrete_sequence=px.colors.sequential.Sunsetdark_r,)
    st.plotly_chart(fig_top_plot_1)

    return top_user_Y

## TOP USER PLOT 2
def top_user_plot_2(df, STATE):
    top_user_Y_S=df[df["STATES"]==STATE]
    top_user_Y_S.reset_index(drop=True, inplace=True)

    fig_top_plot_2=px.bar(top_user_Y_S, x="QUARTERS", y="REGISTEREDUSERS", title= "REGISTEREDUSERS,PINCODES,QUARTERS / TOP USER",
                        width=600,height=500, color= "REGISTEREDUSERS", hover_data="PINCODES",
                        color_continuous_scale=px.colors.sequential.Sunsetdark_r)
    st.plotly_chart(fig_top_plot_2)


#Create Ques and ans / Data from postgres SQL 
# SQL connection For Question
def top_chart_transaction_amount(table_name):
    phonepe_db=psycopg2.connect(host="localhost",
                                user="postgres",
                                port=5432,
                                database="Project_Phonepe",
                                password=12345)
    cursor=phonepe_db.cursor()

    # QUS PLOT 1
    query1=f'''select states, sum(transaction_amount) as transaction_amount 
                from {table_name}
                group by states order by transaction_amount desc 
                limit 15'''

    cursor.execute(query1)
    table1=cursor.fetchall()
    phonepe_db.commit()

    df_1= pd.DataFrame(table1, columns=("states", "transaction amount"))
    coll1,coll2=st.columns(2)
    with coll1:
        fig_amount1= px.bar(df_1, x="states", y="transaction amount", title="TOP 10 TRANSACTION AMOUNT",hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Turbo,height=550,width=450)
        st.plotly_chart(fig_amount1)


    # QUS PLOT 2
    query2=f'''select states, sum(transaction_amount) as transaction_amount 
                from {table_name}
                group by states order by transaction_amount 
                limit 15'''

    cursor.execute(query2)
    table2=cursor.fetchall()
    phonepe_db.commit()

    df_2= pd.DataFrame(table2, columns=("states", "transaction amount"))
    with coll2:
        fig_amount2= px.bar(df_2, x="states", y="transaction amount", title="LAST 10 TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Turbo_r,height=650,width=450)
        st.plotly_chart(fig_amount2)

    # QUS PLOT 3
    query3=f'''select states, avg(transaction_amount) as transaction_amount 
                from {table_name}
                group by states order by transaction_amount'''

    cursor.execute(query3)
    table3=cursor.fetchall()
    phonepe_db.commit()

    df_3= pd.DataFrame(table3, columns=("states", "transaction amount"))

    fig_amount3= px.bar(df_3, x="transaction amount", y="states", title=" AVERAGE TRANSACTION AMOUNT", hover_name= "states",
                        orientation= "h",
                        color_discrete_sequence=px.colors.sequential.amp_r,height=550,width=800)
    st.plotly_chart(fig_amount3) 

# SQL connection For Question
def top_chart_transaction_count(table_name):
    phonepe_db=psycopg2.connect(host="localhost",
                                user="postgres",
                                port=5432,
                                database="Project_Phonepe",
                                password=12345)
    cursor=phonepe_db.cursor()

    # QUS PLOT 1
    query1=f'''select states, sum(transaction_count) as transaction_count 
                from {table_name}
                group by states order by transaction_count desc 
                limit 15'''

    cursor.execute(query1)
    table1=cursor.fetchall()
    phonepe_db.commit()

    df_1= pd.DataFrame(table1, columns=("states", "transaction count"))
    coll1,coll2=st.columns(2)
    with coll1:
        fig_amount1= px.bar(df_1, x="states", y="transaction count", title="TOP 10 TRANSACTION COUNT",hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Turbo,height=550,width=450)
        st.plotly_chart(fig_amount1)


    # QUS PLOT 2
    query2=f'''select states, sum(transaction_count) as transaction_count 
                from {table_name}
                group by states order by transaction_count 
                limit 15'''

    cursor.execute(query2)
    table2=cursor.fetchall()
    phonepe_db.commit()

    df_2= pd.DataFrame(table2, columns=("states", "transaction_count"))
    with coll2:
        fig_amount2= px.bar(df_2, x="states", y="transaction_count", title="TOP 10 TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Turbo_r,height=650,width=450)
        st.plotly_chart(fig_amount2)

    # QUS PLOT 3
    query3=f'''select states, avg(transaction_count) as transaction_count 
                from {table_name}
                group by states order by transaction_count'''

    cursor.execute(query3)
    table3=cursor.fetchall()
    phonepe_db.commit()

    df_3= pd.DataFrame(table3, columns=("states", "transaction_count"))

    fig_amount3= px.bar(df_3, x="transaction_count", y="states", title="AVERAGE TRANSACTION COUNT", hover_name= "states",
                        orientation= "h",
                        color_discrete_sequence=px.colors.sequential.amp_r,height=550,width=800)
    st.plotly_chart(fig_amount3)


# SQL connection For Question
def top_chart_registered_user(table_name, state):
    phonepe_db=psycopg2.connect(host="localhost",
                                user="postgres",
                                port=5432,
                                database="Project_Phonepe",
                                password=12345)
    cursor=phonepe_db.cursor()

    # QUS PLOT 1
    query1=f'''select districts,sum(registeredusers) AS registereduser
                from {table_name}
                where states = '{state}'
                Group BY districts
                order by registereduser Desc
                limit 10 '''

    cursor.execute(query1)
    table1=cursor.fetchall()
    phonepe_db.commit()

    df_1= pd.DataFrame(table1, columns=("districts", "registereduser"))

    coll1,coll2 =st.columns(2)
    with coll1:
        fig_amount1= px.bar(df_1, x="districts", y="registereduser", title="TOP 10 REGISTERED USER",hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Turbo,height=550,width=450)
        st.plotly_chart(fig_amount1)


    # QUS PLOT 2
    query2=f'''select districts,sum(registeredusers) AS registereduser
                from {table_name}
                where states = '{state}'
                Group BY districts
                order by registereduser
                limit 10'''

    cursor.execute(query2)
    table2=cursor.fetchall()
    phonepe_db.commit()

    df_2= pd.DataFrame(table2, columns=("districts", "registereduser"))

    with coll2:
        fig_amount2= px.bar(df_2, x="districts", y="registereduser", title="LAST 10 REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Turbo_r,height=550,width=450)
        st.plotly_chart(fig_amount2)

    # QUS PLOT 3
    query3=f'''select districts,avg(registeredusers) AS registereduser
                from {table_name}
                where states = '{state}'
                Group BY districts
                order by registereduser'''

    cursor.execute(query3)
    table3=cursor.fetchall()
    phonepe_db.commit()

    df_3= pd.DataFrame(table3, columns=("districts", "registereduser"))

    fig_amount3= px.bar(df_3, x="registereduser", y="districts", title="AVERAGE REGISTERED USER", hover_name= "districts",
                        orientation= "h",
                        color_discrete_sequence=px.colors.sequential.amp_r,height=550,width=800)
    st.plotly_chart(fig_amount3)


# Creating Design Streamlit Page  
        
st.set_page_config(layout= "wide")    
st.balloons()

with st.sidebar:
    
    # YouTube logo image.svg
    youtube_logo_url = "https://cdn.worldvectorlogo.com/logos/phonepe-1.svg"
    st.sidebar.image(youtube_logo_url, width=160)
    st.sidebar.header(":drak_blue[◀️DATA VISUALIZATION & EXPLORATION▶️]")   
    select=option_menu("MENU CLASSES", ["INTRO", "DATA ANALYTICS", "TOP CHART ANALYSIS", "CONTACT US"],
                       icons=['book','gear','map','phone'])
      


if select== "DATA ANALYTICS":
    tab1, tab2, tab3 = st.tabs(["📊AGGREGATED ANALYTICS", "📊MAP ANALYTICS", "📊TOP ANALYTICS"])

    with tab1:
        button_type1=st.radio("SELECT AGGRE ANALYSIS", ["INSURANCE ANALYSIS", "TRANSACTION ANALYSIS", "USER ANALYSIS"])

        if button_type1=="INSURANCE ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🟠 CHOOSE THE YEAR ",agg_insurance["YEARS"].min(),agg_insurance["YEARS"].max(),agg_insurance["YEARS"].min())
            SYQTCA_Y=TRANSACTION_COUNT_AMOUNT_Y(agg_insurance,years)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("⚫ CHOOSE THE QUARTER",SYQTCA_Y["QUARTERS"].min(),SYQTCA_Y["QUARTERS"].max(),SYQTCA_Y["QUARTERS"].min())
            TRANSACTION_AMOUNT_COUNT_Y_Q(SYQTCA_Y, QUARTER)

        elif button_type1=="TRANSACTION ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🟣 CHOOSE THE YEAR",agg_transaction["YEARS"].min(),agg_transaction["YEARS"].max(),agg_transaction["YEARS"].min())
            agg_tran_SYQTCA_Y=TRANSACTION_COUNT_AMOUNT_Y(agg_transaction,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🔵 CHOOSE THE STATE", agg_tran_SYQTCA_Y["STATES"].unique())
            agg_trans_type(agg_tran_SYQTCA_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🔴 CHOOSE THE QUARTER",agg_tran_SYQTCA_Y["QUARTERS"].min(),agg_tran_SYQTCA_Y["QUARTERS"].max(),agg_tran_SYQTCA_Y["QUARTERS"].min())
            agg_tran_SYQTCA_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(agg_tran_SYQTCA_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟤 CHOOSE THE STATE TYPE", agg_tran_SYQTCA_Y_Q["STATES"].unique())
            agg_trans_type(agg_tran_SYQTCA_Y_Q, states)
                
        elif button_type1=="USER ANALYSIS":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🟠 CHOOSE THE YEAR ",agg_user["YEARS"].min(),agg_user["YEARS"].max(),agg_user["YEARS"].min())
            agg_user_Y=agg_user_plot_1(agg_user, years)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🔵 CHOOSE THE QUARTER",agg_user_Y["QUARTERS"].min(),agg_user_Y["QUARTERS"].max(),agg_user_Y["QUARTERS"].min())
            agg_user_Y_Q= agg_user_plot_2(agg_user_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🔴 CHOOSE THE STATE", agg_user_Y_Q["STATES"].unique())
            agg_user_plot_3(agg_user_Y_Q, states)

    
    with tab2:
        button_type2=st.radio("SELECT MAP ANALYSIS ",["MAP INSURANCE", "MAP TRANSACTION", "MAP USER"])

        if button_type2=="MAP INSURANCE":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("⚫ CHOOSE THE YEAR",ma_insurance["YEARS"].min(),ma_insurance["YEARS"].max(),
                                ma_insurance["YEARS"].min(), key="year_slider")
            map_insurance_SYQ_Y=TRANSACTION_COUNT_AMOUNT_Y(ma_insurance,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟡 CHOOSE THE STATE / DISTRICT VIEW",map_insurance_SYQ_Y["STATES"].unique())
            map_insurance_SYQ_D(map_insurance_SYQ_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🟤 CHOOSE THE QUARTER_Y",map_insurance_SYQ_Y["QUARTERS"].min(),map_insurance_SYQ_Y["QUARTERS"].max(),
                                  map_insurance_SYQ_Y["QUARTERS"].min())
            map_insurance_SYQ_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(map_insurance_SYQ_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟠 CHOOSE THE STATE TYPE", map_insurance_SYQ_Y_Q["STATES"].unique())
            map_insurance_SYQ_D(map_insurance_SYQ_Y_Q, states)
                                
        
        elif button_type2=="MAP TRANSACTION":

            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🔵 CHOOSE THE YEAR",ma_transaction["YEARS"].min(),ma_transaction["YEARS"].max(),
                                ma_transaction["YEARS"].min(), key="year_slider")
            map_transaction_SYQ_Y=TRANSACTION_COUNT_AMOUNT_Y(ma_transaction,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🔴 CHOOSE THE STATE / DIST",map_transaction_SYQ_Y["STATES"].unique())
            map_insurance_SYQ_D(map_transaction_SYQ_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("⚫ CHOOSE THE QUARTER / YQ ",map_transaction_SYQ_Y["QUARTERS"].min(),map_transaction_SYQ_Y["QUARTERS"].max(),
                                  map_transaction_SYQ_Y["QUARTERS"].min())
            map_transaction_SYQ_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(map_transaction_SYQ_Y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟤 CHOOSE THE STATE TYPE", map_transaction_SYQ_Y_Q["STATES"].unique())
            map_insurance_SYQ_D(map_transaction_SYQ_Y_Q, states)


        elif button_type2=="MAP USER":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🟡 CHOOSE THE YEAR / MU",ma_user["YEARS"].min(),ma_user["YEARS"].max(),
                                ma_user["YEARS"].min(), key="year_slider")
            map_user_y=map_user_plot_1(ma_user,years)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🟠 CHOOSE THE QUARTER / MYQ ",map_user_y["QUARTERS"].min(),map_user_y["QUARTERS"].max(),
                                  map_user_y["QUARTERS"].min())
            map_user_SYQ_Y_Q= map_user_plot_2(map_user_y, QUARTER)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🔴 CHOOSE THE STATE TYPE /MU", map_user_SYQ_Y_Q["STATES"].unique())
            map_user_plot_3(map_user_SYQ_Y_Q, states)



    with tab3:
        button_type3=st.radio("SELECT TOP ANALYSIS",["TOP INSURANCE", "TOP TRANSACTION", "TOP USER"])

        if button_type3=="TOP INSURANCE":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("⚫ CHOOSE THE YEAR / TOP INSUR",to_insurance["YEARS"].min(),to_insurance["YEARS"].max(),
                                to_insurance["YEARS"].min())
            top_insurance_TRANS_Y=TRANSACTION_COUNT_AMOUNT_Y(to_insurance,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟤 CHOOSE THE STATE / TOP INSUR",top_insurance_TRANS_Y["STATES"].unique())
            top_insurance_plot_1(top_insurance_TRANS_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🔵 CHOOSE THE QUARTER / TOP Y_Q ",top_insurance_TRANS_Y["QUARTERS"].min(),top_insurance_TRANS_Y["QUARTERS"].max(),
                                  top_insurance_TRANS_Y["QUARTERS"].min())
            top_insurance_TRANS_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(top_insurance_TRANS_Y, QUARTER)


        elif button_type3=="TOP TRANSACTION":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("⚫ CHOOSE THE YEAR / TOP TRANS",to_transaction["YEARS"].min(),to_transaction["YEARS"].max(),
                                to_transaction["YEARS"].min())
            top_transaction_TRANS_Y=TRANSACTION_COUNT_AMOUNT_Y(to_transaction,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🟤 CHOOSE THE STATE / TOP TRANS",top_transaction_TRANS_Y["STATES"].unique())
            top_insurance_plot_1(top_transaction_TRANS_Y, states)

            coll1,coll2=st.columns(2)
            with coll1:
                QUARTER=st.slider("🟠 CHOOSE THE QUARTER / TOP TRANS",top_transaction_TRANS_Y["QUARTERS"].min(),top_transaction_TRANS_Y["QUARTERS"].max(),
                                  top_transaction_TRANS_Y["QUARTERS"].min())
            top_transaction_TRANS_Y_Q= TRANSACTION_AMOUNT_COUNT_Y_Q(top_transaction_TRANS_Y, QUARTER)
            
        elif button_type3=="TOP USER":
            coll1,coll2=st.columns(2)
            with coll1:
                years=st.slider("🔵 CHOOSE THE YEAR / TOP USER",to_user["YEARS"].min(),to_user["YEARS"].max(),
                                to_user["YEARS"].min())
            top_user_SRQ_Y=top_user_plot_1(to_user,years)

            coll1,coll2=st.columns(2)
            with coll1:
                states=st.selectbox("🔴 CHOOSE THE STATE / TOP USER",top_user_SRQ_Y["STATES"].unique())
            top_user_plot_2(top_user_SRQ_Y, states)
        
elif select == "TOP CHART ANALYSIS":
    QUESTION=st.selectbox("🔴 Choose The Topic analysis", ["1. Transaction Amount And Count Of Aggregated Insurance?",
                                                        "2. Transaction Amount And Count Of Map Insurance?",
                                                        "3. Transaction Amount And Count Of Aggregated Transaction?",
                                                        "4. Transaction Amount And Count Of Map Transaction?",
                                                        "5. Transaction Count Of Aggregated User?"])
    
    if QUESTION == "1. Transaction Amount And Count Of Aggregated Insurance?":
        st.subheader("▶️AGGRAGATED TRANSACTION AMOUNT")
        top_chart_transaction_amount("aggregated_insurance")
        st.subheader("▶️AGGRAGATED TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_insurance")
    
    elif QUESTION == "2. Transaction Amount And Count Of Map Insurance?":
        st.subheader("▶️MAP TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_insurance")
        st.subheader("▶️MAP TRANSACTION COUNT")
        top_chart_transaction_count("map_insurance")

    
    elif QUESTION == "3. Transaction Amount And Count Of Aggregated Transaction?":
        st.subheader("▶️AGGREGATED TRANSACTION AMOUNT")
        top_chart_transaction_amount("aggregated_transaction")
        st.subheader("▶️AGGREGATED TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_transaction")   

    elif QUESTION == "4. Transaction Amount And Count Of Map Transaction?":
        st.subheader("▶️MAP TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_transaction")
        st.subheader("▶️MAP TRANSACTION COUNT")
        top_chart_transaction_count("map_transaction")      

    elif QUESTION == "5. Transaction Count Of Aggregated User?":
        st.subheader("▶️AGGREGATED TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_user") 

    

    

    
if select=="INTRO":
    st.title("*:green ▶️PHONEPE PROJECT PRESENTATION FROM SHIBU ◀️*")


    st.markdown('''
    :coin: :violet[PhonePe] :orange[is] :green[an] :blue[Indian] :violet[multinational]
    :gray[digital] :red[payments] :green[and] :blue[financial] :violet[services]
    :gray[company] ''')


    st.markdown(''':green[**:coin:   PhonePe is a payments app that allows you to use BHIM UPI,
                                    your credit card and debit card or wallet to recharge your mobile phone,
                                    pay all your utility bills and to make instant payments at your favourite offline and online stores.**]''')
    
    st.markdown(""":blue[:coin:   You can also invest in mutual funds and buy insurance plans on PhonePe. Get Car & Bike Insurance on our app.
                                    Link your bank account transaction on PhonePe and transfer money with BHIM UPI instantly! 
                                    The PhonePe app is safe and secure, meets all your payment, investment, mutual funds,
                                    insurance and banking needs like mobile, dish, fasttag, gas, 
                                    electricity, water and entertainment recharges can be done. ]""")
    
    st.markdown("""
            
    **Type of business -**	Private                    
    **Type of site -** Digital payments & Financial services   
    **Founded -**	 2015 ( 9 years ago )  
    **Headquarters -** Salarpuria Softzone, Bangalore , Karnataka, India.  
    **Founder(s)-** Sameer Nigam , (Co-Founder & CEO) Rahul Chari                   
    **Revenue -** 	Increase ₹16.46 billion (US$206 million) (FY 2021–22)                 
    **Parent -**  Walmart                  
    **URL -**	phonepe.com                 
    **Commercial -** Yes                 
    **Registration Users -** 500 million
                                 
    **Industry -** Internet, E-commerce, Shopping, Fintech, Financial services, Mutual funds, Insurance, Digital gold, 
    Payment gateway, ONDC, Lending, Wealth Management.
                    
    **Services -** Digital payments, Mobile payments
    systems Financial services, Merchant payments, Mutual funds, Insurance, Digital gold Payment, gateway account transaction, 
    Aggregator Merchant Lending, Hyperlocal e-commerce app built on ONDC - Pincode Stock broking app and web platform & Share Market.
    
                    
    :green[*Note: Current status - Active]""")                     

if select =="CONTACT US":
    
    st.title("CONTACT US")
    
    coll1, coll2 = st.columns(2)

    with coll1: 
        st.subheader('Shibu.C')
        st.caption(":green[📱 9944266277] , *:blue[ 📧 shibu266277@gmail.com]*")

        st.caption(":red[Note: * fill all mandatory fields]")     
        Name = st.text_input("Name*")
        Mobile = st.text_input("Mobile*")
        Email = st.text_input("Email*")
        Message = st.text_area("Message (optional)")

        from streamlit_star_rating import st_star_rating
        st.caption(":violet[* Please rate you experience]")  
        st_star_rating(label = " ", maxValue = 5, defaultValue = 3, key = "rating", emoticons = True )

        if st.button("Submit"):
            st.success('''!!!Thank you for your Valuable comments & rating!!!''')       

    with coll2:        
        st.link_button("Git Hub", "https://streamlit.io/gallery")
        st.link_button("Linked in", "https://streamlit.io/gallery")
        st.link_button("Whatsapp", "https://streamlit.io/gallery")
        st.link_button("E-Mail", "https://streamlit.io/gallery")

  
    st.subheader('Phonepe Pulse Data Visualisation')

    st.markdown('''The goal of this project is to extract data from the Phonepe pulse Github repository,
                 transform and clean the data,insert it into a MySQL database, and create a live geo visualization dashboard using Streamlit and Plotly in Python.
                 The dashboard will display the data in an interactive and visually appealing manner, with atleast 10 different dropdown options for users to select differentfacts and figures to display.
                 The solution must be secure, efficient,and user-friendly,providing valuable insights and informationabout the data in the Phonepe pulse Github repository.''')

    st.slider(":red[RATING FOR MY PROJECT]", 0, 100)
    st.select_slider(":green[FEEDBACK]",["IMPORVE","SUPER","EXCELLENT"])
