import streamlit as st
import numpy as np
import pandas as pd
import pymysql
import os
import json
import requests
import pymysql
#from matplotlib import pyplot as m
import plotly.express as px
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine

#AGGREGATION TRANSACTION DATA EXTRACTION

path1="D:/youtube project/phonepe_pro/pulse/data/aggregated/transaction/country/india/state/"
agg_state_list=os.listdir(path1)
clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for state in agg_state_list:
    current_state=path1+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transacion_type'].append(Name)
              clm['Transacion_count'].append(count)
              clm['Transacion_amount'].append(amount)
              clm['State'].append(state)
              clm['Year'].append(year)
              clm['Quater'].append(int(file.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)
Agg_Trans["State"]=Agg_Trans["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Agg_Trans["State"]=Agg_Trans["State"].str.replace("-"," ")
Agg_Trans["State"]=Agg_Trans["State"].str.title()
Agg_Trans["State"]=Agg_Trans["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#AGGREAGATION USER DATA EXTRACTION

path2="D:/youtube project/phonepe_pro/pulse/data/aggregated/user/country/india/state/"
agg_user_list=os.listdir(path2)
clm1={'State':[], 'Year':[],'Quater':[],'brand':[],'Transaction_count':[],'percentage':[]}

for state in agg_user_list:
    current_state=path2+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D1=json.load(Data)
            try:
                for i in D1['data']['usersByDevice']:
                    brand=i['brand']
                    count=i['count']
                    percentage=i['percentage']
                    clm1['brand'].append(brand)
                    clm1['Transaction_count'].append(count)
                    clm1['percentage'].append(percentage)
                    clm1['State'].append(state)
                    clm1['Year'].append(year)
                    clm1['Quater'].append(int(file.strip('.json')))
            except:
                pass
            
#Succesfully created a dataframe
Agg_User=pd.DataFrame(clm1)

Agg_User["State"]=Agg_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Agg_User["State"]=Agg_User["State"].str.replace("-"," ")
Agg_User["State"]=Agg_User["State"].str.title()
Agg_User["State"]=Agg_User["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")



#MAP DATA EXTRACTION

path3="D:/youtube project/phonepe_pro/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list=os.listdir(path3)
#DATAFRAME OF MAP_TRANSACTION
clm2={'State':[], 'Year':[],'Quater':[],'district':[], 'metric_count':[], 'metric_amount':[]}
for state in map_trans_list:
    current_state=path3+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['hoverDataList']:
                Name=i['name']
                count=i['metric'][0]['count']
                amount=i['metric'][0]['amount']
                clm2['district'].append(Name)
                clm2['metric_count'].append(count)
                clm2['metric_amount'].append(amount)
                clm2['State'].append(state)
                clm2['Year'].append(year)
                clm2['Quater'].append(int(file.strip('.json')))
        
#Succesfully created a dataframe
Map_Trans=pd.DataFrame(clm2)
Map_Trans["State"]=Map_Trans["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Map_Trans["State"]=Map_Trans["State"].str.replace("-"," ")
Map_Trans["State"]=Map_Trans["State"].str.title()
Map_Trans["State"]=Map_Trans["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#MAP DATA EXCTRACTION
path4="D:/youtube project/phonepe_pro/pulse/data/map/user/hover/country/india/state/"
map_user_list=os.listdir(path4)
#DATAFRAME OF MAP_TRANSACTION
clm3={'State':[], 'Year':[],'Quater':[],'distinct':[],'registeredUsers':[],'appOpens':[]}
for state in map_user_list:
    current_state=path4+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['hoverData'].items():
                distinct=i[0]
                registeredUsers=i[1]['registeredUsers']
                appOpens=i[1]['appOpens']
                clm3['distinct'].append(distinct)
                clm3['registeredUsers'].append(registeredUsers)
                clm3['appOpens'].append(appOpens)
                clm3['State'].append(state)
                clm3['Year'].append(year)
                clm3['Quater'].append(int(file.strip('.json')))
            
        
#Succesfully created a dataframe
Map_User=pd.DataFrame(clm3)
Map_User["State"]=Map_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Map_User["State"]=Map_User["State"].str.replace("-"," ")
Map_User["State"]=Map_User["State"].str.title()
Map_User["State"]=Map_User["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#TOP DATA EXTRACTION
path5="D:/youtube project/phonepe_pro/pulse/data/top/transaction/country/india/state/"
top_trans_list=os.listdir(path5)
clm4={'State':[], 'Year':[],'Quater':[],'pincodes':[], 'top_count':[], 'top_amount':[]}
for state in top_trans_list:
    current_state=path5+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['pincodes']:
                entityname=i['entityName']
                top_count=i['metric']['count']
                top_amount=i['metric']['amount']
                clm4['pincodes'].append(entityname)
                clm4['top_count'].append(top_count)
                clm4['top_amount'].append(top_amount)
                clm4['State'].append(state)
                clm4['Year'].append(year)
                clm4['Quater'].append(int(file.strip('.json')))
            
#Succesfully created a dataframe
top_Trans=pd.DataFrame(clm4)
top_Trans["State"]=top_Trans["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_Trans["State"]=top_Trans["State"].str.replace("-"," ")
top_Trans["State"]=top_Trans["State"].str.title()
top_Trans["State"]=top_Trans["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#TOP DATA EXTRACTION

path6="D:/youtube project/phonepe_pro/pulse/data/top/user/country/india/state/"
top_user_list=os.listdir(path6)
clm5={'State':[], 'Year':[],'Quater':[],'pincodes':[], 'registeredUsers':[]}
for state in top_user_list:
    current_state=path6+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['pincodes']:
                name=i['name']
                registeredUsers=i['registeredUsers']
                clm5['pincodes'].append(name)
                clm5['registeredUsers'].append(registeredUsers)
                clm5['State'].append(state)
                clm5['Year'].append(year)
                clm5['Quater'].append(int(file.strip('.json')))
            
           
            
#Succesfully created a dataframe
top_User=pd.DataFrame(clm5)
top_User["State"]=top_User["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_User["State"]=top_User["State"].str.replace("-"," ")
top_User["State"]=top_User["State"].str.title()
top_User["State"]=top_User["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#top trans-districts
path7="D:/youtube project/phonepe_pro/pulse/data/top/transaction/country/india/state/"
top_trans_list1=os.listdir(path7)
clm6={'State':[], 'Year':[],'Quater':[],'districts':[], 'matric_count':[], 'matric_amount':[]}
for state in top_trans_list1:
    current_state=path5+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['districts']:
                entityname=i['entityName']
                top_count=i['metric']['count']
                top_amount=i['metric']['amount']
               
                clm6['districts'].append(entityname)
                clm6['matric_count'].append(top_count)
                clm6['matric_amount'].append(top_amount)
                clm6['State'].append(state)
                clm6['Year'].append(year)
                clm6['Quater'].append(int(file.strip('.json')))
            
#Succesfully created a dataframe
top_Trans_dis=pd.DataFrame(clm6)
top_Trans_dis["State"]=top_Trans_dis["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_Trans_dis["State"]=top_Trans_dis["State"].str.replace("-"," ")
top_Trans_dis["State"]=top_Trans_dis["State"].str.title()
top_Trans_dis["State"]=top_Trans_dis["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


#top user -distict
path8="D:/youtube project/phonepe_pro/pulse/data/top/user/country/india/state/"
top_user_list1=os.listdir(path8)
clm7={'State':[], 'Year':[],'Quater':[],'districts':[],'registeredUsers':[]}
for state in top_user_list1:
    current_state=path8+state+"/"
    Agg_yr=os.listdir(current_state)
    for year in Agg_yr:
        current_year=current_state+year+"/"
        Agg_yr_list=os.listdir(current_year)
        for file in Agg_yr_list:
            current_file=current_year+file
            Data=open(current_file,'r')
            D=json.load(Data)
            for i in D['data']['districts']:
                Name=i['name']
                registeredUsers=i['registeredUsers']
                clm7['districts'].append(Name)
                clm7['registeredUsers'].append(registeredUsers)
                clm7['State'].append(state)
                clm7['Year'].append(year)
                clm7['Quater'].append(int(file.strip('.json')))
            
#Succesfully created a dataframe
top_user_dis=pd.DataFrame(clm7)
top_user_dis["State"]=top_user_dis["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user_dis["State"]=top_user_dis["State"].str.replace("-"," ")
top_user_dis["State"]=top_user_dis["State"].str.title()
top_user_dis["State"]=top_user_dis["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar haveli and Daman and Diu")


myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()
#AAGREGATION TRANSACRION
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM agg_trans")
myconnection.commit()
agg_table=cursor.fetchall()

aggregation_transaction=pd.DataFrame(agg_table,columns=("State","Year","Quater","Transaction_type",
                                                         "Transaction_count","Transaction_amount"))

cursor.close()
myconnection.close()

#AAGREGATION USER
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM agg_user")
myconnection.commit()
agg_user_table=cursor.fetchall()

aggregation_user=pd.DataFrame(agg_user_table,columns=("State","Year","Quater","brands",
                                                         "Transaction_count","percentage"))

cursor.close()
myconnection.close()

#MAP TRANSACTION
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM map_trans")
myconnection.commit()
map_trans_table=cursor.fetchall()

map_transaction=pd.DataFrame(map_trans_table,columns=("State","Year","Quater","district",
                                                         "metric_count","metric_amount"))

cursor.close()
myconnection.close()


#MAP USER
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM map_user")
myconnection.commit()
map_user_table=cursor.fetchall()

map_user=pd.DataFrame(map_user_table,columns=("State","Year","Quater","district",
                                                         "registeredUsers","appOpens"))

cursor.close()
myconnection.close()


#TOP TRANSACTION
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM top_trans")
myconnection.commit()
top_trans_table=cursor.fetchall()

top_transaction=pd.DataFrame(top_trans_table,columns=("State","Year","Quater","pincodes",
                                                         "top_count","top_amount"))

cursor.close()
myconnection.close()
 



#TOP USER
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM top_user")
myconnection.commit()
top_user_table=cursor.fetchall()

top_user=pd.DataFrame(top_user_table,columns=("State","Year","Quater","pincodes",
                                                         "registeredUsers"))

cursor.close()
myconnection.close()
 


#TOP  DISTRICTS TRANSACTION
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM top_trans_dis")
myconnection.commit()
top_transdis_table=cursor.fetchall()

top_trans_dis=pd.DataFrame(top_transdis_table,columns=("State","Year","Quater","districts",
                                                         "matric_count","matric_amount"))

cursor.close()
myconnection.close()
 

#TOP USER DISTRICTS 
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

#TABLE TO DATAFRAME
cursor.execute("SELECT * FROM top_user_districts")
myconnection.commit()
top_userdis_table=cursor.fetchall()

top_userdis=pd.DataFrame(top_userdis_table,columns=("State","Year","Quater","districts",
                                                         "registeredUsers"))

cursor.close()
myconnection.close()











#STREAMLIT PART

#FUNCTION OF AGGREAGATED TRANSACTION CHART VIEW AND GRAPHICAL

def transaction_amount(df,years):
    t=df[df["Year"]== years]
    t.reset_index(drop=True,inplace=True)
    t1=t.groupby("State")[["Transaction_amount"]].sum()
    t1.reset_index(inplace=True)
    fig_amount=px.bar(t1,x="State",y="Transaction_amount",title="TRANSACTION_AMOUNT OF YEAR",width=650,height=600)
    st.plotly_chart(fig_amount)
    #graphical
    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url) 
    data1=json.loads(response.content.decode('utf-8'))  
    states_name=[] 
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
        
    states_name.sort()
    
    fig_india_1=px.fig = px.choropleth(data_frame=t1, geojson=data1, locations='State', color='Transaction_amount', featureidkey='properties.ST_NM',
                                       color_continuous_scale="rainbow",range_color=(t1["Transaction_amount"].min(),t1["Transaction_amount"].max()),
                                        hover_name="State",title="TRANSACTION_AMOUNT",fitbounds="locations",
                                        height=600,width=650)
    fig_india_1.update_geos(visible=False)
    st.plotly_chart(fig_india_1)
    return t
#quarter wise analysis
def transaction_amount_Q(df,quarter):
    t=df[df["Quater"]== quarter]
    t.reset_index(drop=True,inplace=True)
    
    t1_Q=t.groupby("State")[["Transaction_amount"]].sum()
    t1_Q.reset_index(inplace=True)
    
     
    fig_amount=px.bar(t1_Q,x="State",y="Transaction_amount",title="TRANSACTION_AMOUNT OF QUATER",width=650,height=600)
    st.plotly_chart(fig_amount)
    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url) 
    data1=json.loads(response.content.decode('utf-8'))  
    states_name=[] 
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
        
    states_name.sort()
    
    fig_india_1=px.fig = px.choropleth(data_frame=t1_Q, geojson=data1, locations='State', color='Transaction_amount', featureidkey='properties.ST_NM',
                                       color_continuous_scale="rainbow",range_color=(t1_Q["Transaction_amount"].min(),t1_Q["Transaction_amount"].max()),
                                        hover_name="State",title=f"{t['Year'].unique()} YEAR {quarter}QUATER TRANSACTION_AMOUNT",fitbounds="locations",
                                        height=600,width=650)
    fig_india_1.update_geos(visible=False)
    st.plotly_chart(fig_india_1)
    
    
      



#TRANSACTION COUNT DATA ANALYSIS YEAR AND QUARTER WISW
def transaction_count(df,years):
    t_c=df[df["Year"]== years]
    t_c.reset_index(drop=True,inplace=True)
    t1=t_c.groupby("State")[["Transaction_count"]].sum()
    t1.reset_index(inplace=True)
    fig_count=px.bar(t1,x="State",y="Transaction_count",title="TRANSACTION_COUNT OF YEAR",width=650,height=600)
    st.plotly_chart(fig_count)
    #graphical
    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url) 
    data1=json.loads(response.content.decode('utf-8'))  
    states_name=[] 
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
        
    states_name.sort()
    
    fig_india_2=px.fig = px.choropleth(data_frame=t1, geojson=data1, locations='State', color='Transaction_count', featureidkey='properties.ST_NM',
                                       color_continuous_scale="rainbow",range_color=(t1["Transaction_count"].min(),t1["Transaction_count"].max()),
                                        hover_name="State",title= "TRANSACTION_COUNT",fitbounds="locations",
                                        height=650,width=700)
    fig_india_2.update_geos(visible=False)
    st.plotly_chart(fig_india_2)
    return t_c
#quarterwise analysis
def transaction_count_Q(df,quarter):
    t_c=df[df["Quater"]== quarter]
    t_c.reset_index(drop=True,inplace=True)
    
    t1_Q=t_c.groupby("State")[["Transaction_amount"]].sum()
    t1_Q.reset_index(inplace=True)
    
     
    fig_amount=px.bar(t1_Q,x="State",y="Transaction_amount",title="TRANSACTION_COUNT OF QUARTER",width=650,height=600)
    st.plotly_chart(fig_amount)
    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url) 
    data1=json.loads(response.content.decode('utf-8'))  
    states_name=[] 
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
        
    states_name.sort()
    
    fig_india_1=px.fig = px.choropleth(data_frame=t1_Q, geojson=data1, locations='State', color='Transaction_amount', featureidkey='properties.ST_NM',
                                       color_continuous_scale="rainbow",range_color=(t1_Q["Transaction_amount"].min(),t1_Q["Transaction_amount"].max()),
                                        hover_name="State",title=f"{t_c['Year'].unique()} YEAR {quarter}QUATER TRANSACTION_AMOUNT",fitbounds="locations",
                                        height=600,width=650)
    fig_india_1.update_geos(visible=False)
    st.plotly_chart(fig_india_1)
    
    
    
    
def agg_user(df,years):
        t2=df[df["Year"]== years]
        t2.reset_index(drop=True,inplace=True)
        t3=t2.groupby("brands")[["Transaction_count"]].sum()
        t3.reset_index(inplace=True)
        fig_count=px.bar(t3,x="brands",y="Transaction_count",title="TRANSACTION_COUNT OF EACH BRAND",width=650,height=600)
        st.plotly_chart(fig_count)
def map_users(df,years):
    t=df[df["Year"]== years]
    t.reset_index(drop=True,inplace=True)
    
    t1=t.groupby("State")[["registeredUsers"]].sum()
    t1.reset_index(inplace=True)
    fig_amount=px.bar(t1,x="State",y="registeredUsers",title=f"RegisteredUsers",width=650,height=600)
    st.plotly_chart(fig_amount)
    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response=requests.get(url) 
    data1=json.loads(response.content.decode('utf-8'))  
    states_name=[] 
    for feature in data1["features"]:
        states_name.append(feature["properties"]["ST_NM"])
        
    states_name.sort()
    
    fig_india_2=px.fig = px.choropleth(data_frame=t1, geojson=data1, locations='State', color='registeredUsers', featureidkey='properties.ST_NM',
                                       color_continuous_scale="rainbow",range_color=(t1["registeredUsers"].min(),t1["registeredUsers"].max()),
                                        hover_name="State",title= "registeredUsers",fitbounds="locations",
                                        height=600,width=650)
    fig_india_2.update_geos(visible=False)
    st.plotly_chart(fig_india_2)
    
#STREAMLIT PART DESIGN
from PIL import Image
st.set_page_config(layout="wide")
st.title('PHONEPE PULSE DATA VISUALIZATION')
with st.sidebar:
     icon = Image.open(r"C:\Users\navit\OneDrive\Pictures\IMAGES\PHONE.png")
     st.image(icon,use_column_width=True)
     st.title("MAIN MENU")
#PAGE 1
def phonepe_about():
    st.markdown('## INTRODUCING OF PHONEPULSE')
    video_file_path =r"C:\Users\navit\Downloads\VID-20240226-WA0006.mp4"
    st.video(video_file_path)
    st.title("ABOUT")
    st.write('''The Indian digital payments story has truly captured the world's imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones and data,

When PhonePe started 5 years back, we were constantly looking for definitive data sources on digital payments in India. Some of the questions we were seeking answers to were - How are consumers truly using digital payments? What are the top cases? Are kiranas across Tier 2 and 3 getting a facelift with the penetration of QR codes?
This year as we became India's largest digital payments platform with 46% UPI market share, we decided to demystify the what, why and how of digital payments in India.

This year, as we crossed 2000 Cr. transactions and 30 Crore registered users, we thought as India's largest digital payments platform with 46% UPI market share, we have a ring-side view of how India sends, spends, manages and grows its money. So it was time to demystify and share the what, why and how of digital payments in India.

PhonePe Pulse is your window to the world of how India transacts with interesting trends, deep insights and in-depth analysis based on our data put together by the PhonePe team.''')

#PAGE 2    
def insights():
    #video_file_path =r"https://www.youtube.com/watch?v=Yy03rjSUIB8&list=PPSV.mp4"
    #st.video(video_file_path)
    st.markdown('# The Evolution & Future of India Digital Payments Industry')
    st.write('''Catch Sameer Nigam, Founder & CEO of PhonePe, in conversation with 
         Karthik Raghupathy, Head of Strategy & Investor Relations, 
         PhonePe as he talks about the growth & opportunities of the Indian digital payments landscape''')
    video_file_path =r"https://www.youtube.com/watch?v=Yy03rjSUIB8&list=PPSV.mp4"
    st.video(video_file_path)
    
#PAGE 3   
def data_explore():
   
    page2=st.selectbox('Select', ['Aggregation_transaction_amount', 'Aggregation_transaction_count', 'Aggregated_user','Map_user'])
    st.write('You selected:',page2)
            
    #tab1,tab2,tab3,tab4=st.tabs(["Aggregation_transaction_amount","AGGREGATED ANALYSIS","MAP ANALYSIS","TOP ANALYSIS"])
    #with tab1:
        #method=st.radio("select the method",["Aggregation_transaction_amount","Aggregation_transaction_count","Aggregated_user"])
    if page2=="Aggregation_transaction_amount":
            col1,col2=st.columns(2)
            with col1:
                years = st.slider("select the year",aggregation_transaction['Year'].min(),aggregation_transaction['Year'].max(),aggregation_transaction['Year'].min())
                tr_a=transaction_amount(aggregation_transaction,years)
           # col1,col2=st.columns(2)
            with col2:
                quarters = st.slider("select the quarter",tr_a['Quater'].min(),tr_a['Quater'].max(),tr_a['Quater'].min())
            transaction_amount_Q(tr_a,quarters)
                
                #df = pd.read_csv('D:\youtube project\State_trans.csv')
    elif page2=="Aggregation_transaction_count":
            col1,col2=st.columns(2)
            with col1:
                years = st.slider("select the year",aggregation_transaction['Year'].min(),aggregation_transaction['Year'].max(),aggregation_transaction['Year'].min())
                tr_c=transaction_count(aggregation_transaction,years)
             
            with col2:
                quarters = st.slider("select the quarter",tr_c['Quater'].min(),tr_c['Quater'].max(),tr_c['Quater'].min())
            transaction_count_Q(tr_c,quarters)
                    
    elif page2=="Aggregated_user":
         
                years = st.slider("select the year",aggregation_user['Year'].min(),aggregation_user['Year'].max(),aggregation_user['Year'].min())
                agg_user(aggregation_user,years)
    elif page2=="Map_user":
         
                years = st.slider("select the year",map_user['Year'].min(),map_user['Year'].max(),map_user['Year'].min())
                map_users(map_user,years)
        
        
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996')
cursor=myconnection.cursor()
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Navi@1996',database='phonepe')
cursor=myconnection.cursor()

def top_charts():
    
    st.title("TOP CHARTS")
    st.markdown("# select any one to viewing")
    questions = ['1.which state has highest digital transaction in india-TOP10',
                 '2.which state has lowest digital transaction in india-LEAST 10',
                 '3.What is the total transaction amount of each state',
                '4.what is the total transaction of each  brand',
                '5.what is the total user count of each districts ',
                '6.what is the maximum transaction amount of each states ',
                '7.what is the minimum transaction amount of each states',
                '8.application users']
    choice_ques = st.selectbox('Questions : Click the question that you would like to query',questions)
    
    if choice_ques == questions[0]:
        df = pd.read_sql_query('''select State as state, Year as year ,Transaction_amount as transaction_amount from agg_trans order by Transaction_amount desc limit 10''',myconnection)
        st.write(df)
        st.write("TOP 10 STATES OF HIGHEST TRANSACTION")
        figure = px.bar(df,x='transaction_amount',y='state',orientation='h',color='state')
        st.plotly_chart(figure,use_container_width = 800 ,height=1000)
        
    elif choice_ques == questions[1]:
        df = pd.read_sql_query('''select State as state ,Year as year,Transaction_amount as transaction_amount from agg_trans order by Transaction_amount limit 10''',myconnection)
        st.write(df)
        st.write("TOP 10 STATES OF LOWEST TRANSACTION")
       
        figure = px.bar(df,x='transaction_amount',y='state',orientation='h',color='state')
        st.plotly_chart(figure,use_container_width = True)
        
    elif choice_ques == questions[2]:
        df = pd.read_sql_query('''select State as state,sum(Transaction_amount) as transaction_amount from agg_trans group by State''',myconnection)
        st.write(df)
        st.write("TOTAL TRANSACTION OF EACH STATES")
        figure = px.bar(df,x='transaction_amount',y='state',orientation='h',color='state')
        st.plotly_chart(figure,use_container_width = True)
      
    elif choice_ques == questions[3]:
        df = pd.read_sql_query('''SELECT brand as Brand, SUM(Transaction_count) AS transaction_count 
FROM agg_user 
group by brand''',myconnection)
        st.write(df)
        st.write("TOTAL TRANSACTION COUNT OF EACH BRANDS")
        figure = px.bar(df,x='transaction_count',y='Brand',orientation='h',color='Brand')
        st.plotly_chart(figure,use_container_width = True)
    elif choice_ques == questions[4]:
       df = pd.read_sql_query('''select district as Districts,sum(registeredUsers) as registeredUsers from map_user group by districts''',myconnection)
       st.write(df)
       st.write("TOTAL USER COUNT OF EACH DISTRICTS")
       figure = px.bar(df,x='Districts',y='registeredUsers',orientation='h',color='Districts')
       st.plotly_chart(figure,use_container_width = True)
    elif choice_ques == questions[5]:
        df = pd.read_sql_query('''select State as state,max(Transaction_amount) as transaction_amount from agg_trans group by State''',myconnection)
        st.write(df)
        st.write("MAXIMUM TRANSACTION AMOUNT  OF EACH STATES")
        figure = px.bar(df,x='state',y='transaction_amount',orientation='h',color='state')
        st.plotly_chart(figure,use_container_width = True)
    elif choice_ques == questions[6]:
        df = pd.read_sql_query('''select State as state,min(Transaction_amount) as transaction_amount from agg_trans group by State''',myconnection)
        st.write(df)
        st.write("MINIMUM TRANSACTION AMOUNT  OF EACH STATES")
        figure = px.bar(df,x='state',y='transaction_amount',orientation='h',color='state')
        st.plotly_chart(figure,use_container_width = True)
    elif choice_ques == questions[7]:
        df = pd.read_sql_query('''select Year as year,sum(appOpens)as appopen from map_user group by Year''',myconnection)
        st.write(df)
        st.write("TRANDING OF APPLICATION USERS")
        figure = px.bar(df,x='year',y='appopen',orientation='h',color='year')
        st.plotly_chart(figure,use_container_width = True)
        
def main():
     
    page1 = st.sidebar.selectbox("",["INTRODUCING OF PHONEPE PULSE","INSIGHTS","DATA EXPLORE","TOP CHARTS"])
    if page1 == "INTRODUCING OF PHONEPE PULSE":
         phonepe_about()
    elif page1 == "INSIGHTS":
         insights()
  
    elif page1 == "DATA EXPLORE":
          data_explore()
          
    elif page1 == "TOP CHARTS":
          top_charts()
          
    

            

        
if __name__ == "__main__":
    main()
    

    
