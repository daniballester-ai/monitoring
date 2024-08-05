import pandas as pd
import streamlit as st
import mysql.connector
import matplotlib.pyplot as plt
import altair as alt
import joblib
from flask import Flask, request, jsonify
from datetime import datetime
from flask_ngrok import run_with_ngrok
from sklearn.preprocessing import LabelEncoder
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
import requests
import json
import time  # Importe o módulo time


# Função para atualizar o DataFrame
def update_dataframe():
    # Conecta ao banco de dados MySQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Horus@#24",
        database="monitor_copy"
    )
    mycursor = mydb.cursor()

    # Consulta a tabela transactions
    mycursor.execute("SELECT * FROM transactions")

    # Cria um DataFrame pandas a partir dos resultados da consulta
    df = pd.DataFrame(mycursor.fetchall(), columns=['id', 'time', 'Status', 'count', 'Alert', 'final_decision'])

    # Converte a coluna 'time' para o tipo datetime
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%H:%M')

    # Remove a coluna id
    df = df.drop('id', axis=1)

    # Cria uma coluna 'Normal' com ícones de círculo verde ou vermelho
    df['Normal'] = df['Alert'].apply(lambda x: '🟢' if x == 1 else '🔴')

    # Fecha a conexão com o banco de dados
    mycursor.close()
    mydb.close()

    return df

df = update_dataframe()


# Definindo uma regra para identificar transações problemáticas
def detect_issues(row):
    if row['status'] in ['denied', 'failed', 'reversed']:
        return 1
    return 0

# --- função para enviar email -----------------------------------
def send_alert_email(to_email, transaction_details):
    # Configurações do servidor SMTP
    smtp_server = "smtp.gmail.com"  # Altere conforme necessário
    smtp_port = 587  # Para TLS
    smtp_user = "danielle.ballester.896@ufrn.edu.br"  # Altere para seu e-mail
    smtp_password = "Horus@#24"  # Altere para sua senha

    # Criação da mensagem
    message = EmailMessage()
    message.set_content(f"Alerta de Transação:\n\n{transaction_details}")
    message["Subject"] = "Alerta de Anomalia em Transação"
    message["From"] = smtp_user
    message["To"] = to_email

    # Envio do e-mail
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Inicializa a conexão TLS
            server.login(smtp_user, smtp_password)
            server.send_message(message)
            print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

# --- início da leitura de novos dados ------------------------------------------------

# ---- recebendo da api ------
# Função para obter dados da API
def get_data_from_api():
    api_url = "http://localhost:5000/api/get_record"  # URL da sua API Flask
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Lança um erro para códigos de status HTTP não 200
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erro ao se conectar à API: {e}")
        return None

# Obter dados da API
data = get_data_from_api()

# Verificar se os dados foram obtidos com sucesso
if data:
    # Extrair e exibir variáveis individuais
    time = data.get('time')
    status = data.get('status')
    count = data.get('count')
    

status_copy = status

joblib_file = "isolation_forest_model.joblib"
model = joblib.load(joblib_file)

# Codificar variáveis categóricas
le = LabelEncoder()

# Transformar os dados em um DataFrame
df5 = pd.DataFrame({'time': [time], 'status': [status], 'count': [count]})

# Ajustar o formato do tempo para lidar com 'h'
df5['hour'] = pd.to_datetime(df5['time'].str.replace('h ', ':'), format='%H:%M').dt.hour
df5['minute'] = pd.to_datetime(df5['time'].str.replace('h ', ':'), format='%H:%M').dt.minute

# Now you can drop the original 'time' column if you don't need it
df5 = df5.drop('time', axis=1)

# Aplicar a regra ao DataFrame
df5['Issue'] = df5.apply(detect_issues, axis=1)

# Codificar a coluna 'status'
df5['status'] = le.fit_transform(df5['status'])

# Prever anomalias
df5["Alert"] = pd.Series(model.predict(df5))
df5["Alert"] = df5["Alert"].map({1: 0, -1: 1})

# Combinar os resultados das regras e do modelo
df5['final_decision'] = df5.apply(lambda row: 'Alert' if row['Issue'] == 1 or row['Alert'] == 1 else 'Approve', axis=1)

# Get current year, month, and day
current_year = datetime.now().year
current_month = datetime.now().month
current_day = datetime.now().day

# Assuming df5 has 'hour' and 'minute' columns
current_year = datetime.now().year
current_month = datetime.now().month
current_day = datetime.now().day

df5['datetime'] = df5.apply(lambda row: datetime(current_year, current_month, current_day, row['hour'], row['minute'], 0).strftime('%Y-%m-%d %H:%M:%S'), axis=1)

# Conecta ao banco de dados MySQL
mydb = mysql.connector.connect(
host="localhost",
user="root",
password="Horus@#24",
database="monitor_copy"
)

# SQL query to get the last inserted ID
sql = "SELECT LAST_INSERT_ID()"

mycursor = mydb.cursor()
mycursor.execute(sql)
last_id = mycursor.fetchone()[0]


# Insert data into the database
for index, row in df5.iterrows():
    sql = "INSERT INTO monitor_copy.transactions (id, time, status, count, Alert, final_decision) VALUES (%s, %s, %s, %s, %s, %s)"
    values = (last_id, row['datetime'], status_copy, row['count'], row['Issue'], row['final_decision'])
    mycursor.execute(sql, values)

    # Envio de alerta por email
    if row['final_decision'] == 'Alert' :
        transaction_details = df5.iloc[index].to_dict()
        send_alert_email("danielleballester@gmail.com", transaction_details)

# Commit the changes
mydb.commit()   



# --- Início da aplicação Streamlit ------------------------------------------------------

st.markdown("## 🚨 POS Monitoring System")

st.divider()


# S I D E   B A R -----------------------------------------------------------------------
# Adiciona uma imagem ao sidebar
st.sidebar.image('cloud.jfif', width=120) 

# Cria filtros no sidebar
#st.sidebar.header('Controls')

st.sidebar.divider()



hour_filter = st.sidebar.slider('Hour', int(df['time'].str[:2].min()), int(df['time'].str[:2].max()), (int(df['time'].str[:2].min()), int(df['time'].str[:2].max())))
minute_filter = st.sidebar.slider('Minute', int(df['time'].str[3:5].min()), int(df['time'].str[3:5].max()), (int(df['time'].str[3:5].min()), int(df['time'].str[3:5].max())))

#Filtro para Normal (usando single select)
normal_filter = st.sidebar.selectbox('Normal / Anormal', ['All', '🟢', '🔴'])

# Filtro para Status
status_filter = st.sidebar.selectbox('Status', ['All Status'] + list(df['Status'].unique()))

# Filtra o DataFrame de acordo com os filtros selecionados
filtered_df = df[
    (df['time'].str[:2].astype(int).between(*hour_filter)) &
    (df['time'].str[3:5].astype(int).between(*minute_filter))
]

if normal_filter != 'All':
    filtered_df = filtered_df[filtered_df['Normal'] == normal_filter]

if status_filter != 'All Status':
    filtered_df = filtered_df[filtered_df['Status'] == status_filter]    

# Exibe o total de registros no sidebar
st.sidebar.write(f'Total Data: {len(filtered_df)}')


# --- Cria o gráfico de barras empilhadas 100% ---

# Filtra o DataFrame para incluir apenas os status desejados
filtered_df = filtered_df[filtered_df['Status'].isin(['denied', 'failed', 'reversed'])]

# Agrupa os dados por Status e Normal (sem agrupar por 'time')
df_grouped_stacked = filtered_df.groupby(['Status', 'Normal'])['count'].sum().reset_index()

# Adiciona a coluna 'Alert'
df_grouped_stacked['Alert'] = df_grouped_stacked['Normal'].apply(lambda x: -1 if x == '🔴' else 1)

# Agrupa por Status e Alert
df_grouped_stacked = df_grouped_stacked.groupby(['Status', 'Alert'])['count'].sum().reset_index()

# Calcula a porcentagem para cada status
df_grouped_stacked['percentage'] = df_grouped_stacked.groupby('Status')['count'].transform(lambda x: x / x.sum())

# Cria o gráfico de barras empilhadas 100%
chart_stacked = alt.Chart(df_grouped_stacked).mark_bar().encode(
    x='Status:N',
    y='percentage:Q',
    color='Alert:N',  # Usando 'Alert' para a cor
    tooltip=['Status:N', 'Alert:N', 'count:Q']
).properties(
    width=300,
    height=400
)

# Adiciona texto com os totais no gráfico
text_layer = chart_stacked.mark_text(
    align='center',
    baseline='bottom',
    dx=0,
    dy=-10,
    color='white'
).encode(
    x='Status:N',
    y=alt.Y('percentage:Q', stack='zero'),
    text=alt.Text('count:Q', format=',.0f'),
)

# Combina o gráfico de barras com o texto
chart_stacked = chart_stacked + text_layer

# --- Gera o gráfico com Altair ---
# Filtra o DataFrame para incluir apenas os status desejados
filtered_df = filtered_df[filtered_df['Status'].isin(['denied', 'failed', 'reversed'])]

# Conta os registros por Status e Normal
df_grouped = filtered_df.groupby(['Status', 'Normal']).size().reset_index(name='count')

# Calcula o total por Status
df_totals = df_grouped.groupby('Status')['count'].sum().reset_index(name='total_count')

# Junta os totais com os dados agrupados
df_grouped = pd.merge(df_grouped, df_totals, on='Status')

# Cria o gráfico de barras empilhadas
stacked_bar_chart = alt.Chart(df_grouped).mark_bar().encode(
    x=alt.X('Status:N', title='Status'),
    y=alt.Y('count:Q', stack='zero', title='Quantity'),
    color=alt.Color('Normal:N', legend=alt.Legend(title="Normal/Anormal")),
    tooltip=['Status:N', 'Normal:N', 'count:Q', 'total_count:Q']
).properties(
    width=600,
    height=400
)

# Adiciona texto com os totais no gráfico
text_layer = stacked_bar_chart.mark_text(
    align='center',
    baseline='bottom',
    dx=0,
    dy=-10,
    color='white'
).encode(
    x='Status:N',
    y=alt.Y('count:Q', stack='zero'),
    text=alt.Text('total_count:Q', format=',.0f'),
)

# Combina o gráfico de barras com o texto
chart = stacked_bar_chart + text_layer


# Adiciona texto com os totais no gráfico
text_layer = stacked_bar_chart.mark_text(
    align='center',
    baseline='bottom',
    dx=0,
    dy=-10,
    color='white'
).encode(
    x='Status:N',
    y=alt.Y('count:Q', stack='zero'),
    text=alt.Text('total_count:Q', format=',.0f'),
)

# Combina o gráfico de barras com o texto
chart = stacked_bar_chart + text_layer




# Duas colunas no painel -------------
col1, col2 = st.columns([1, 2])  # Cria duas colunas

with col1:  # Exibe a tabela na primeira coluna
    # Exibe a tabela abaixo do gráfico
    st.dataframe(filtered_df[['time', 'Status', 'count', 'Normal']])
    
with col2:  # A segunda coluna fica vazia por enquanto
    st.altair_chart(chart, use_container_width=True)   
    
    


# --- Gera o gráfico de linhas com Altair ---
# Filtra o DataFrame para incluir apenas os status desejados
filtered_df = filtered_df[filtered_df['Status'].isin(['denied', 'failed', 'reversed'])]

# Converte a coluna 'time' para o tipo datetime
filtered_df['time'] = pd.to_datetime(filtered_df['time'], format='%H:%M')

# Agrupa os dados por hora e Status
df_grouped = filtered_df.groupby([pd.Grouper(key='time', freq='T'), 'Status', 'Normal'])['count'].sum().reset_index()

# Converte a coluna 'time' para string no formato 'HH:MM'
df_grouped['time_str'] = df_grouped['time'].dt.strftime('%H:%M')

# Cria a base do gráfico
base = alt.Chart(df_grouped).mark_line().encode(
    x='time:T',
    y='count:Q',
    color='Status:N',
    tooltip=['Status:N', 'Normal:N', alt.Tooltip('time_str:N', title='Time'), 'count:Q']
).properties(width=600, height=400)

# Ajusta a escala do eixo X
if (hour_filter[1] - hour_filter[0]) == 0 and (minute_filter[1] - minute_filter[0]) <= 1:
    # Exibe minutos no eixo X
    chart = base
else:
    # Exibe somente as horas inteiras no eixo X
    chart = base.encode(
        alt.X('time:T', axis=alt.Axis(format='%H:%M', tickCount=10)),
    )

# --- Layout terceira coluna ---
# Adicionando conteúdo à coluna da segunda linha
col3 = st.columns(1)
with col3[0]:
    st.altair_chart(chart.interactive(), use_container_width=True)


# Fecha a conexão com o banco de dados
mydb.close()

    
    #st.rerun()





