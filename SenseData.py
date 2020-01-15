#### 2019-12
#### Nathalia Oliveira 
#### SenseData - Gerenciar Financeiro
#### Objetivo: Gerenciar informação de inadimplência de escritórios
#### Adicionar diariamente escritórios com faturas em aberto(Inadimplentes)
#### Critérios definidos: 
####     1. Boletos vencidos há mais de 5 dias
####     2. Boletos com status diferente de "Cancelado"

#Importa bibliotecas
import pandas as pd
import pyodbc as odbc
import requests
import io
import json
import turbodbc
from datetime import datetime,timezone, date
from time import gmtime, strftime, timezone
import timestring


#Conexão Indicators   
conn_indicators = odbc.connect('DSN=DSN;UID=UID;PWD=PWD')

#Conexão  NiboDb 
conn = odbc.connect('DSN=DSN;UID=UID;PWD=PWD')


#Parâmetros do processo
description = 'SenseData Finance'
status = 'Executando'
rotina = 'Diária'
startdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
startdate = timestring.Date(startdate)
today = datetime.now().strftime('%Y-%m-%d')
today = str(today)


#Insere Processo na tabela de Log
cursor = conn_indicators.cursor()
cursor.execute('''
insert into LogPythonJobs(Description,Status,StartDate,rotina) VALUES ('{}','{}','{}','{}')
'''.format(description,status, startdate,rotina) 
) 
conn_indicators.commit()
cursor.close()

def update_LogPythonJobs(status,statusdescription):
    """A partir do Input de um status e de uma descrição de status, atualiza a tabela nibodbindicators.dbo.LogPythonJobs
       As chaves consideradas são description e startDate (já declaradas no início do script) 
    """
    startdate_str = (str(startdate))
    enddate = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    enddate = str(timestring.Date(enddate))
    cursor = conn_indicators.cursor()
    cursor.execute('''UPDATE dbo.LogPythonJobs
                      SET Status = '{}',
                      StatusDescription = '{}',     
                      EndDate = '{}'      
                      where Description = '{}'  and StartDate= '{}'
                
                    '''.format(status,statusdescription,enddate,description,startdate_str))
    conn_indicators.commit()
    cursor.close()


# Verifica se existem faturas não excluídas no SenseData
try:
    myToken = '#MWM1MmFkYTdiZGJlODgzMWMyMmQ4MzE3YWQxZGMzM2M='
    myUrl = 'http://api.senseconnect.io/v1/customer_billing/?page=1'
    head = {'Authorization': 'Bearer ' + myToken}
    response = requests.get(myUrl, headers=head)
    #print(response.text)
    CountURL  = json.loads(response.text)['status']['count']
except:
    status = 'Erro'
    statusdescription = 'Erro - Get API SenseData'
    update_LogPythonJobs(status,statusdescription)
    
msg =('Existem {0} Registros de títulos no SenseData!').format(CountURL)
print(msg)    

# Recupera Base atual dos clientes inadimplentes de acordo com os critérios definidos
try:
    query = '''
        SELECT 
             S.ScheduleId as id_legacy
            , CONVERT(DATE,MIN(S.[DueDate])) AS due_date
            , SUM(S.Value) as amount
            ,1 as id_status
            ,'' as type
            ,AB.AccountantId  AS ref_item 
            ,null as payment_date
            ,''as ref_invoice
            ,MIN(convert(DATE,S.CreateDate)) AS invoice_date
            ,AB.description as ref_doc
            ,CONVERT(DATE,GETDATE()) AS ref_date
        FROM Entities E
            INNER JOIN Schedules S ON S.EntityId = E.EntityId
            INNER JOIN AccountantBillings AB ON AB.ScheduleId = S.ScheduleId
            LEFT JOIN fact_Contracts fc ON fc.ID = AB.AccountantId
        WHERE E.OrganizationId = '2737ba98-ce27-4e9f-a026-b0c555fa74ab' -- ID Organization Nibo
            AND E.Type = 'Customer' -- Cobrança referente a clientes
            AND S.DeleteDate IS NULL -- Agendamento não foi excluído
            AND S.DueDate <= CONVERT(DATE,DATEADD(DAY,-5,GETDATE()))  -- Cobrança venceu antes de hoje
            AND S.Description LIKE '%ativa%' -- Cobrança é referente a setup/ativação
            AND S.Value <> S.PaidValue -- Valor da cobrança é diferente do valor pago
            AND AB.HasEntry = 0 -- Não consta uma entry referente ao agendamento
            AND S.Value - S.PaidValue > 0 -- Valor agendado menos o valor pago é positivo
            AND S.DueDate > '2019-08-01' -- Data de vencimento do agendamento é posterior ao início do Free
            AND AB.Status <> 'canceled' -- A cobrança agendada nao foi excluída pelo financeiro
            AND fc.CancelledDate IS NULL
        GROUP BY AB.AccountantId, fc.ContractDate, fc.CancelledDate, fc.Contracted,S.ScheduleId,AB.Description
        '''
    df_inadim = pd.read_sql(query, conn)
except:
    status = 'Erro'
    statusdescription = 'Erro - Consulta Inadimplentes'
    update_LogPythonJobs(status,statusdescription)
    
    pd.options.display.float_format = '{:.0f}'.format
    
    
 # Busca Id Sucesso
try:
        query = '''
        SELECT distinct[AccountantId]
               ,convert(int,[SuccessSystemId]) as SuccessSystemId
        FROM [dbo].[SuccessCustomer]

         '''
        df_sucesso = pd.read_sql(query, conn_indicators)
except:
        status = 'Erro'
        statusdescription = 'Erro - Consulta SuccessCustomer'
        update_LogPythonJobs(status,statusdescription)
        

 #Inclui Id do sucesso no Dataframe de Inadimplentes    
    
df_inadim_sd = pd.merge(df_inadim, df_sucesso, left_on='ref_item', right_on='AccountantId')
df_inadim_sd['ref_doc'] = df_inadim_sd['ref_doc'].replace('-','', regex=True)
df_inadim_sd.rename(columns={'SuccessSystemId': 'id_customer'}, inplace=True)
del df_inadim_sd['AccountantId']
df_final_format = df_inadim_sd[['amount', 'due_date','id_legacy','id_status','id_customer','invoice_date','payment_date','ref_doc','ref_invoice','ref_item','type']]
df_final_format_up = df_final_format
df_final_format_up = df_final_format_up.astype(str)
df_inadim_sd = df_inadim_sd.astype(str)

query = ('''
        Select RefDate, count(*) as Qtd FROM dbo.SenseDataFinance 
        where RefDate = '{}'
        group by RefDate 
        ''').format(today)
df_verif = pd.read_sql(query, conn_indicators)
cursor = conn_indicators.cursor()

if (df_verif['Qtd']).empty:
    qtd_reg_SenseDataFinance = 0
else:
    qtd_reg_SenseDataFinance = int(df_verif['Qtd'])



       
if qtd_reg_SenseDataFinance > 0:
    status = 'Erro'
    statusdescription = 'Erro - Já existem registros na tabela SenseDataFinance'
    update_LogPythonJobs(status,statusdescription)
elif qtd_reg_SenseDataFinance == 0:    
    try:
        cursor = conn_indicators.cursor()
        for index,row in df_inadim_sd.iterrows():

            cursor.execute("""INSERT INTO dbo.SenseDataFinance 
                                                                (
                                                                 [Amount]         
                                                                ,[DueDate]              
                                                                ,[IdLegacy]             
                                                                ,[IdStatus]               
                                                                ,[IdCustomer]                
                                                                ,[InvoiceDate] 
                                                                ,[PaymentDate]             
                                                                ,[RefDoc]         
                                                                ,[RefInvoice]          
                                                                ,[RefItem]         
                                                                ,[Type]        
                                                                ,[RefDate]) 
                                                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                                    row['amount']         
                                                    ,row['due_date']              
                                                    ,row['id_legacy']             
                                                    ,row['id_status']               
                                                    ,row['id_customer']                
                                                    ,row['invoice_date'] 
                                                    ,row['payment_date']             
                                                    ,row['ref_doc']         
                                                    ,row['ref_invoice']          
                                                    ,row['ref_item']         
                                                    ,row['type']        
                                                    ,row['ref_date']   

                             ) 
        print(cursor)
        conn_indicators.commit()
        cursor.close()
    except:
        status = 'Erro'
        statusdescription = 'Erro - Insert SenseDataFinance'
        update_LogPythonJobs(status,statusdescription)
    

if CountURL > 0:
    status = 'Erro'
    statusdescription = 'Erro SenseData - Existem {} Registros de faturas no SenseData'.format(CountURL)
    update_LogPythonJobs(status,statusdescription)
else:
    try:
        myToken = '#MWM1MmFkYTdiZGJlODgzMWMyMmQ4MzE3YWQxZGMzM2M='
        head = { 'Content-Type': 'application/json','Authorization': 'Bearer ' + myToken}
        sucesso = 0
        falha = 0
        for idx in df_final_format.index:
            df_tmp = df_final_format.loc[[idx]]
            out_tmp = df_tmp.to_json(orient='records',date_format='iso',force_ascii=False)[1:-1]
            out_tmp = (out_tmp.replace("T00:00:00.000Z", ''))
            out_tmp = (out_tmp.replace("T00:00:00.000Z", ''))
            out_tmp = (out_tmp.replace(""""Valor""", """"vlr"""))
            out_tmp = (out_tmp.replace("""  Entrada""", """ Entrada"""))
            out_tmp = (out_tmp.replace(""" ativação""", """ ativ"""))
            out_tmp = (out_tmp.replace(""" Ativação""", """ ativ"""))
            out_tmp = (out_tmp.replace(""" de """, """ """))
            out_tmp = (out_tmp.replace(""".0,"invoice_date""", ""","invoice_date"""))
            out_format = ("{"+ """"customer_billing":  """+ out_tmp +"}")
            #print(out_format)
            data = out_format
            response = requests.post('http://api.senseconnect.io/v1/customer_billing', data=out_format, headers=head)
            if str(response) == str("<Response [201]>"):
                print('201')
                count_end_data = df_inadim_sd['id_customer'].count()
                print(response)
                sucesso = sucesso +1
                print(sucesso)
            elif str(response) == str("<Response [200]>"):
                print('200')
                count_end_data = df_inadim_sd['id_customer'].count()
                print(response)
                sucesso = sucesso +1
                print(sucesso)
            else:
                print('erro')
                status = 'Verificar'
                statusdescription = 'Verificar - Retorno API:'+ str(response) 
                update_LogPythonJobs(status,statusdescription)
                falha = falha +1
                print(falha)
        status = 'Sucesso Post'
        statusdescription = ('{} Registros inseridos com sucesso. {} Registos não inseridos.' ).format(sucesso, falha)
        print(statusdescription)
        update_LogPythonJobs(status,statusdescription)    

    except:  
        status = 'Erro'
        statusdescription = 'Erro ao inserir registros de títulos no SenseData' 
        update_LogPythonJobs(status,statusdescription)
    
                
            
            
    
   

        