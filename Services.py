import settings
import os
import pandas as pd
import numpy as np
from datetime import datetime
import math
import Repositories as repo
import Log as logger
import sys
pd.options.mode.chained_assignment = None  # default='warn'

class Service:
    """
        Class with responsability to calculation.
        Author: 
            Daniel Ferreira Souza
            Renato Saraiva Angeline
        Updates:
            Eduardo Caversan
            Since:
                2020-12
    """
    def __init__(self):
        self.rep = repo.Repository()
        self.logger = logger.Log(self)
        self.folderName = settings.FILES_PATH
    def saveVM(self, folderName = None, workday = None):
        try:        
            if folderName != None:
                self.folderName = folderName
            #Configuracoes por Contraparte
            self.logger.printLog('Recuperando deals')
            df_cp_csa = self.rep.getDeals(workday=workday)
            if df_cp_csa.empty:
                self.logger.printLog('Arquivo de deals vazio. Abortando calculo VM...')
                return
            self.logger.printLog('Filtrando calculos VM e IV')
            df_cp_csa.where((df_cp_csa['MARGIN_TYPE']=='VM') | (df_cp_csa['MARGIN_TYPE']=='IV') , inplace = True) 
            df_cp_csa.dropna(subset=['csaId'], inplace = True)
            df_cp_csa_full = df_cp_csa
            self.logger.printLog('Extraindo informacoes CSA')
            df_cp_csa = df_cp_csa[['csaId', 'thresholdValue', 'thresholdType', 'mta', 'frequency_value', 'mtn', 'bilateral']].drop_duplicates()
            df_cp_csa['_code'] = pd.Series(df_cp_csa_full['COUNTERPART'])
            values = {'thresholdType':'vlr'}
            df_cp_csa.fillna(value=values, inplace=True)
            #df_cp_csa.to_csv(self.folderName + 'output/df_cp_csa.csv')
            self.logger.printLog('Extraindo informacoes DEALS')
            df2 = pd.DataFrame([])
            df2 = df_cp_csa_full[['csaId', 'LIVEQUANTITY','DISCOUNTEDMARKETVALUE', 'DISCOUNTEDMARKETVALUE_D1','Round', 'currency']]
            df = df2.groupby(['csaId', 'currency']).sum().reset_index()
            #df.to_csv(self.folderName + 'output/df.csv')
            self.logger.printLog('Merge das informacoes CSA - DEALS')
            merged_left = pd.merge(left=df, right=df_cp_csa, how='inner', left_on='csaId', right_on='csaId')
            self.logger.printLog('Calculando Threshold Final')
            #Calculo do threshold final
            merged_left['threshold_final'] = [merged_left['thresholdValue'].values[i] * merged_left['LIVEQUANTITY'].values[i] if 'pct' in x else merged_left['thresholdValue'].values[i] for i,x in enumerate(merged_left['thresholdType'])] 
            self.logger.printLog('Calculando Saldo VM')
            #calculo do valor a transferir
            #merged_left.loc[(merged_left['bilateral'] == 'Yes') & (merged_left['DISCOUNTEDMARKETVALUE'] < 0),'Saldo_VM'] = merged_left['DISCOUNTEDMARKETVALUE'] - merged_left['threshold_final']
            #merged_left.loc[(merged_left['bilateral'] == 'No') & (merged_left['DISCOUNTEDMARKETVALUE'] < 0),'Saldo_VM'] = 0
            #merged_left.loc[merged_left['DISCOUNTEDMARKETVALUE'] > 0,'Saldo_VM'] = merged_left['DISCOUNTEDMARKETVALUE'] - merged_left['threshold_final']
            ### Alterado conforme versão do dia 02/06/2021
            merged_left.loc[(abs(merged_left['DISCOUNTEDMARKETVALUE']) < merged_left['threshold_final']),'Saldo_VM'] = 0
            merged_left.loc[(abs(merged_left['DISCOUNTEDMARKETVALUE']) > merged_left['threshold_final']) & (merged_left['bilateral'] == 'No') & (merged_left['DISCOUNTEDMARKETVALUE'] < 0) ,'Saldo_VM'] = 0
            merged_left.loc[(merged_left['DISCOUNTEDMARKETVALUE'] < 0) & (merged_left['bilateral'] == 'Yes') ,'Saldo_VM'] = (abs(merged_left['DISCOUNTEDMARKETVALUE']) - merged_left['threshold_final']) * - 1
            merged_left.loc[(merged_left['DISCOUNTEDMARKETVALUE'] > 0) & (abs(merged_left['DISCOUNTEDMARKETVALUE']) > merged_left['threshold_final']) ,'Saldo_VM'] = abs(merged_left['DISCOUNTEDMARKETVALUE']) - merged_left['threshold_final']
            #merged_left.to_csv(self.folderName + 'output/VM.csv')
            self.df_vm_full = merged_left
            self.logger.printLog('Fim do calculo do VM')  
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no calculo do VM: {0}".format(err), "E")
            raise
    def saveIAIM(self, folderName = None, workday = None):
        try:
            if folderName != None:
                self.folderName = folderName 
            df_iaim_ = pd.DataFrame([])
            #Configuracoes por Contraparte
            self.logger.printLog('Recuperando deals')
            df_iaim_ = self.rep.getDeals(workday=workday)
            if df_iaim_.empty:
                self.logger.printLog('Arquivo de deals vazio. Abortando calculo IA...')
                return
            self.logger.printLog('Filtrando calculos IA e IV')
            df_iaim_.where((df_iaim_['MARGIN_TYPE']=='IA') | (df_iaim_['MARGIN_TYPE']=='IV') , inplace = True) 
            df_iaim_.dropna(subset=['csaId'], inplace = True)
            #df_iaim_.to_csv(self.folderName + 'output/df_iaim_.csv')
            df_iaim = df_iaim_    
            values = {'Cliente_deposita_IAIM_Type':'N/A', 'Banco_deposita_IAIM_Type':'N/A'}
            df_iaim.fillna(value=values, inplace=True)
            self.logger.printLog('Calculando IAIM Cliente do dia')
            df_iaim['Cliente_deposita_IAIM_Final_Value'] = [df_iaim['Cliente_deposita_IAIM_Value'].values[i] * df_iaim['LIVEQUANTITY'].values[i] if 'pct' in x else df_iaim['Cliente_deposita_IAIM_Value'].values[i] for i,x in enumerate(df_iaim['Cliente_deposita_IAIM_Type'].astype('str'))] 
            self.logger.printLog('Calculando IAIM Banco do dia')
            df_iaim['Banco_deposita_IAIM_Final_Value'] = [df_iaim['Banco_deposita_IAIM_Value'].values[i] * df_iaim['LIVEQUANTITY'].values[i] if 'pct' in x else df_iaim['Banco_deposita_IAIM_Value'].values[i] for i,x in enumerate(df_iaim['Banco_deposita_IAIM_Type'].astype('str'))] 

            df_iaim = df_iaim[['csaId', 'Cliente_deposita_IAIM_Type', 'Cliente_deposita_IAIM_Final_Value', 'Banco_deposita_IAIM_Type','Banco_deposita_IAIM_Final_Value', 'Banco_deposita_IAIM_D1_Value', 'Cliente_deposita_IAIM_D1_Value']]
            self.logger.printLog('Consolidando IAIM')            df_iaim = df_iaim.groupby(['csaId','Cliente_deposita_IAIM_Type', 'Banco_deposita_IAIM_Type']).sum().reset_index()

            df_iaim['dtRef'] = workday
            df_iaim['_id'] = df_iaim['dtRef'] + df_iaim['csaId']
            df_iaim.rename(columns={'Banco_deposita_IAIM_Final_Value': 'Banco_deposita_IAIM_Final_Value_iaim', 'Cliente_deposita_IAIM_Final_Value': 'Cliente_deposita_IAIM_Final_Value_iaim'}, inplace=True)
            self.logger.printLog('Calculando IAIM Banco Final')
            df_iaim['Banco_deposita_IAIM_Final_Value'] = df_iaim['Banco_deposita_IAIM_Final_Value_iaim'].fillna(0).astype('float') - df_iaim['Banco_deposita_IAIM_D1_Value'].fillna(0).astype('float')
            self.logger.printLog('Calculando IAIM Cliente Final')
            df_iaim['Cliente_deposita_IAIM_Final_Value'] = df_iaim['Cliente_deposita_IAIM_Final_Value_iaim'].fillna(0).astype('float') - df_iaim['Cliente_deposita_IAIM_D1_Value'].fillna(0).astype('float')
            self.logger.printLog('Renomeando colunas')
            df_iaim.rename(columns={'Banco_deposita_IAIM_Final_Value_iaim': 'DeliveryCounterpartyIAIMValue', 'Cliente_deposita_IAIM_Final_Value_iaim': 'DeliveryOwnerIAIMValue'}, inplace=True)
            df_iaim = df_iaim[['_id','csaId','Banco_deposita_IAIM_Final_Value', 'Cliente_deposita_IAIM_Final_Value', 'DeliveryCounterpartyIAIMValue', 'DeliveryOwnerIAIMValue', 'dtRef']]
            #df_iaim.to_csv(self.folderName + 'output/iaim.csv')
            self.df_iaim_full = df_iaim
            self.logger.printLog('Fim do calculo do IAIM') 
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no calculo do IAIM: {0}".format(err), "E")
            raise
    def saveSufficiency(self, workday):
        try:
            self.logger.printLog('Executando metodo saveSufficiency.')
            #Configuracoes por Contraparte
            self.logger.printLog('Recuperando deals')
            df_csa = self.rep.getDeals(workday=workday)
            if df_csa.empty:
                self.logger.printLog('Arquivo de deals vazio. Gerando arquivo de suficiencia vazio...')
                nome_colunas = ['CSAId', 'LinkId', 'GuaranteeType', 'CounterParty', 'CPFCNPJ', 'MTN', 'MTA', 'Round', ' Threshold Value',
                'ThresholdType', 'Currency', 'Frequency','Bilateral', 'SaldoRemanescente',
                'GrossExpousure', 'SaldoVM','ValorRequeridoCliente', 'ValorRequeridoBanco', 'DeliveryOwnerIAIMValue',
                'DeliveryCounterpartyIAIMValue','VMIAIM', 'Balance', 'ValorSuficiencia', 'EventAmount',
                'ValorRestanteDesbloqueio', 'TipoDesbloqueio', 'QuantDesbloqueio', 'ValorRestanteBloqueio', 'TipoBloqueio', 
                'QuantBloqueio', 'IDMurex', 'ContaClienteCorretora','ProdutoInstrument', 'SubprodutoInstrumentType', 
                'Banco', 'Agencia', 'Aplicacao', 'CodigoCetip', 'DataEmissaoTradeDate', 'ValorBloqueado', 
                'CotacaoAtualD1', 'QuantidadeBloqueda', 'Confirmacao', 'DataEnvio','ReturnToCounter', 'DeliveryToCounter', 'ReturnToOwner', 'DeliveryToOwner', 
                'ReturnToCounterVM', 'DeliveryToCounterVM', 'ReturnToOwnerVM', 'DeliveryToOwnerVM']
                df_empty = pd.DataFrame(columns=nome_colunas)
                df_empty.to_csv(self.folderName + 'output/collateral_concat_' + workday + '.csv', index=False)
                return            
            df_csa = df_csa[['csaId','linkId', 'name', 'taxId', 'mtn', 'mta', 'Round', 'thresholdValue', 'thresholdType','currency', 'frequency_value', 'COUNTERPART', 'bilateral','assets_product','assets_subproduct','assets_haircut', 'assets_ceilingtype', 'assets_ceilingvalue', 'assets_duedate']]
            ### Alterado conforme versão do dia 02/06/2021
            df_csa.drop_duplicates(subset ="csaId", inplace=True)
            df_vm_full = self.df_vm_full
            df_iaim_full = self.df_iaim_full
            df_vm_full.dropna(subset=['csaId'], inplace = True)
            ### Alterado conforme versão do dia 02/06/2021
            #df_vm_full = df_vm_full[['csaId', 'LIVEQUANTITY', 'DISCOUNTEDMARKETVALUE', 'DISCOUNTEDMARKETVALUE_D1', 'Saldo_VM']]
            df_vm_full = df_vm_full[['csaId', 'LIVEQUANTITY', 'DISCOUNTEDMARKETVALUE', 'Saldo_VM', 'threshold_final']]
            df_csa_vm = pd.merge(df_csa, df_vm_full, on='csaId', how='outer')
            df_csa_vm_iaim = pd.merge(df_csa_vm, df_iaim_full, on='csaId', how='outer')
            ### Alterado conforme versão do dia 02/06/2021
            #df_csa_vm_iaim['Saldo_VM'] = [df_csa_vm_iaim['Saldo_VM'].values[i] if abs(df_csa_vm_iaim['Saldo_VM'].values[i]) > df_csa_vm_iaim['mta'].values[i] else 0 for i,x in enumerate(df_csa_vm_iaim['Saldo_VM'])] 
            df_csa_vm_iaim['mta'] = df_csa_vm_iaim['mta'].fillna(0)
            df_csa_vm_iaim['Saldo_VM'] = [df_csa_vm_iaim['Saldo_VM'].values[i] if abs(df_csa_vm_iaim['Saldo_VM'].values[i]) > df_csa_vm_iaim['mta'].values[i] else 0 for i,x in enumerate(df_csa_vm_iaim['Saldo_VM'])] 
            # zerar no caso verificacao
            df_csa_vm_iaim['valor_requerido_cliente'] = df_csa_vm_iaim['Saldo_VM'].fillna(0) + df_csa_vm_iaim['Cliente_deposita_IAIM_Final_Value'].fillna(0)
            df_csa_vm_iaim['valor_requerido_banco'] = df_csa_vm_iaim['Saldo_VM'].fillna(0) + df_csa_vm_iaim['Banco_deposita_IAIM_Final_Value'].fillna(0)
            df_csa_vm_iaim['taxId'] = ('000000000000' + df_csa_vm_iaim['taxId'].astype('str')).str[-14:]
            #Configuracoes por Contraparte
            self.logger.printLog('Recuperando collaterals')
            df_collateral_blocked = self.rep.getCollaterals(workday=workday)
            #df_collateral_blocked = df_collateral_blocked[df_collateral_blocked['fl_bloqueio_tatico'] == 1]
            self.logger.printLog('Preenchendo campos nulos com 0')
            df_collateral_blocked.fillna(0, inplace=True)
            df_collateral_blocked = df_collateral_blocked[['Produto_Instrument', 'Subproduto_InstrumentType', 'ValorLiquido', 'PrincipalAtual', 'ValorLivreBloqueio', 'ValorBruto_ValorAtual', 'valor_garantia_bloqueado', 'CotacaoAtualD1', 'csaId']]
            #df_collateral_blocked.to_csv(self.folderName + 'output/df_collateral_blocked_before.csv')
            df_collateral_blocked = df_collateral_blocked.groupby(['csaId', 'Produto_Instrument', 'Subproduto_InstrumentType']).sum().reset_index()
            #df_collateral_blocked.to_csv(self.folderName + 'output/df_collateral_blocked_after.csv')

            self.logger.printLog('Atualizando valor de bloqueio BP')
            #atualiza valor bloqueio BP
            #df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['ValorLivreBloqueio'] > 0), 'valor_garantia_bloqueado'] = df_collateral_blocked['ValorLiquido'] / df_collateral_blocked['PrincipalAtual'] * df_collateral_blocked['valor_garantia_bloqueado']
            ### Alterado conforme versão do dia 02/06/2021
            #df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') , 'valor_garantia_bloqueado'] = df_collateral_blocked['ValorLiquido'] / df_collateral_blocked['PrincipalAtual'] * df_collateral_blocked['valor_garantia_bloqueado']
            ## Alterado para correção do valor Balance que é calculado pelo GBO (Principal Atual)
            df_collateral_blocked['valor_garantia_bloqueado'] = df_collateral_blocked['PrincipalAtual']
            self.logger.printLog('Tratando informacoes para Haircut')
            #haircut
            sorterProduct = df_csa['assets_product'].astype('str').str.split('|') 
            sorterSubProduct = df_csa['assets_subproduct'].astype('str').str.split('|')
            sorterHaircut = df_csa['assets_haircut'].astype('str').str.split('|')
            sorterArray = pd.DataFrame(columns=['csaId','assets_product','assets_subproduct','haircut'])
            for x, y, v, w in zip(df_csa['csaId'].to_list(), sorterProduct, sorterSubProduct, sorterHaircut):
                for z in zip(y,v,w):
                    df2 = pd.DataFrame([[x,z[0], z[1], z[2]]],columns=['csaId','assets_product','assets_subproduct','haircut'])
                    sorterArray = sorterArray.append(df2, ignore_index=True)
            #self.logger.printLog('Aplicando mascara CNPJ')
            #sorterArray['taxId'] = ('000000000000' + sorterArray['taxId'].astype('str')).str[-14:]
            #sorterArray.to_csv(self.folderName + 'output/sorterArray.csv', index=False)
            
            #df_collateral_blocked['CPF_CNPJ'] = ('000000000000' + df_collateral_blocked['CPF_CNPJ'].astype('str')).str[-14:]
            self.logger.printLog('Tratando informacoes de Produtos e SubProdutos')
            values = {'SubProduto':'n/a','Produto_Instrument':'n/a','Subproduto_InstrumentType':'n/a'}
            df_collateral_blocked.fillna(value=values, inplace=True)
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'] == 0, 'Subproduto_InstrumentType'] = 'n/a'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'] == 0, 'Produto_Instrument'] = 'n/a'
            #df_collateral_blocked['CPF_CNPJ'] = ('000000000000' + df_collateral_blocked['CPF_CNPJ'].astype('str')).str[-14:]
            #df_collateral_blocked.to_csv(self.folderName + 'output/df_collateral_blocked.csv', index=False)
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCI')), 'Produto'] = 'LCI'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCA')), 'Produto'] = 'LCA'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LF')), 'Produto'] = 'LF'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDB')), 'Produto'] = 'CDB'
            ### Inclusao de tratamento para CASH (Tecnologia)
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'CASH') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CASH')), 'Produto'] = 'CASH'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CASH'), 'SubProduto'] = 'CASH'
            ### Alterado conforme versão do dia 02/06/2021 - INICIO
            df_collateral_blocked['Subproduto_InstrumentType'] = df_collateral_blocked['Subproduto_InstrumentType'].astype('str')
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDI', na=False), 'Produto'] = 'CDI'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('TIME DEPOSIT', na=False), 'Produto'] = 'TD'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('ACOES', na=False), 'Produto'] = 'ACOES'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('TITULOS PUBLICOS', na=False), 'Produto'] = 'TITULOS PUBLICOS'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCI', na=False)), 'SubProduto'] = 'LCI'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCA', na=False)), 'SubProduto'] = 'LCA'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LF', na=False)), 'SubProduto'] = 'LF'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDB', na=False)), 'SubProduto'] = 'CDB'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDI'), 'SubProduto'] = 'CDI'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('TIME DEPOSIT', na=False), 'SubProduto'] = 'TD'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('ACOES', na=False), 'SubProduto'] = df_collateral_blocked['Subproduto_InstrumentType']
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('TITULOS PUBLICOS', na=False), 'SubProduto'] = df_collateral_blocked['Subproduto_InstrumentType']
            df_collateral_blocked['CotacaoAtualD1'] = df_collateral_blocked['CotacaoAtualD1'].astype('double')
            df_collateral_blocked['pond_garantia_bloqueado'] = df_collateral_blocked['valor_garantia_bloqueado'] * df_collateral_blocked['CotacaoAtualD1']
            #Retirado o campo quantidade_garantia_bloqueado pois o mesmo nao eh enviado nem utilizado na versao adaptada do Python
            #df_collateral_blocked = df_collateral_blocked[['CPF_CNPJ','Produto', 'SubProduto', 'valor_garantia_bloqueado', 'pond_garantia_bloqueado', 'quantidade_garantia_bloqueado']].groupby(['CPF_CNPJ','Produto', 'SubProduto'], as_index=False).sum()
            df_collateral_blocked = df_collateral_blocked[['csaId','Produto', 'SubProduto', 'valor_garantia_bloqueado', 'pond_garantia_bloqueado']].groupby(['csaId','Produto', 'SubProduto'], as_index=False).sum()
            sorterArray['haircut'] = sorterArray['haircut'].astype(float)
            sorterArray = sorterArray[['csaId','assets_product', 'assets_subproduct', 'haircut']].groupby(['csaId','assets_product', 'assets_subproduct'], as_index=False)['haircut'].agg(['sum','count'])
            sorterArray['haircut'] = sorterArray['sum'] /  sorterArray['count'] 
            sorterArray.drop(columns=['sum', 'count'], inplace=True)
            sorterArray = pd.DataFrame(sorterArray)
            self.logger.printLog('Realizando merge de informacoes CSA e Collateral')
            df_collateral_blocked_haircut = pd.merge(left=df_collateral_blocked, right=sorterArray, how='left', left_on=['csaId','Produto','SubProduto'], right_on=['csaId','assets_product', 'assets_subproduct'])
            #df_collateral_blocked_haircut.rename(columns={'taxId': 'CPF_CNPJ'}, inplace=True)
            """
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDI'), 'Produto'] = 'CDI'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('TIME DEPOSIT'), 'Produto'] = 'TD'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('ACOES'), 'Produto'] = 'ACOES'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('TITULOS PUBLICOS'), 'Produto'] = 'TITULOS PUBLICOS'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCI')), 'SubProduto'] = 'LCI'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LCA')), 'SubProduto'] = 'LCA'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('LF')), 'SubProduto'] = 'LF'
            df_collateral_blocked.loc[(df_collateral_blocked['Produto_Instrument'] == 'RENDA FIXA') & (df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDB')), 'SubProduto'] = 'CDB'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('CDI'), 'SubProduto'] = 'CDI'
            df_collateral_blocked.loc[df_collateral_blocked['Subproduto_InstrumentType'].str.contains('TIME DEPOSIT'), 'SubProduto'] = 'TD'
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('ACOES'), 'SubProduto'] = df_collateral_blocked['Subproduto_InstrumentType']
            df_collateral_blocked.loc[df_collateral_blocked['Produto_Instrument'].str.contains('TITULOS PUBLICOS'), 'SubProduto'] = df_collateral_blocked['Subproduto_InstrumentType']
            df_collateral_blocked['pond_garantia_bloqueado'] = df_collateral_blocked['valor_garantia_bloqueado'] * df_collateral_blocked['CotacaoAtualD1']
            #df_collateral_blocked.to_csv(self.folderName + 'output/df_collateral_blocked.csv', index=False)
            self.logger.printLog('Realizando merge de informacoes CSA e Collateral')
            ############# EDUARDO - INCLUIDO CAMPO SUBPRODUTO COMO CHAVE NO JOIN. ##########
            df_collateral_blocked_haircut = pd.merge(left=df_collateral_blocked, right=sorterArray, how='left', left_on=['CPF_CNPJ','Produto','SubProduto'], right_on=['taxId','assets_product','assets_subproduct'])
            df_collateral_blocked_haircut.loc[df_collateral_blocked_haircut['assets_subproduct'] == 'TITULOS PUBLICOS', 'SubProduto'] = 'TITULOS PUBLICOS'
            df_collateral_blocked_haircut.loc[df_collateral_blocked_haircut['assets_subproduct'] == 'ACOES', 'SubProduto'] = 'ACOES'
            df_collateral_blocked_haircut = df_collateral_blocked_haircut[df_collateral_blocked_haircut['assets_subproduct'] == df_collateral_blocked_haircut['SubProduto']]
            """
            ### Alterado conforme versão do dia 02/06/2021 - FIM
            self.logger.printLog('Calculando valor garantia bloqueado')
            #df_collateral_blocked_haircut['valor_garantia_bloqueado'] = df_collateral_blocked_haircut['valor_garantia_bloqueado'].astype('float').fillna(0) - (df_collateral_blocked_haircut['valor_garantia_bloqueado'].astype('float').fillna(0) * df_collateral_blocked_haircut['haircut'].astype('float').fillna(0))
            df_collateral_blocked_haircut['valor_garantia_bloqueado'] = df_collateral_blocked_haircut['valor_garantia_bloqueado'].astype('float').fillna(0)
            self.logger.printLog('Calculando valor ponderado garantia bloqueado')
            #Alterado para adaptacao GBO - Retirado cálculo do Haircut
            #df_collateral_blocked_haircut['pond_garantia_bloqueado'] = df_collateral_blocked_haircut['pond_garantia_bloqueado'].astype('float').fillna(0) - (df_collateral_blocked_haircut['pond_garantia_bloqueado'].astype('float').fillna(0) * df_collateral_blocked_haircut['haircut'].astype('float').fillna(0))
            df_collateral_blocked_haircut['pond_garantia_bloqueado'] = df_collateral_blocked_haircut['pond_garantia_bloqueado'].astype('float').fillna(0)
            df_collateral_blocked_haircut = df_collateral_blocked_haircut[['csaId', 'valor_garantia_bloqueado', 'pond_garantia_bloqueado']]
            #df_collateral_blocked_haircut['CPF_CNPJ'] = ('000000000000' + df_collateral_blocked_haircut['CPF_CNPJ'].astype('str')).str[-14:]            df_collateral_blocked_haircut = df_collateral_blocked_haircut.groupby(['csaId']).sum().reset_index()
            #df_collateral_blocked_haircut.to_csv(self.folderName + 'output/df_collateral_blocked_haircut.csv', index=False)
            #df_csa_vm_iaim.to_csv(self.folderName + 'output/df_csa_vm_iaim.csv', index=False)
            self.logger.printLog('Realizando merge de informacoes VM IAIM e Collateral Blocked Haircut')
            df_sufficiency = pd.merge(left=df_csa_vm_iaim, right=df_collateral_blocked_haircut, how='left', left_on='csaId', right_on='csaId')
            #del df_sufficiency['CPF_CNPJ']
            self.logger.printLog('Renomeando colunas')
            df_sufficiency.rename(columns={'DISCOUNTEDMARKETVALUE': 'DISCOUNTEDMARKETVALUE_vm_iaim', 'DISCOUNTEDMARKETVALUE_D1': 'DISCOUNTEDMARKETVALUE_mtms'}, inplace=True)
            self.logger.printLog('Renomeando colunas')
            df_sufficiency['valor_garantia_bloqueado'] = df_sufficiency['valor_garantia_bloqueado'].fillna(0) 
            df_sufficiency['DeliveryToCounter'] = [df_sufficiency['Banco_deposita_IAIM_Final_Value'].values[i] if df_sufficiency['Banco_deposita_IAIM_Final_Value'].values[i] > 0  else 0 for i,x in enumerate(df_sufficiency['Banco_deposita_IAIM_Final_Value'])]
            df_sufficiency['DeliveryToOwner'] = [df_sufficiency['Cliente_deposita_IAIM_Final_Value'].values[i] if df_sufficiency['Cliente_deposita_IAIM_Final_Value'].values[i] > 0  else 0 for i,x in enumerate(df_sufficiency['Cliente_deposita_IAIM_Final_Value'])]
            df_sufficiency['ReturnToOwner'] = [abs(df_sufficiency['Banco_deposita_IAIM_Final_Value'].values[i]) if df_sufficiency['Banco_deposita_IAIM_Final_Value'].values[i] < 0  else 0 for i,x in enumerate(df_sufficiency['Banco_deposita_IAIM_Final_Value'])]
            df_sufficiency['ReturnToCounter'] = [abs(df_sufficiency['Cliente_deposita_IAIM_Final_Value'].values[i]) if df_sufficiency['Cliente_deposita_IAIM_Final_Value'].values[i] < 0  else 0 for i,x in enumerate(df_sufficiency['Cliente_deposita_IAIM_Final_Value'])]
            #df_sufficiency.to_csv(self.folderName + 'output/sufficiency_' + workday + '.csv', index=False)
            ### Alterado conforme versão do dia 02/06/2021 - INICIO            
            #DeliveryCounterpartyIAIMValue
            #DeliveryOwnerIAIMValue
            df_sufficiency['valor_garantia_bloqueado_pos_ia'] = df_sufficiency['valor_garantia_bloqueado'].fillna(0) -  df_sufficiency['DeliveryOwnerIAIMValue'].fillna(0) - df_sufficiency['DeliveryCounterpartyIAIMValue'].fillna(0)            df_sufficiency['ReturnToCounterVM'] = [abs(df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] > 0) & 
                (df_sufficiency['valor_requerido_cliente'].values[i] <= 0)
            ) else 
            abs(df_sufficiency['valor_requerido_cliente'].values[i] - df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_requerido_cliente'].values[i] > 0) & 
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] > 0) & 
                (df_sufficiency['valor_requerido_cliente'].values[i] < df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i])
            ) else 0 for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])] 
            df_sufficiency['ReturnToOwnerVM'] = [abs(df_sufficiency['valor_requerido_banco'].values[i] - df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] < 0) & 
                (df_sufficiency['valor_requerido_banco'].values[i] < 0) &
                (df_sufficiency['valor_requerido_banco'].values[i] > df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i])
            ) else 
            abs(df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_requerido_banco'].values[i] >= 0) & 
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] < 0)
            ) else 0 for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]     
            df_sufficiency['DeliveryToCounterVM'] = [abs(df_sufficiency['valor_requerido_cliente'].values[i]) 
            if(
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] == 0) & 
                (df_sufficiency['valor_requerido_cliente'].values[i] < 0) 
            ) else 
            abs(df_sufficiency['valor_requerido_cliente'].values[i] - df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_requerido_cliente'].values[i] < 0) & 
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] < 0) &
                (df_sufficiency['valor_requerido_cliente'].values[i] < df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i])
            ) else abs(df_sufficiency['valor_requerido_cliente'].values[i]) if((df_sufficiency['valor_requerido_cliente'].values[i] < 0) & (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] > 0)) else 0 for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]        
            df_sufficiency['DeliveryToOwnerVM'] = [abs(df_sufficiency['valor_requerido_banco'].values[i]) 
            if(
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] < 0) & 
                (df_sufficiency['valor_requerido_banco'].values[i] > 0)
            ) else 
            abs(df_sufficiency['valor_requerido_banco'].values[i] - df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i]) 
            if(
                (df_sufficiency['valor_requerido_banco'].values[i] > 0) & 
                (df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] > 0) &
                (df_sufficiency['valor_requerido_banco'].values[i] > df_sufficiency['valor_garantia_bloqueado_pos_ia'].values[i] )
            ) else 0 for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]             
            df_sufficiency['ReturnToOwnerVM'].fillna(0)
            df_sufficiency['ReturnToOwnerVM'] = [math.ceil(df_sufficiency['ReturnToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwnerVM'])] 
            df_sufficiency['DeliveryToOwnerVM'].fillna(0)
            df_sufficiency['DeliveryToOwnerVM'] = [math.ceil(df_sufficiency['DeliveryToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwnerVM'])] 
            df_sufficiency['ReturnToCounterVM'].fillna(0)
            df_sufficiency['ReturnToCounterVM'] = [math.floor(df_sufficiency['ReturnToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounterVM'])] 
            df_sufficiency['DeliveryToCounterVM'].fillna(0)
            df_sufficiency['DeliveryToCounterVM'] = [math.floor(df_sufficiency['DeliveryToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounterVM'])] 
            df_sufficiency['ReturnToOwnerVM'] = [df_sufficiency['ReturnToOwnerVM'].values[i] if df_sufficiency['ReturnToOwnerVM'].values[i]  > df_sufficiency['mta'].values[i] else 0 for i,x in enumerate(df_sufficiency['ReturnToOwnerVM'])]         
            df_sufficiency['DeliveryToOwnerVM'] = [df_sufficiency['DeliveryToOwnerVM'].values[i] if df_sufficiency['DeliveryToOwnerVM'].values[i]  > df_sufficiency['mta'].values[i] else 0 for i,x in enumerate(df_sufficiency['DeliveryToOwnerVM'])]         
            df_sufficiency['ReturnToCounterVM'] = [df_sufficiency['ReturnToCounterVM'].values[i] if df_sufficiency['ReturnToCounterVM'].values[i]  > df_sufficiency['mta'].values[i] else 0 for i,x in enumerate(df_sufficiency['ReturnToCounterVM'])]     
            df_sufficiency['DeliveryToCounterVM'] = [df_sufficiency['DeliveryToCounterVM'].values[i] if df_sufficiency['DeliveryToCounterVM'].values[i]  > df_sufficiency['mta'].values[i] else 0 for i,x in enumerate(df_sufficiency['DeliveryToCounterVM'])]     
            df_sufficiency['ReturnToOwnerVM'] = [df_sufficiency['mtn'].values[i] if (df_sufficiency['ReturnToOwnerVM'].values[i]  > 0) & (df_sufficiency['ReturnToOwnerVM'].values[i]  < df_sufficiency['mtn'].values[i]) & (df_sufficiency['mtn'].values[i] > 0) else df_sufficiency['ReturnToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwnerVM'])]         
            df_sufficiency['DeliveryToOwnerVM'] = [df_sufficiency['mtn'].values[i] if (df_sufficiency['DeliveryToOwnerVM'].values[i]  > 0) & (df_sufficiency['DeliveryToOwnerVM'].values[i]  < df_sufficiency['mtn'].values[i]) & (df_sufficiency['mtn'].values[i] > 0) else df_sufficiency['DeliveryToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwnerVM'])]     
            df_sufficiency['ReturnToCounterVM'] = [df_sufficiency['mtn'].values[i] if (df_sufficiency['ReturnToCounterVM'].values[i]  > 0) & (df_sufficiency['ReturnToCounterVM'].values[i]  < df_sufficiency['mtn'].values[i]) & (df_sufficiency['mtn'].values[i] > 0) else df_sufficiency['ReturnToCounterVM'].values[i]  for i,x in enumerate(df_sufficiency['ReturnToCounterVM'])]     
            df_sufficiency['DeliveryToCounterVM'] = [df_sufficiency['mtn'].values[i] if (df_sufficiency['DeliveryToCounterVM'].values[i]  > 0) & (df_sufficiency['DeliveryToCounterVM'].values[i]  < df_sufficiency['mtn'].values[i]) & (df_sufficiency['mtn'].values[i] > 0) else df_sufficiency['DeliveryToCounterVM'].values[i]  for i,x in enumerate(df_sufficiency['DeliveryToCounterVM'])]     
            df_sufficiency['ReturnToOwner'] = [math.ceil(df_sufficiency['ReturnToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwner'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwner'])] 
            df_sufficiency['DeliveryToOwner'] = [math.ceil(df_sufficiency['DeliveryToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwner'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwner'])] 
            df_sufficiency['ReturnToCounter'] = [math.floor(df_sufficiency['ReturnToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounter'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounter'])] 
            df_sufficiency['DeliveryToCounter'] = [math.floor(df_sufficiency['DeliveryToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounter'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounter'])] 
            df_sufficiency['DataEnvio'] = int(datetime.now().strftime("%Y%m%d"))
            df_sufficiency['valor_suficiencia_apos_MTA_MTN'] =  df_csa_vm_iaim['Saldo_VM'].fillna(0) + df_csa_vm_iaim['Banco_deposita_IAIM_Final_Value'].fillna(0) - df_csa_vm_iaim['Cliente_deposita_IAIM_Final_Value'].fillna(0) - df_sufficiency['valor_garantia_bloqueado']
            df_sufficiency['thresholdValue'] = [df_sufficiency['thresholdValue'].values[i] if 'pct' in x else df_sufficiency['threshold_final'].values[i] for i,x in enumerate(df_sufficiency['thresholdType'])] 
            """
            df_sufficiency['ReturnToOwnerVM'] = [df_sufficiency['valor_requerido_banco'].fillna(0).values[i] - 
                                                 df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i]
                                    if  ((df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 0) & 
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] < 0) &
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] > 
                                        df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i]))               
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['ReturnToOwnerVM_'] = [df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i] 
                                    if  (
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] >= 0) &
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] < 0)
                                        )
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['ReturnToOwnerVM'] = abs(df_sufficiency['ReturnToOwnerVM']) + abs(df_sufficiency['ReturnToOwnerVM_'])

            df_sufficiency['DeliveryToOwnerVM'] = [df_sufficiency['valor_requerido_banco'].fillna(0).values[i] - 
                                                   df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i]
                                    if  ((df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] >= 0) & 
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] >= 0) &
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] > 
                                        df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i]))              
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['DeliveryToOwnerVM_'] = [df_sufficiency['valor_requerido_banco'].fillna(0).values[i] 
                                                    - df_sufficiency['Banco_deposita_IAIM_Final_Value'].fillna(0).values[i]
                                    if  (
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] >= 0) &
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] < 0)
                                        )
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['DeliveryToOwnerVM'] = abs(df_sufficiency['DeliveryToOwnerVM']) + abs(df_sufficiency['DeliveryToOwnerVM_'])         

df_sufficiency['ReturnToCounterVM'] = [df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i]
                                                   - df_sufficiency['valor_requerido_banco'].fillna(0).values[i]
                                                   + df_sufficiency['Banco_deposita_IAIM_Final_Value'].fillna(0).values[i]
                                    if  (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] >= 0) & 
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] >= 0) &
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 
                                        df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i])              
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['ReturnToCounterVM_'] = [df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i] 
                                    if  (
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 0) &
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] >= 0)
                                        )
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['ReturnToCounterVM'] = abs(df_sufficiency['ReturnToCounterVM']) + abs(df_sufficiency['ReturnToCounterVM_'])

            df_sufficiency['DeliveryToCounterVM'] = [df_sufficiency['valor_requerido_cliente'].fillna(0).values[i] - 
                                                   df_sufficiency['valor_garantia_bloqueado'].fillna(0).values[i] 
                                    if  ((df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 0) & 
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] < 0) &
                                        (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 
                                        df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i]))              
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['DeliveryToCounterVM_'] = [df_sufficiency['valor_requerido_cliente'].fillna(0).values[i] -
                                                      df_sufficiency['Cliente_deposita_IAIM_Final_Value'].fillna(0).values[i]
                                    if  (
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim'].values[i] < 0) &
                                            (df_sufficiency['DISCOUNTEDMARKETVALUE_mtms'].values[i] >= 0)
                                        )
                                    else 0
                                    for i,x in enumerate(df_sufficiency['valor_garantia_bloqueado'])]
            df_sufficiency['DeliveryToCounterVM'] = abs(df_sufficiency['DeliveryToCounterVM']) + abs(df_sufficiency['DeliveryToCounterVM_'])
            #vm
            df_sufficiency['ReturnToOwnerVM'] = [math.floor(df_sufficiency['ReturnToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwnerVM'])] 
            df_sufficiency['ReturnToOwnerVM'] = [math.ceil(df_sufficiency['ReturnToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwnerVM'])] 
            df_sufficiency['DeliveryToOwnerVM'] = [math.floor(df_sufficiency['DeliveryToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwnerVM'])] 
            df_sufficiency['DeliveryToOwnerVM'] = [math.ceil(df_sufficiency['DeliveryToOwnerVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwnerVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwnerVM'])] 
            df_sufficiency['ReturnToCounterVM'] = [math.floor(df_sufficiency['ReturnToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounterVM'])] 
            df_sufficiency['ReturnToCounterVM'] = [math.ceil(df_sufficiency['ReturnToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounterVM'])]                     df_sufficiency['DeliveryToCounterVM'] = [math.floor(df_sufficiency['DeliveryToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounterVM'])] 
            df_sufficiency['DeliveryToCounterVM'] = [math.ceil(df_sufficiency['DeliveryToCounterVM'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounterVM'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounterVM'])]     
            #iaim
            df_sufficiency['ReturnToOwner'] = [math.floor(df_sufficiency['ReturnToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwner'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwner'])] 
            df_sufficiency['ReturnToOwner'] = [math.ceil(df_sufficiency['ReturnToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToOwner'].values[i] for i,x in enumerate(df_sufficiency['ReturnToOwner'])] 

            df_sufficiency['DeliveryToOwner'] = [math.floor(df_sufficiency['DeliveryToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwner'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwner'])] 
            df_sufficiency['DeliveryToOwner'] = [math.ceil(df_sufficiency['DeliveryToOwner'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToOwner'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToOwner'])] 
            df_sufficiency['ReturnToCounter'] = [math.floor(df_sufficiency['ReturnToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounter'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounter'])] 
            df_sufficiency['ReturnToCounter'] = [math.ceil(df_sufficiency['ReturnToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['ReturnToCounter'].values[i] for i,x in enumerate(df_sufficiency['ReturnToCounter'])]         
            df_sufficiency['DeliveryToCounter'] = [math.floor(df_sufficiency['DeliveryToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounter'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounter'])] 
            df_sufficiency['DeliveryToCounter'] = [math.ceil(df_sufficiency['DeliveryToCounter'].values[i] / df_sufficiency['Round'].values[i]) * df_sufficiency['Round'].values[i] if df_sufficiency['Round'].values[i] > 0 else df_sufficiency['DeliveryToCounter'].values[i] for i,x in enumerate(df_sufficiency['DeliveryToCounter'])]     
            df_sufficiency['DataEnvio'] = int(datetime.now().strftime("%Y%m%d"))
            df_sufficiency['valor_suficiencia_apos_MTA_MTN'] =  df_csa_vm_iaim['Saldo_VM'].fillna(0) + df_csa_vm_iaim['Banco_deposita_IAIM_Final_Value'].fillna(0) - df_csa_vm_iaim['Cliente_deposita_IAIM_Final_Value'].fillna(0) - df_sufficiency['valor_garantia_bloqueado']
            """
            ###
            ### Alterado conforme versão do dia 02/06/2021 - FIM
            df_sufficiency['QuantidadeBloqueda'] = 0
            df_sufficiency['valor_suficiencia'] = 0
            df_sufficiency['quant_bloqueio'] = 0
            df_sufficiency['valor_restante_bloqueio'] = 0
            df_sufficiency['quant_desbloqueio'] = 0
            df_sufficiency['Banco'] = 0
            df_sufficiency['Confirmacao'] = 0
            df_sufficiency['ValorBloqueado'] = 0
            df_sufficiency['tipo_bloqueio'] = 0
            df_sufficiency['Cliente_deposita_IAIM_Type'] = 0
            df_sufficiency['Aplicacao'] = 0
            df_sufficiency['CotacaoAtualD1'] = 0
            df_sufficiency['DISCOUNTEDMARKETVALUE'] = df_sufficiency['DISCOUNTEDMARKETVALUE_vm_iaim']
            df_sufficiency['Produto_Instrument'] = 0
            df_sufficiency['DataEmissao_TradeDate'] = 0
            df_sufficiency['IDMurex'] = 0
            df_sufficiency['Subproduto_InstrumentType'] = 0
            df_sufficiency['Banco_deposita_IAIM_Type'] = 0
            df_sufficiency['valor_restante_desbloqueio'] = 0
            df_sufficiency['ContaClienteCorretora'] = 0
            df_sufficiency['CodigoCetip'] = 0
            df_sufficiency['Agencia'] = 0
            df_sufficiency['tipo_desbloqueio'] = 0
            df_sufficiency['valor_requerido'] = df_sufficiency['Saldo_VM']
            self.df_sufficiency = df_sufficiency
            df_sufficiency = df_sufficiency[['csaId','linkId','assets_product','name','taxId','mtn','mta','Round',
                                 'thresholdValue', 'thresholdType', 'currency', 'frequency_value','bilateral',
                                 'LIVEQUANTITY', 'DISCOUNTEDMARKETVALUE', 'Saldo_VM','valor_requerido_cliente',
                                 'valor_requerido_banco', 'DeliveryOwnerIAIMValue',
                                 'DeliveryCounterpartyIAIMValue','valor_requerido', 'valor_garantia_bloqueado', 
                                 'valor_suficiencia', 'valor_suficiencia_apos_MTA_MTN', 'valor_restante_desbloqueio', 
                                 'tipo_desbloqueio', 'quant_desbloqueio',  'valor_restante_bloqueio',
                                 'tipo_bloqueio', 'quant_bloqueio','IDMurex', 'ContaClienteCorretora', 'Produto_Instrument',
                                 'Subproduto_InstrumentType', 'Banco', 'Agencia', 'Aplicacao','CodigoCetip',  
                                 'DataEmissao_TradeDate','ValorBloqueado', 'CotacaoAtualD1','QuantidadeBloqueda',
                                 'Confirmacao', 'DataEnvio', 'ReturnToCounter', 'DeliveryToCounter', 'ReturnToOwner', 'DeliveryToOwner', 
                            'ReturnToCounterVM', 'DeliveryToCounterVM', 'ReturnToOwnerVM', 'DeliveryToOwnerVM' ]]

            df_sufficiency.rename(columns={​​
                'csaId': 'CSAId', 'linkId': 'LinkId', 'assets_product': 'GuaranteeType',
                'name': 'CounterParty', 'taxId': 'CPFCNPJ', 'mtn': 'MTN', 'mta': 'MTA', 'Round': 'Round', 'thresholdValue': ' Threshold Value',
                'thresholdType': 'ThresholdType', 'currency': 'Currency', 'frequency_value': 'Frequency'
                ,'bilateral': 'Bilateral', 'LIVEQUANTITY': 'SaldoRemanescente',
                'DISCOUNTEDMARKETVALUE': 'GrossExpousure', 'Saldo_VM': 'SaldoVM',
                'valor_requerido_cliente': 'ValorRequeridoCliente', 'valor_requerido_banco':
                'ValorRequeridoBanco', 'Cliente_deposita_IAIM_Final_Value': 'DeliveryOwnerIAIMValue',
                'Banco_deposita_IAIM_Final_Value': 'DeliveryCounterpartyIAIMValue',
                'valor_requerido': 'VMIAIM', 'valor_garantia_bloqueado': 'Balance', 
                'valor_suficiencia': 'ValorSuficiencia', 'valor_suficiencia_apos_MTA_MTN': 'EventAmount'
                ,'valor_restante_desbloqueio': 'ValorRestanteDesbloqueio', 'tipo_desbloqueio': 
                'TipoDesbloqueio', 'quant_desbloqueio': 'QuantDesbloqueio', 'valor_restante_bloqueio':
                'ValorRestanteBloqueio', 'tipo_bloqueio': 'TipoBloqueio', 'quant_bloqueio':
                'QuantBloqueio', 'IDMurex': 'IDMurex', 'ContaClienteCorretora': 'ContaClienteCorretora'
                ,'Produto_Instrument': 'ProdutoInstrument', 'Subproduto_InstrumentType':
                'SubprodutoInstrumentType', 'Banco': 'Banco', 'Agencia': 'Agencia', 'Aplicacao':
                'Aplicacao', 'CodigoCetip': 'CodigoCetip', 'DataEmissao_TradeDate': 
                'DataEmissaoTradeDate', 'ValorBloqueado': 'ValorBloqueado', 'CotacaoAtualD1':
                'CotacaoAtualD1', 'QuantidadeBloqueda': 'QuantidadeBloqueda', 'Confirmacao':
                'Confirmacao', 'DataEnvio': 'DataEnvio'
                }, inplace=True)

            self.logger.printLog('Salvando arquivo de suficiencia')
            df_sufficiency.to_csv(self.folderName + 'output/collateral_concat_' + workday + '.csv', index=False)
            self.logger.printLog('Fim do calculo do Sufficiency')
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no calculo do Sufficiency: {0}".format(err), "E")
            raise
    def moveFilesToBackup(self, workday):
        try:
            self.logger.printLog('Movendo arquivos para pasta de Backup')   
            self.rep.moveFilesToBackup(workday)
            self.logger.printLog('Arquivos movidos para pasta de Backup')    
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro ao mover arquivo para pasta de Backup: {0}".format(err), "E")
            raise

