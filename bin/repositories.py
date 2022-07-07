import settings
import os
from glob import glob
import pandas as pd
import numpy as np
import requests
import json
import Log as logger
import sys
import shutil

class Repository:
    """
        Class with responsability to connect in database, services and files.

        Author: 
            Nicolas Alves
            Renato Saraiva 

            Since:
                2020-12
    """

    def __init__(self, ftp=None):
        self.ftp = ftp
        self.logger = logger.Log(self)
        self.folderName = settings.FILES_PATH
        self.dealsFile = settings.DEALS_FILE_NAME
        self.garantFile = settings.GUARANT_FILE_NAME   
        self.eofFile = settings.EOF_FILE_NAME     

        
    def mongoConnection(self, db, collection, stringConnection=None):

        if collection == 'csas':
            with open(os.getcwd() + "/csadb/csa.json") as json_file:
                csas = json.load(json_file)
            return csas['csa']

        if collection == 'calendar':
            colunas=[]
            return None

        if collection == 'collateralblockeds':
            colunas=['CPF_CNPJ', 'Produto', 'SubProduto', 'Produto_Instrument', 'Subproduto_InstrumentType', 'ValorLiquido', 'PrincipalAtual', 'ValorLivreBloqueio', 'ValorBruto_ValorAtual', 'valor_garantia_bloqueado', 'CotacaoAtualD1', 'fl_bloqueio_tatico']
            df = pd.DataFrame(np.array([['12123123000103', '', '', 'RENDA FIXA', 'LCI', 1000000.0, 1000000.00, 234100.00, 435678.98, 1000000.00,0,1]]), columns=colunas)
            return df

    def setAttributes(self, folderName, datenow):
        try:
            self.folderName = folderName
            self.datenow = datenow
        except:
            err = sys.exc_info()
            self.logger.printLog("Unexpected error: {0}".format(err), "E")
            raise

    def currencyPtaxFake(self, workday):
        colunas = ['moeda', 'vl_ptax_venda_fechamento', 'venda_brl']
        df_ptax = pd.DataFrame(np.array([['BRL', 1, 1]]), columns=colunas)
        df_ptax_ = pd.DataFrame(np.array([['BRL', 1, 1]]), columns=colunas)

        df_ptax_['moeda'] = 'USD'
        df_ptax_['vl_ptax_venda_fechamento'] = 5.65
        df_ptax_['venda_brl'] = 5.65
        df_ptax = df_ptax.append(df_ptax_)

        return df_ptax

    def moveFilesToBackup(self, workday):
        try:
            dealsInput = self.folderName + 'input/' + self.dealsFile + workday + '.csv'
            collateralInput = self.folderName + 'input/' + self.garantFile + workday + '.csv'
            dealsBackup = self.folderName + 'backup/' + self.dealsFile + workday + '.csv'
            collateralBackup = self.folderName + 'backup/' + self.garantFile + workday + '.csv'

            shutil.move(dealsInput, dealsBackup)
            shutil.move(collateralInput, collateralBackup)
        except:
            err = sys.exc_info()
            self.logger.printLog("Unexpected error: {0}".format(err), "E")
            raise

    def getDeals(self, workday):
        try:
            self.logger.printLog('Executando metodo getDeals.')
            folder = self.folderName + 'input/' + self.dealsFile + workday + '.csv'
            self.logger.printLog('Lendo arquivo ' + folder)
            df_deals = pd.read_csv(folder, low_memory=False, sep=",", header=0, decimal=".")

            return df_deals
        except pd.errors.EmptyDataError:
            self.logger.printLog("Arquivo de deals vazio.", "I")
            return pd.DataFrame()
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no metodo getDeals: {0}".format(err), "E")
            raise

    def getCollaterals(self, workday):
        try:
            self.logger.printLog('Executando metodo getCollaterals.')
            folder = self.folderName + 'input/' + self.garantFile + workday + '.csv'
            self.logger.printLog('Lendo arquivo ' + folder)
            return pd.read_csv(folder, low_memory=False, sep=",", header=0, decimal=".")
        except pd.errors.EmptyDataError:
            self.logger.printLog("Arquivo de garantias vazio.", "I")
            colunas=['CPF_CNPJ', 'Produto', 'SubProduto', 'Produto_Instrument', 'Subproduto_InstrumentType', 'ValorLiquido', 'PrincipalAtual', 'ValorLivreBloqueio', 'ValorBruto_ValorAtual', 'valor_garantia_bloqueado', 'CotacaoAtualD1', 'fl_bloqueio_tatico']
            df = pd.DataFrame(np.array([['A', 'A', 'A', 'A', 'A', float("0"), float("0"), float("0"), float("0"), float("0"),float("0"),float("1")]]), columns=colunas)
            return df
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no metodo getCollaterals: {0}".format(err), "E")
            raise

    def createEOF(self, workday):
        try:
            self.logger.printLog('Gerando arquivo bastao.')
            f = self.folderName + 'output/' + self.eofFile + workday + '.eof'
            arq = open(f,"x")
            arq.close()
        except:
            err = sys.exc_info()
            self.logger.printLog("Erro no metodo createEOF: {0}".format(err), "E")
            raise
