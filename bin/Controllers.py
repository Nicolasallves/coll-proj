from datetime import datetime
import sys
import settings as settings
import Repositories as repo
import Services as serv
import Log as logger
class Controller:
    def __init__(self):
        self.rep = repo.Repository()
        self.service = serv.Service()
        self.logger = logger.Log(self)
        self.workday = None
    def runCollateral(self, workday=None):
        try:
            if workday is None:
                workday=datetime.today().strftime('%Y%m%d')
            self.logger.printLog('############# Executando calculo de Collateral do dia ' + workday + " #############")
            ###run services with collateral rules
            self.service.saveVM(workday=workday)
            self.service.saveIAIM(workday=workday)
            self.service.saveSufficiency(workday=workday)
            self.service.moveFilesToBackup(workday=workday)
            self.logger.printLog('############# Fim do calculo de Collateral do dia ' + workday + " #############")
        except:
            err = sys.exc_info()
            self.logger.printLog("Unexpected error: {0}".format(err), "E")
            raise

