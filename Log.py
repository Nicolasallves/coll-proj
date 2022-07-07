import datetime
import logging
import settings

class Log:
    """
        Class with responsability to log events.

        Author: 
            Nicolas Alves

            Since:
                2021-03
    """

    def __init__(self, klass):
        self.klass = klass
        self.logFilePath = settings.LOG_FILE_PATH
        self.logFile = settings.LOG_FILE_NAME
        self.workday = datetime.date.today()
        logging.basicConfig(filename=self.logFilePath + self.logFile + str(self.workday) + '.log',
                                format='%(asctime)s.%(msecs)d - %(name)s - %(levelname)s - %(message)s',
                                datefmt='%d/%m/%Y %H:%M:%S',
                                level=settings.LOG_LEVEL)
        self.log = logging.getLogger(self.klass.__class__.__name__)

    def printLog(self, msg, level = 'I'):
        self.msg = msg
        if level == 'I':
            self.log.info(self.msg)
        if level == 'W':
            self.log.warning(self.msg)
        if level == 'E':
            self.log.error(self.msg)
