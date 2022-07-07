from Controllers import Controller
import sys
import os

class Main:
    """
        Class with responsability to execute Collateral Calculations.

        Author: 
            Eduardo Caversan

            Since:
                2021-03
    """
    def execute(workday):
        try:
            c = Controller()
            c.runCollateral(workday)
            os._exit(0)
        except:
            os._exit(1)

    if __name__ == "__main__":
        if(len(sys.argv) > 1):
            execute(sys.argv[1])
        else:
            execute(None)
