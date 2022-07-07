#!/bin/bash

#Path dos executaveis do Collateral
COLL_PATH='/sistemas/e2/collateral/bin'

#Configura repositorio PIP
scl enable rh-python38 'python -m pip config set global.index-url http://artifactory.santanderbr.corp/artifactory/api/pypi/pypi-all/simple'
scl enable rh-python38 'python -m pip config set global.trusted-host artifactory.santanderbr.corp'
#Atualiza depedencias
scl enable rh-python38 'python -m pip install --user -r '$COLL_PATH'/requirements.txt'

exit $?
