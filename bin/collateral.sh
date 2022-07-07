#!/bin/bash

ODATE=$1
ROOT=/sistemas/e2/collateral/bin
COMPONENT_NAME=Main
RUN_COMPONENT=$COMPONENT_NAME.py

#Altera diretorio para local dos executaveis
cd $ROOT

#Executa Motor Python Collateral
scl enable rh-python38 'python '$RUN_COMPONENT' '$ODATE

exit $?
