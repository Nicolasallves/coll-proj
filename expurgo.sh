#!/bin/bash

#
if [ -z $1 ]
then
    HOLDING_PERIOD=90
else
    HOLDING_PERIOD=$1
fi

if [ -z $2 ]
then
    FILE_PREFIX=collateral_
else
    FILE_PREFIX=$2
fi

if [ -z $3 ]
then
    FILE_EXTENSION=.log
else
    FILE_EXTENSION=$3
fi

if [ -z $4 ]
then
    LOG_PATH=/log/
else
    LOG_PATH=$4
fi

#Apaga os arquivos conforme parâmetros passados
find $LOG_PATH$FILE_PREFIX*$FILE_EXTENSION -mtime +$HOLDING_PERIOD -type f -delete

exit $?
