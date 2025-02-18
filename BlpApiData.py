# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 16:52:33 2020

Run Data Query via Bloomberg BLP API

https://matthewgilbert.github.io/blp/generated/blp.blp.html
https://matthewgilbert.github.io/blp/quickstart.html

@author: TQiu
"""


import os
from blp import blp
import pdblp
import pandas as pd


def getBLPData(tickers, outFPath, startDate='20021231', endDate='20221231', dataFreq='DAILY', pxFields=['PX_LAST']):
    outFName = 'PxData_'+dataFreq+'_'+startDate+'_'+endDate
    try:
        dfBBGData = pd.read_csv(outFPath+'/'+outFName+'.csv', header=[0, 1], index_col=0)
    except FileNotFoundError:
        #dfBBGData = getBBGHistPriceData(tickers, startDate, endDate, dataFreq, outFPath, outFName, pxFields)
        dfBBGData = getBDH(tickers, startDate, endDate, dataFreq, outFPath, outFName, pxFields)
        dfBBGData = pd.read_csv(outFPath+'/'+outFName+'.csv', header=[0, 1], index_col=0)
    return dfBBGData


def getBBGData(tickers, outFPath, startDate='20021231', endDate='20221231', dataFreq='DAILY', pxFields=['PX_LAST']):
    outFName = 'PxData_'+dataFreq+'_'+startDate+'_'+endDate
    try:
        dfBBGData = pd.read_csv(outFPath+'/'+outFName+'.csv', header=[0, 1], index_col=0)
    except FileNotFoundError:
        dfBBGData = getBBGHistPriceData(tickers, startDate, endDate, dataFreq, outFPath, outFName, pxFields)
        #dfBBGData = getBDH(tickers, startDate, endDate, dataFreq, outFPath, outFName, pxFields)
        dfBBGData = pd.read_csv(outFPath+'/'+outFName+'.csv', header=[0, 1], index_col=0)
    return dfBBGData


def parseBBGData(dfBBGData, pxField='PX_LAST', doFillNA=False, dropLevel='field'):
    dfBBGData = dfBBGData.xs(pxField, axis=1, level=dropLevel)
    if doFillNA:
        dfBBGData = dfBBGData.fillna(method='ffill')
    dfBBGData.index = pd.to_datetime(dfBBGData.index)
    return dfBBGData


def readBBGData(tickers, outFPath, startDate='20021231', endDate='20221231', dataFreq='DAILY', pxField='PX_LAST', doFillNA=True, dropLevel='field'):
    if pxField == None:
        return parseBBGData(getBBGData(tickers, outFPath, startDate, endDate, dataFreq))
    pxFields = [pxField]
    return parseBBGData(getBBGData(tickers, outFPath, startDate, endDate, dataFreq, pxFields), pxField, doFillNA, dropLevel)


def getBDH(tkrList, startDate, endDate, dataFreq, outFPath, outFName=None, 
           pxFields=['PX_LAST']):
    if outFName == None:
        outFName = 'PxData_'+dataFreq+'_'+startDate+'_'+endDate
    with blp.BlpQuery() as bbq:
        bbq.start()
        df = bbq.bdh(securities=tkrList, fields=pxFields, 
                     start_date=startDate, end_date=endDate, 
                     options={'periodicitySelection': dataFreq})
        df = df.pivot(index='date', columns='security')[pxFields]
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'.csv',index=True,index_label='Date')
    return df


def getBDS(tkrList, outFPath, outFName=None, 
           refFields=['SECURITY_DES','CLASSIFICATION_LEVEL_4_NAME', \
                      'CNTRY_OF_RISK','BB_COMPOSITE']):
    if outFName == None:
        outFName = 'RefData'
    with blp.BlpQuery() as bbq:
        bbq.start()
        df = bbq.bds(tkrList, refFields)
        df = df.pivot(index='ticker',columns='field',values='value')
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'.csv',index=True,index_label='Date')
    return df


def getBBGHistPriceData(tkrList, startDate, endDate, dataFreq, outFPath, outFName=None, pxFields=['PX_LAST','PX_LOW','PX_HIGH','PX_OPEN']):
    if outFName == None:
        outFName = 'PxData_'+dataFreq+'_'+startDate+'_'+endDate
    with pdblp.bopen(port=8194,timeout=50000) as bb:
        df = bb.bdh(tkrList, pxFields, startDate, endDate, elms=[('periodicitySelection', dataFreq)])
        df.droplevel('field',axis=1)
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'.csv',index=True,index_label='Date')
        bb.stop()
    return df


def getBBGRefAttrData(tkrList, outFPath, outFName=None, refFields=['SECURITY_DES','CLASSIFICATION_LEVEL_4_NAME','CNTRY_OF_RISK','BB_COMPOSITE']):
    if outFName == None:
        outFName = 'RefData'
    with pdblp.bopen(port=8194,timeout=50000) as bb:
        df = bb.ref(tkrList, refFields)
        df = df.pivot(index='ticker',columns='field',values='value')
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'.csv',index=True,index_label='Date')
        bb.stop()
    return df


__all__ = ["getBLPData", "getBBGData", "parseBBGData", "readBBGData", "getBDH", "getBDS", "getBBGHistPriceData", "getBBGRefAttrData"]