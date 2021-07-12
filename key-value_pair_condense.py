import csv
import os
import operator
import itertools
from collections import defaultdict
dictHeader= {};
dictFiles = {};

outName = "output.txt" #output file name
directory = 'E:\\Log_Analysis\\Logs' #folder path to files to process
SingleFileOutput = True # A unique file will be created for each set of key values if set to False. For example, if some entries are missing key values they will be put in a different file than entries that contain all key values.


maxInt = 100000000 #csv.field_size_limit
csv.field_size_limit(maxInt)

def resetHeader(dictHeader): #Pick the key values you want included in the output
    dictHeader["dstip"] = ""; #change "dstip" to whatever key value you want included in CSV output
    #dictHeader["sentbyte"] = "";
    #dictHeader["rcvdbyte"] = "";
    #dictHeader["sentpkt"] = "";
    #dictHeader["rcvdpkt"] = "";
    #dictHeader["devname"] = "";
    #dictHeader["date"] = "";
    #dictHeader["srcip"] = "";
    #dictHeader["dstport"] = "";
    #dictHeader["action"] = "";
    #dictHeader["srcintf"] = "";
    #dictHeader["dstintf"] = "";
    #dictHeader["vpn"] = "";

def getFileHandle(strFilePath, strWriteMode): #keep file handles open for compatibilty
    if strFilePath in dictFiles:
        return dictFiles[strFilePath] #return handle to specified file
    else:
        target = open(strFilePath, strWriteMode)
        dictFiles[strFilePath] = target #store file handle in dict for future use
        return target

def logToFile(strfilePathOut, strDataToLog, boolDeleteFile, strWriteMode):
    fileHandle = getFileHandle(strfilePathOut, strWriteMode)
    if boolDeleteFile == True:
      fileHandle.truncate()
    fileHandle.write(strDataToLog)
    #fileHandle.close()

def prependQuote(strRow):
    if strRow[:1] != '"':
        strRow = '\"' + strRow
    return (strRow)

def apendQuote(strRow):
    if strRow[-1:] != '"':
        strRow = strRow + '\"'
    elif len(strRow) == 1: #handle blank value?
      strRow = strRow + '"'
    return (strRow)



strCurrentDirectory = os.getcwd()
print(strCurrentDirectory)
resetHeader(dictHeader)
txtValues = ""
for root, directories, filenames in os.walk(directory): 
  for filename in filenames:  
      filenamed = os.path.join(root,filename) 

      print(filenamed)
      with open(filenamed, "rt") as csvfile:
          reader = csv.reader( csvfile, delimiter=' ', quotechar='^')
          
          for row in reader:
              resetHeader(dictHeader)
              dictValues = {} #dict to sort and order output values consistently
              boolGrabNextValue = False
              for col in row:
                 if "=" in col:
                     valArray = col.split("=")
                     for key in dictHeader:
                      if key == valArray[0]:
                       if valArray[1] == '': #blank
                        dictValues[key] = ""
                       elif valArray[1][:1] == '"' and valArray[1][-1:] != '"': #first character is a quote and last char is not
                          boolGrabNextValue = True
                       if len(dictValues) == 0:
                           dictValues[key] = prependQuote(valArray[1])
                           dictHeader[key] = 1  #set key to note value was present
                           lastkey = key
                           break
                       else:
                           #dictValues[lastkey] = apendQuote(dictValues[lastkey])
                           dictValues[key] = prependQuote(valArray[1])
                           dictHeader[key] = 1 #set key to note value was present
                           lastkey = key
                           break
                 elif boolGrabNextValue == True:
                  dictValues[lastkey] = dictValues[lastkey] + " " + col
                  if col[-1] == '"':
                    boolGrabNextValue = False

              tmpOut = outName
              
              for key in sorted (dictHeader):
                  if dictHeader[key] == 1: #Check dict for key value was present
                    if SingleFileOutput == False:
                      tmpOut = key + "_" + tmpOut
                    if txtValues == "":
                      txtValues = apendQuote(dictValues[key])
                    else:
                      txtValues = txtValues + "," + apendQuote(dictValues[key])
              if txtValues != "":
                logToFile(strCurrentDirectory + "/" + tmpOut,apendQuote(txtValues) + "\n", False, "a")
                txtValues = ""

#close file handles
if len(dictFiles) > 0:
    for fileName in dictFiles: 
        dictFiles[fileName].close() #Close file handle
        

