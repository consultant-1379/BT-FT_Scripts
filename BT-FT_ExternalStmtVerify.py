# This Python file uses the following encoding: utf-8
import subprocess
#import pandas as pd
from openpyxl import load_workbook
import time
import re


def ExternalTxt(Def):
    global l,log,count,Result_Body,flag1
    Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='80%'><tr><center><th><font size=4>Observations in External Statement TXT File</font></th></center></tr>"
    try:
        Txt=open(Def,'r')
        ExTxt=''
        ETxt=Txt.readlines()
        for i in ETxt:
            ExTxt+=i
        ExtTxt=ExTxt.split("@@")[1:]
        #print("ex:",ExtTxt)
        for Query in ExtTxt:
            #print("query:",Query)
            View=Query.split("==")
            if View[0] in l.keys():
                l[View[0]].append(View[1].strip())
            else:
                flag1=1
                Result_Body+="<tr><td><font size=3>View Name("+View[0]+") is not present in Model-T</font></td></tr>"
                count+=1
                
    except Exception as e:
        print(e)
    finally:
        Txt.close()
        Result_Body+="</table>"
        #print(l)



'''def DwhdbCheck():
    global log,l,count,DCPass
    dwhcount=0
    try:
        log.write("External Statement Loading into Dwhdb Check\n")
        for key in l.keys():
            if "create" in l[key][1].lower():
                l[key][1]=l[key][1].replace("count(*)","*")
                l[key][1]=l[key][1].replace("sys.", "")

                query=l[key][1][4:l[key][1].index(")")]
                print(query)
                pipe = subprocess.call(["perl", "ExecuteSql.pl","dwhdb","2640","dc",DCPass,query])
                if pipe!=0:
    	            exit()
                dbout=open('SqlOutput.txt','r')
                DBout=dbout.readline().split(" @@ ")
                Viewtext=DBout[2].replace('"', '')
                Viewtext=Viewtext.replace("dc.", '')
                print(Viewtext.lower()+"=="+l[key][1][l[key][1].index("BEGIN")+6:l[key][1].index("END")].lower())
                if Viewtext.lower()!=l[key][1][l[key][1].index("BEGIN")+6:l[key][1].index("END")].lower():
                    log.write("\t Definition in Model-T and View Text column in DwhDB are not same for View Name ("+l[key][1]+").\n")
                    dwhcount+=1
                    count+=1
                if DBout[1] not in key:
                    log.write("\t Viewname ("+DBout[1]+") in Dwhdb is not in Viewname ("+key+") of Model-T.\n")
                    dwhcount+=1
                    count+=1
    except Exception as e:
        print(e)
    finally:
        log.write("No.Of Observations in Dwhdb Check: "+str(dwhcount)+"\n\n")'''


def DwhdbInsertCheck(pkg):
    global log,l,count,pas,Result_Body,DCPass,DwhrepPass
    InsertCount=0
    passcount=0
    try:
        #log.write("External Statement Loading into Dwhdb Check\n")
        Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='100%'><tr><center><th><font size=4>Validation of External Statements</font></th></center></tr></table>"
        Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='100%'><tr><th><font size =3>View Name</font></th><th><font size=3>Remark</font></th><th><font size=3>Result</font></th></tr>"
        for key in l.keys():
            if "insert" in l[key][1].lower():
                flag=True
                Values=[]
                Table=''
                Columns='' 
                l[key][1]=l[key][1].replace(') VALUES',') values')
                Inserts=re.split("INSERT [into|INTO]",l[key][1])
                #print(key)
                if "values" in Inserts[1]:
                    for i in range(1,len(Inserts)):
                        if i==1:
                            Temp=Inserts[i].split(' ')[1]
                            if '(' in Temp:
                                Table=Temp[:Temp.index('(')].strip()
                            else:
                                Table=Temp.strip()
                            #print("Table Name:",Table)
                            Columns=Inserts[i].split(Table)[1].split('values')[0].replace('(','').replace(')','').strip().split(',')
                        #print(Inserts[i])
                        if "),(" in Inserts[i].split('values')[1] or "), (" in Inserts[i].split('values')[1]:
                            for i in re.split("\),\s?\(",Inserts[i].split('values ')[1]):
                                #print(i.replace("'",""))
                                Values.append(i.replace("'","").replace("\n","").split(','))
                            Values[0][0]=Values[0][0][1:]
                            Values[len(Values)-1][len(Values[len(Values)-1])-1]=Values[len(Values)-1][len(Values[len(Values)-1])-1][:-1]
                        else:
                            Value=re.split(",\s?",Inserts[i].split('values')[1][Inserts[i].split('values')[1].index('(')+1:Inserts[i].split('values')[1].split("\nEND")[0].rindex(')')].replace('\n','').strip().replace("'",''))
                            Value=[i for i in Value if i != "getdate()"]
                            #print(Value)
                            for i in range(len(Value)):
                                #if Value[i]!=' ':
                                 #   Value[i]=Value[i].strip()
                                if Value[i]=='':
                                    Value[i]='null'
                            Values.append(Value)
                    
                    #print("Table Name:",Table)
                    #print("Columns:",Columns)
                    #print("Valuesinmodelt:",Values)
                    Columns=[i.strip() for i in Columns]
                    if "CREATED" in Columns:
                        Columns.remove("CREATED")
                    if "MODIFIED" in Columns:
                        Columns.remove("MODIFIED")
                    
                    query="select dataname,datatype,datasize,DATASCALE from Referencecolumn where typeid like '"+pkg+Table+"'"
                    
                    pipe = subprocess.call(["perl", "ExecuteSql.pl","repdb","2641","dwhrep",DwhrepPass,query])
                    dbout=open('SqlOutput.txt','r')
                    DBout=[out.replace('\n','').split(" @@ ") for out in dbout.readlines()]
                    #print(DBout)
                    for out in DBout:
                        if out[0] in Columns:
                            if out[1]=="numeric" and out[3]!='0':
                                for Value in Values:
                                    Temp=Value[Columns.index(out[0])]
                                    if len(Temp.split('.')) ==1 or len(Temp.split('.')[1]) <= int(out[3]):
                                        Value[Columns.index(out[0])]=str(format(float(Temp),"."+out[3]+"f"))                            
                    Column=','.join(Columns)
                    query="select "+Column+" from "+Table
                    #print(query)
                    pipe = subprocess.call(["perl", "ExecuteSql.pl","dwhdb","2640","dc",DCPass,query])
                    dbout=open('SqlOutput.txt','r')
                    DBout=[out.replace('\n','').replace("@@ .","@@ 0.").replace("@@ -.","@@ -0.").split(" @@ ") for out in dbout.readlines()]
                    for DB in DBout:
                        for D in range(0,len(DB)):
                            if DB[D] == '':
                                DB[D]="null"
                    #print(DBout)
                    for Value in Values:
                        if Value not in DBout:
                            print(Value)
                            print(DBout)
                            InsertCount+=1
                            count+=1
                            flag=False
                            break
                    if(flag):
                        pas+=1
                        passcount+=1
                    else:
                        #print("col1:",Columns)
                        #print("val1:",Values)
                        #Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='100%'><tr><center><th><font size=4>External Statement implemented in DwhDB</font></th></center></tr>"
                        #Result_Body+="<tr><th><font size =3>View Name</font></th><th><font size=3>Remark</font></th><th><font size=3>Result</font></th></tr>"
                        Result_Body+="<tr><td><font size=2><center>"+key+"</center><td><font size=2><center>Insert</center></font></td><td><font color=red size=2><center>FAIL</center></font></td></tr>"


    except Exception as e:
        print(e)
    finally:
        print("No.Of Observations in Dwhdb Check: "+str(InsertCount))



   
def RepdbCheck(query):
    global log,count,l,pas,Result_Body,Result_Footer,flag,DwhrepPass
    repcount=0
    query1="select * from ExternalStatement where versionid like '"+query+"%'"
    #print(query1)
    pipe = subprocess.call(["perl", "ExecuteSql.pl","repdb","2641","dwhrep",DwhrepPass,query1])
    if pipe!=0:
        print("exit")
        exit()
    try:
        #print("Hii")
        dbout=open('SqlOutput.txt','r')
        DBout=dbout.readlines()
        Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='80%'><tr><center><th><font size=4>Verification of External Statement loading into REPDB</font></th></center></tr></table>"
        Result_Body+="<table BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='80%'><tr><th><font size =3>View Name</font></th><th><font size =3>Remark</font></th><th><font size=3>Result</font></th></tr>"
        line=[]
        for out in DBout:
            #print(out)
            flag=0
            if out.startswith(query):
                if len(out.split(" @@ "))==6:
                    line=out.split(" @@ ")
                else:
                    line=out.split(" @@ ")[0:len(out.split(" @@ "))-1]
                str1=out.split(" @@ ")[-1]
            elif out.isnumeric():
                line.append(str1)
                line.append(out)
            elif " @@ " in out:
                str1+=out.split(" @@ ")[0]
                line.append(str1)
                line.append(out.split(" @@ ")[1])
            else:
                str1+=out
            if len(line)==6:
                #print(line)
                if "AGGLEVEL" not in line[1] and len(line)>0:
                    if line[1] in l.keys():
                        if l[line[1]][0]!=line[3]:
                            Result_Body+="<tr><td><font size=2><center>"+line[1]+"</center></font></td><td><font size=2><center>Database Mismatch</center></font></td><td><font color=red size=2><center>FAIL</center></font></td></tr>"
                            repcount+=1
                            flag+=1
                            count+=1
                        if l[line[1]][1]!=line[4]:
                            #print(l[line[1]])
                            #print("jgcdwgc"+str(line[4]))
                            Result_Body+="<tr><td><font size=2><center>"+line[1]+"</center></font></td><td><font size=2><center>Definition Mismatch</center></font></td><td><font color=red size=2><center>FAIL</center></font></td></tr>"
                            repcount+=1
                            flag+=1
                            count+=1
                    else:
                        Result_Body+="<tr><td><font size=2><center>"+line[1]+"</center></font></td><td><font size=2><center>View Name is not present in the Model-T</center></font></td><td><font color=red size=2><center>FAIL</center></font></td></tr>"
                        repcount+=1
                        flag+=1
                        count+=1
                    if flag==0:
                        pas+=1
                        #print("RepDB:"+str(pas))
    except Exception as e:
        print(e)
    finally:
        Result_Body+="</table><br><br><br>"
        dbout.close()


t = time.localtime()
Start_Time = time.strftime("%H:%M:%S", t)
flag1=0
Result_Header="<tr><td>START TIME: </td><td>"+Start_Time+"</td></tr>"
Result_Body=""
Result_Footer=""
count=0
pas=0
print("****************External Statement Sheet Verification***************")
try:
    Passwords=open('Passwords.txt','r').readlines()
    DCPass=Passwords[0]
    DwhrepPass=Passwords[1]
    Txtname=open('data.txt','r').readline().split("_")
    #print(Txtname)
    try:
        l={}
        flag=0
        Temp=Txtname[-1]
        Txtname[-1]=Txtname[-2]
        Txtname[-2]=Temp.split(".")[0][1:]
        xl='_'.join(Txtname)
        pkg='_'.join(Txtname[:-2])
        pkg+=":(("+Txtname[-2]+")):"
        print(pkg)
        print("Model-T Name: "+xl+".xlsx")
        log=open("/eniq/home/dcuser/BT-FT_Log/Verify_Ext_Stmt_Sheet_"+xl+".html",'w')
        log.write("<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'><html><head><title>ENIQ Regression Feature Test</title><STYLE TYPE='text/css'>h3{font-family:tahoma;font-size:12px}body,td,tr,p,h{font-family:tahoma;font-size:11px}.pre{font-family:Courier;font-size:9px;color:#000}.h{font-size:9px}.td{font-size:9px}.tr{font-size:9px}.h{color:#3366cc}.q{color:#00c}</STYLE></head>")
        log.write("<h1> <font color=MidnightBlue><center> <u> EXTERNAL STATEMENT SHEET VERIFICATION </u> </font> </h1><body bgcolor=GhostWhite> <center> <table  BORDER='3' CELLSPACING='2' CELLPADDING='3' WIDTH='80%' >")
        File = load_workbook(filename=xl+".xlsx")
        Excel = File['External Statement']
        for row in Excel:
            if row[0].value != 'View Name':
                if ".txt" in row[2].value:
                    l[row[0].value]=[row[1].value]
                    n=row[2].value
                    flag=1
                else:
                    l[row[0].value]=[row[1].value,row[2].value.strip()]

        if flag==1:
            if xl+".txt"==n:
                ExternalTxt(n)
            else:
                print("External Statement Text File name in Model-T is different from name of Model-T,hence killing the process.")
                exit()
        if flag1==0:
            RepdbCheck(('_'.join(Txtname[0:-2])+":(("+Txtname[-2]+"))"))
        else:
            print("Txt file is having some fails, check the log file for the fail scenarios and run the script after the appropriate changes")
        DwhdbInsertCheck(pkg)
    except Exception as e:
        print(e)
except Exception as e:
    print(e)
finally:
    #log.write("\nTotal No.Of Observations for External Statement Sheet: "+str(count))
    #log.write(str(l))
    t = time.localtime()
    End_Time = time.strftime("%H:%M:%S", t)
    Result_Header+="<tr><td>END TIME: </td><td>"+End_Time+"</td></tr>"
    Result_Header+="<tr><td>RESULT: </td><td>"+"Pass:"+str(pas)+" Fail:"+str(count)+"</td></tr></table><br>"
    Result_Footer+="</body></html>"
    log.write(Result_Header+Result_Body+Result_Footer)
    print("External Statement Sheet Verification: Pass: "+str(pas)+" Fail: "+str(count))
    print("****************External Statement Sheet Verification Completed***************")
