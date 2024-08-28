#!/bin/bash

FILE="/eniq/home/dcuser/BT-FT_Script/CPAN_Modules.zip";

if [ -f "$FILE" ]; then

MWH="/eniq/home/dcuser/mws.properties";
cp "$MWH" .;

source /eniq/sql_anywhere/bin64/sa_config.sh;

sed -i -e 's/\r$//' PythonInstall.sh

echo shroot12 | su - root bash -c '/eniq/home/dcuser/BT-FT_Script/PythonInstall.sh'
echo shroot12 | su - root bash -c 'chmod 777 /eniq/home/dcuser/BT-FT_Script/cpanInstallerRHEL.pl /eniq/home/dcuser/BT-FT_Script/CPAN_Modules.zip';
echo shroot12 | su - root bash -c 'perl /eniq/home/dcuser/BT-FT_Script/cpanInstallerRHEL.pl /eniq/home/dcuser/BT-FT_Script/CPAN_Modules.zip';
mkdir /eniq/home/dcuser/BT-FT_Log;


rm /eniq/home/dcuser/BT-FT_Script/CPAN_Modules.zip;

fi

echo "TP Name:";
read input
echo "$input" > data.txt

echo "TP Name: '$input'"
arr=(${input//"_"/ })

if [ ${arr} == "DC" -a ${arr[2]} != "LLE" -a ${arr[2]} != "IPRAN" -a ${arr[2]} != "WLE"  -a ${arr[2]} != "FFAXW" -a ${arr[2]} != "FFAX" ]
then
echo "$input is a PM Techpack.";
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_txt_creator.pl;
perl -lpe 's/^\s*(.*\S)\s*$/$1/' data.txt > data.tmp && mv data.tmp data.txt;
perl -pi -e 'chomp if eof' data.txt;

echo "Select a test case script to run:
[1] Log Verification
[2] PM Data Loading Check
[3] Counters Loading Check
[4] Keys Loading Check
[5] Duplicate and Suspected Data Check
[6] Date-Time ID Check
[7] Counter Data Validation
[8] Aggregation Status Check
[9] Man Mods File check
[10] OSS_ID Key Data Validation
[11] NodeName Key Data Validation
[12] SN Key Data Validation
[13] DC_RELEASE Key Data Validation
[14] Validation of NR flex and flex vectors
[15] Dynamic_Counters Validation
[16] Validation of ERBS flex counters
[17] Bulk_CM Moid Validation
[18] External Statement Sheet Verification 
[19] MultiDyn Counter Data Validation
[20] ALL";

echo "For multiple options, please use comma(,) seperated values:";
read input;

arr=(${input//","/ })

for val in "${arr[@]}";
do
if [ "$val" -lt "0" -o "$val" -gt "20" ]
then
echo "Invalid Input $val";
exit
fi
done

echo "You have chosen $input";

for val in "${arr[@]}";
do

echo "$val";

if [ "$val" == "1" ]
then 
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Log_check.pl;
	
	
elif [ "$val" == "2" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DataLoading_Check.pl;
	
elif [ "$val" == "3" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Counter_check.pl;

elif [ "$val" == "4" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Keys_check.pl;
	
elif [ "$val" == "5" ]
then
	echo "Duplicate and Suspected Data Check test cases are triggering";
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Duplicate_check.pl;
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Suspect_Row_check.pl;
	echo "Duplicate and Suspected Data Check test cases completed";
elif [ "$val" == "6" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Date_check.pl;
	
elif [ "$val" == "7" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_CounterDataValidation.pl;
	
elif [ "$val" == "8" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Aggregation_Status.pl;
elif [ "$val" == "9" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Man_Mods_File_Check.pl;
elif [ "$val" == "10" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT_FT_OSS_ID_Validation.pl;
elif [ "$val" == "11" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_NodeName_KeyDataValidation.pl;
elif [ "$val" == "12" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_SN_KeyDataValidation.pl;
elif [ "$val" == "13" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DCRelease_KeyDataValidation.pl;
	
elif [ "$val" == "14" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Verify_NR_Flex.pl;
		
elif [ "$val" == "15" ]
then 
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DynamicCounter_Validation.pl;
	
elif [ "$val" == "16" ]
then 
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_ERBS_Flex.pl;

elif [ "$val" == "17" ]
then 
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Bulk_Cm_Moid_Validation.pl;
elif [ "$val" == "18" ]
then 
	python3 /eniq/home/dcuser/BT-FT_Script/BT-FT_ExternalStmtVerify.py;
elif [ "$val" == "19" ]
then 
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_MultiDyn_Counter_Data_Validation.pl;
else
	echo "All test cases are triggering";
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Log_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DataLoading_Check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Keys_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Counter_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Duplicate_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Suspect_Row_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Date_check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_CounterDataValidation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Aggregation_Status.pl;

perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Man_Mods_File_Check.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT_FT_OSS_ID_Validation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_NodeName_KeyDataValidation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_SN_KeyDataValidation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DCRelease_KeyDataValidation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Verify_NR_Flex.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_DynamicCounter_Validation.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_ERBS_Flex.pl;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Bulk_Cm_Moid_Validation.pl;
python3 /eniq/home/dcuser/BT-FT_Script/BT-FT_ExternalStmtVerify.py;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_MultiDyn_Counter_Data_Validation.pl;
echo "All Test cases completed"; 

fi

done

else
echo "$input is a Topology Techpack.";
echo "Date:";
read input
input=`echo $input | sed -e 's/^[[:space:]]*//'`
echo "$input" > time.txt
echo "Date: '$input'"

echo "TopoNodenameKey:";
read TopoNodenameKey
TopoNodenameKey=`echo $TopoNodenameKey | sed -e 's/^[[:space:]]*//'`

echo "TopoNodenameKey: '$TopoNodenameKey'"

echo "TopoNodename:";
read TopoNodename
TopoNodename=`echo $TopoNodename | sed -e 's/^[[:space:]]*//'`

echo "TopoNodename: '$TopoNodename'"

Pass=`bash /eniq/sw/installer/getPassword.bsh -u dc`
#echo "$Pass"
arr=(${Pass//": "/ })
echo "${arr[2]}" > Passwords.txt
Pass=`bash /eniq/sw/installer/getPassword.bsh -u dwhrep`
arr=(${Pass//": "/ })
echo "${arr[2]}" >> Passwords.txt
Pass=`bash /eniq/sw/installer/getPassword.bsh -u etlrep`
arr=(${Pass//": "/ })
echo "${arr[2]}" >> Passwords.txt

echo "Select a test case script to run:
[1] Log Verification
[2] Topology Data Loading Check
[3] Keys Loading Check
[4] Man Mods File Check
[5] External Statement Sheet Verification
[6] ALL";

echo "For multiple options, please use comma(,) seperated values:";
read input;

arr=(${input//","/ })

for val in "${arr[@]}";
do
if [ "$val" -lt "0" -o "$val" -gt "6" ]
then
echo "Invalid Input $val";
exit
fi
done

echo "You have chosen $input";

for val in "${arr[@]}";
do

echo "$val";

if [ "$val" == "1" ]
then 
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Log_check.pl;
	
elif [ "$val" == "2" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_TopologyLoading_Check.pl $TopoNodenameKey $TopoNodename;
	
elif [ "$val" == "3" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Keys_check.pl $TopoNodenameKey $TopoNodename;

elif [ "$val" == "4" ]
then
	
	perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Man_Mods_File_Check.pl;
elif [ "$val" == "5" ]
then 
	python3 /eniq/home/dcuser/BT-FT_Script/BT-FT_ExternalStmtVerify.py;
else

perl /eniq/home/dcuser/BT-FT_Script/BT-FT_TopologyLoading_Check.pl $TopoNodenameKey $TopoNodename;
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Keys_check.pl $TopoNodenameKey $TopoNodename; 
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Log_check.pl; 
perl /eniq/home/dcuser/BT-FT_Script/BT-FT_Man_Mods_File_Check.pl;
python3 /eniq/home/dcuser/BT-FT_Script/BT-FT_ExternalStmtVerify.py;
fi
done
fi
rm -rf Passwords.txt
exit 1;

