#!/bin/bash

MWS="$(cat /eniq/installation/config/eniq_sw_locate | awk -F "@" '{print $1}')";

#echo "$MWS";

cd /net/$MWS/JUMP/LIN_MEDIA/

Path="$(ls)"

#echo "$Path"

Path=/net/$MWS/JUMP/LIN_MEDIA/$Path

#echo "$Path"

cd /etc/yum.repos.d/

touch RHEL.repo

echo "[RHEL]
Name=rhel
baseurl=file://$Path
gpgcheck=0
enabled=1" > RHEL.repo

#cat RHEL.repo

echo y | yum install python3