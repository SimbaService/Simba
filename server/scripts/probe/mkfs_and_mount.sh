#!/bin/bash
DISK=$1
MP=$2
DEV=/dev/$DISK

mountpoint $MP
if [ $? -eq 0 ] ; then
echo "already mounted"
else
mkfs.xfs -f -i size=1024 ${DEV}
mkdir -p $MP
mount -o noatime,nodiratime,nobarrier,logbufs=8 ${DEV} $MP
fi
