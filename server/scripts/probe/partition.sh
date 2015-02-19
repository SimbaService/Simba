#!/bin/bash

DISK=$1
MP=$2
DEV=/dev/$DISK

mountpoint $MP
if [ $? -eq 0 ] ; then

echo "already mounted"

else

umount ${DEV}1
dd if=/dev/zero of=$DEV bs=1M count=10
fdisk $DEV <<EOF
n
p
1


w
EOF

mkfs.xfs -f -i size=1024 ${DEV}1
mkdir -p $MP
mount -o noatime,nodiratime,nobarrier,logbufs=8 ${DEV}1 $MP

fi
