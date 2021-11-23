#!/bin/bash
printf "Start time: "; /bin/date
printf "Job is running on node: "; /bin/hostname
printf "Job running as user: "; /usr/bin/id
printf "Job is running in directory: "; /bin/pwd

for ((i=1; i<=$#; i++))
do
  printf "${i}: ${!i}\n"
done

printenv

ls -lavh .

wget https://raw.githubusercontent.com/scipp-atlas/mario-mapyde/main/scripts/Delphes2SA.py -O Delphes2SA.py

python3 Delphes2SA.py --input ${1} --output ${2}
