#!/bin/bash
printf "Start time: "; /bin/date
printf "Job is running on node: "; /bin/hostname
printf "Job running as user: "; /usr/bin/id
printf "Job is running in directory: "; /bin/pwd

printenv

for ((i=1; i<=$#; i++))
do
  printf "${i}: ${!i}\n"
done

python3 Delphes2SA.py --input ${1} --output ${2}
