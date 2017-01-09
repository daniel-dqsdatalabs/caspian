#!/usr/bin/env bash
#creates new file for each tbl file by removing last character in each line
#Run in the folder where the tbl files are present

rm *.psv 2>/dev/null
files=$(ls *.tbl)
echo "Following files detected as tbl"
echo "$files"
echo "--------------------------------------------";
for file in ${files}; do
	echo "Processing :"${file}" to "${file}".psv";
	sed 's/.$//' ${file} >> ${file}.psv;    #sed s(substitute)/search/replaceString/
done
echo "--------------------------------------------";
echo "Done";