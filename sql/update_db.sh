#!/bin/bash

set -e

DB_BASE_PATH="/var/lib/mastermind-minion"
DB_PATH="$DB_BASE_PATH/minions.db"
DB_PATCHES="$DB_BASE_PATH/patches/"

if [ -e $DB_PATH ]; then
	echo "Using database $DB_PATH";
	for line in `sqlite3 $DB_PATH 'select * from patches order by id asc'`
	do
		patches+=(`echo $line | cut -d"|" -f1`)
	done
else
	echo "No usable database found, new one will be created";
fi

function apply_patch {
	printf "Applying patch %s from %s: " "$1" "$2"
	sqlite3 $DB_PATH < $2 && printf "applied\n" && sqlite3 $DB_PATH "insert into patches values ('$1', '$3');" || (printf "Patch %s failed, exiting...\n" $1 && exit 1)
}

for patch_fname in `find $DB_PATCHES -type f -regextype posix-egrep -regex ".*/[0-9]{2}-.*" | sort`
do
	patch_bname=`basename $patch_fname`
	version=`echo $patch_bname | cut -d'-' -f1`

	applied=0
	for applied_version in ${patches[*]}
	do
		if [ $version == $applied_version ]; then
			printf "Patch %s already applied\n" $version
			applied=1
			break
		fi
	done
	if [ $applied -eq 0 ]; then
		apply_patch $version $patch_fname $patch_bname
	fi
done

