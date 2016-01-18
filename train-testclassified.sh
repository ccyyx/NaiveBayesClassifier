#!/bin/bash

getTestData(){
	count=$(($1/4))
	pth=$(basename `pwd`)
	mkdir /root/NBCorpus/NBCorpus/CountryTest/$pth
	for file in ./*
		do
		if((count==0));then
			break
		else
			mv $file /root/NBCorpus/NBCorpus/CountryTest/$pth
			let --count
		fi
		done
	}

pre(){
	count=0
	for file in $1/*
	do
		if test -d $file;then
			cd $file
			pre $file
		else
			let ++count
		fi
	done

	if((count<80));then
		rm -r $(pwd)
	else
		getTestData $count
	fi	
}
mkdir /root/NBCorpus/NBCorpus/CountryTest
pre /root/NBCorpus/NBCorpus/CountryTrain

