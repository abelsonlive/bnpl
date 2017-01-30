#!/bin/sh 

bnpl file.directory --path='tests/fixtures/' |\
	bnpl fpcalc.uid |\
	bnpl taglib.get_tags --tags=artist,genre,title|\
	bnpl core.importer