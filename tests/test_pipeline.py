#!/bin/sh 

bnpl file.directory --path='tests/fixtures' | bnpl taglib.get_tags | bnpl fpcalc.uid | bnpl core.loader | jq .