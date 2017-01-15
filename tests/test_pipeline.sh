#!/bin/sh 

bnpl file.directory --path='fixtures/' | bnpl taglib.get_tags | bnpl fpcalc.uid | bnpl core.importer | jq .