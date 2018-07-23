# pusher

[![GoDoc](https://godoc.org/github.com/m-lab/pusher?status.svg)](https://godoc.org/github.com/m-lab/pusher)
[![Build Status](https://travis-ci.org/m-lab/pusher.svg?branch=master)](https://travis-ci.org/m-lab/pusher)
[![Coverage Status](https://coveralls.io/repos/github/m-lab/pusher/badge.svg?branch=master)](https://coveralls.io/github/m-lab/pusher?branch=master)

Push data from nodes to cloud storage.  This is meant to be a sidecar service
for experiments deployed on [M-Lab](https://www.measurementlab.net).
Experiments write data to a particular directory and this system watches that
directory and tars, compresses, and uploads the data files.
