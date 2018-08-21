# Pensieve
This is an example project to show the feasibility of calculating Krux segment overlap with [Pilosa](https://www.pilosa.com/).

## Setup
Run these setup commands first:

> brew install pilosa golang
> go get github.com/pilosa/go-pilosa
> go build pensieve.go
> pip install git+ssh://git@github.com/theatlantic/python-pilosa.git@fix-top-n

In a separate window, run the Pilosa server:

> pilosa server

Make sure all of the krux output files (all .gz parts) are in a directory (say, `./files`).  Then, create the index and import all of the files:

> curl localhost:10101/index/segmentation -X POST
> ./pensieve -dir ./files

## Calculate Overlaps

> ./getintersections <kruxid> <kruxid> [<kruxid>, ...]
