# Pensieve
This is an example project to show the feasibility of calculating Krux segment overlap with [Pilosa](https://www.pilosa.com/).

## Setup
Run these setup commands first:

```shell
> brew install pilosa golang python
> go get github.com/pilosa/go-pilosa
> git clone git@github.com:theatlantic/go-pensieve.git && cd go-pensieve
> go build pensieve.go
> pip install git+ssh://git@github.com/theatlantic/python-pilosa.git@fix-top-n
```

In a separate window, run the Pilosa server:

```shell
> pilosa server
```

Make sure all of the krux output files (all .gz parts) are in a directory (say, `./files`).  Then, create the index and import all of the files:

```shell
> curl localhost:10101/index/segmentation -X POST
> ./pensieve -dir ./files
```

## Calculate Overlaps

```shell
> ./getintersections <kruxid> <kruxid> [<kruxid>, ...]
```