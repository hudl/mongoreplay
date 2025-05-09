# ClusterTime Fixer

## Usage

Once built:

```sh
$ ./clustertime_remover replay_file.bson modified_replay_file.bson
```

From source:

```sh
$ go run clustertime_remover/main.go replay_file.bson modified_replay_file.bson
```

## Building

```sh
$ go build -o clustertime_remover/clustertime_remover clustertime_remover/main.go
```
