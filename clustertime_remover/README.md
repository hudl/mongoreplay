# ClusterTime Fixer

## Usage

Once built:

```sh
$ ./replay_fixer replay_file.bson modified_replay_file.bson
```

From source:

```sh
$ go run replay_fixer/main.go replay_file.bson modified_replay_file.bson
```

## Building

```sh
$ go build -o replay_fixer replay_fixer/main.go
```
