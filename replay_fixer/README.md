# Replay File Fixer

This app can be used to strip the `$query` wrapper that queries destined for a sharded cluster seem to have, for playback against a non-shareded cluster.

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
