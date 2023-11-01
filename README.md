## ktool - a tool for Kafka

<p align="center">
<img src="https://assets.itsallbroken.com/github/ktool.gif" />
</p>

`ktool` is a dead-simple tool for working with data in Kafka:

* Copy partitions / topics to disk
* Replay messages
* Inspect message content from Kafka, and on disk
* Copy between partitions / Kafka clusters
* JSON output for chaining with other tools

## Installation

Download pre-built binaries from the [releases page].

Alternatively clone this repo and run `cargo build --release` if you have a rust
toolchain installed! 

[releases page]: https://github.com/domodwyer/ktool/releases/latest

## Usage

A data message stream is either **on-disk** or **in Kafka**. On disk sources are
specified as file paths:

* `./prod.kbin`
* `./data/dump.kbin`
* `/backups/monday.kbin`

Kafka sources are expressed with a `kafka://` prefix, and refer to the broker
addresses, topic, and optional partition number:

```
          
                  kafka:// $BROKERS / a_topic / 4
                  ─ ─ ─ ─  ─ ─ ─ ─    ─ ─ ─ ─   ┬
                     │        │          │
                                                │   partition
            prefix ─ ┘        │          │       ─ ─ number
                           broker      topic
                          addresses    name
```

* `kafka://$BROKERS/my_topic/4` - topic: `my_topic`, partition: 4
* `kafka://$BROKERS/bananas` - topic: `bananas` (one partition topic)
* `kafka://127.0.0.1/hello/4` - host: `127.0.0.1`, topic: `hello`, partition: 4

If no partition number is specified, it is assumed the topic has only one
partition - an error is raised if this is not the case.

## Examples

All examples use `$BROKERS` to refer to the set of broker addresses, and is
formatted as comma-delimited ip:port pairs:

```console
$ export BROKERS="10.0.0.1:4242,broker2.internal.network,10.0.0.3"
```

If the port is omitted, the Kafka default of 9092 is used.

### Copying Message Streams to Disk

Copy data from `my_topic` partition 42 to disk:

```console
$ ktool cp kafka://$BROKERS/my_topic/42 copy.kbin
```

### Replay Messages

To replay, copy a stream of messages on disk into a partition:

```console
$ ktool cp ./tests/fixture.kbin kafka://$BROKERS/my_topic/42
[*] opening dump file: ./tests/fixture.kbin
[*] connecting to kafka brokers: 127.0.0.1:60212
[*] read complete
[*] flushing writes
[*] write complete
[+] complete - copied 1 messages in 0 seconds (1913 msg/s)
```

Copying data to/from disk also respects the `--offset` or `--timestamp` flags to
specify a subset of messages to copy - try running `ktool cp --help`.

### Copy Between Topics/Clusters/Partitions

To copy between two different topics or Kafka clusters (or even between two
partitions of the same topic!) simply specify a Kafka source and destination:

```console
$ ktool cp kafka://$BROKERS/my_topic/42 kafka://localhost/backup`
```

In the above example, messages are copied from the cluster specified in
`$BROKERS`, reading messages from partition 42 of `my_topic` and the messages
are wrote to a localhost Kafka cluster, in the `backup` topic.

### Read Messages

To read messages and their payloads, use `ktool read`:

```console
$ ktool read ./backup.kbin
[*] opening dump file: ./tests/fixture.kbin
Message { topic: "topic", partition: 0, offset: 0, timestamp: Some(CreateTime(1663602628526)), headers: "NONE", key: Some("banana-key"), payload: Some("platanos") }
```

or to read from Kafka:

```console
$ ktool read kafka://$BROKERS/my_topic/24`
Message { topic: "topic", partition: 0, offset: 0, timestamp: Some(CreateTime(1663602628526)), headers: "NONE", key: Some("banana-key"), payload: Some("platanos") }
```

#### JSON Output

To read message envelopes as JSON, pass the `--json` flag:

```console
$ ktool read ./backup.kbin --json
{"topic":"topic","partition":0,"offset":0,"timestamp":{"CreateTime":1663602628526},"headers":null,"key":[98,97,110,97,110,97,45,107,101,121],"payload":[112,108,97,116,97,110,111,115]}
```

### Topic List / Metadata

To view topics, leaders, partitions, and various other cluster metadata:

```console
$ ktool metadata 127.0.0.1:60212
[+] metadata retrieved (src: 127.0.0.1:60212/0, id: 0)

Brokers:
	id 0 -> 127.0.0.1:60212

Topics:
	topic
		partition 0, leader 0, replicas [0] (in sync: [0])
	data-shared
		partition 0, leader 0, replicas [0] (in sync: [0])
	__consumer_offsets
		partition 0, leader 0, replicas [0] (in sync: [0])
		partition 1, leader 0, replicas [0] (in sync: [0])
		partition 2, leader 0, replicas [0] (in sync: [0])
```
