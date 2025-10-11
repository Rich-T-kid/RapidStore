# This file outlines the commands that RapidStore supports through is TCP interface

### Notes
- this is more of a guide for how the server will interperate these commands.If your looking for how to interact with the server over tcp look at `server_cmd.md`
- commands are case insensitive I.E Set == SET == SeT
- all strings will be parsed out using spaces as the delimiter, so be carfull of spaces inbetween commands and args

# Server Commands
## Ping Command
```bash
SS Ping
```
**Ping server, responds with PONG**

## Echo Command
```bash
SS Echo Message
```
**Server responds with an echo of the message**
## Close Command
```bash
SS Close
```
**Gracfully shutdown server**

# Key commands

## Set Command 
```bash
Set myKey MyValue [TTL]
```
**set a key-value pair, option time to live can be provided in the form of seconds. If no ttl is provided its set to never expire**
## Get Command
```bash
Get myKey 
```
**grab the corresponding value of a key-value pair**
## Get all keys Command
```bash
GetAll
```
**grabs all the keys**
## Delete Command
``` bash
Del key
```
**delete a key-value pair**
## TTL Command
```bash
TTL key
```
**check the time to live (`ttl`) for a key** 
## Exist Command
```bash
Exists key
```
**check if a key exist**
## Type Command
```bash
Type key
```
**returns the type of the value of the corresponding key**
## Increment Command
```bash
Increment key
```
**increment the value of the corresponding key**
## Decrement Command
```bash
decrement key
```
**decrement the value of the corresponding key**
## Append Command
```bash
Append key suffix
```
**append a new suffix to a key**
## Multiple Key-Set Command
```bash
Mset key-value key-value
```
**create multiple key-value pairs**

# Data structure focused commands

## Hset Command
```bash
Hset key field value [TTL]
```
**set a field in hash table,option time to live can be provided in the form of seconds. If no ttl is provided its set to never expire**
## Hget Command

```bash
Hget key field
```

**get a field from a hash table**

## HgetAll Command

```bash
HgetAll key
```

**get all fields and values in a hash table**

## Hdel Command

```bash
Hdel key field
```

**delete a field from a hash table**

## Hexists Command

```bash
Hexists key field
```

**check if a field exists in a hash table**

---

## Lpush Command

```bash
Lpush key value
```

**push a value to the left (head) of a list**

## Rpush Command

```bash
Rpush key value
```

**push a value to the right (tail) of a list**

## Lpop Command

```bash
Lpop key
```

**pop a value from the left (head) of a list**

## Rpop Command

```bash
Rpop key
```

**pop a value from the right (tail) of a list**

## Lrange Command

```bash
Lrange key start stop
```

**get a range of elements from a list**

---

## Sadd Command

```bash
Sadd key member
```

**add a member to a set**

## Smembers Command

```bash
Smembers key
```

**list all members of a set**

## Srem Command

```bash
Srem key member
```

**remove a member from a set**

## Sismember Command

```bash
Sismember key member
```

**check if a member exists in a set**

## Scard Command

```bash
Scard key
```

**get the number of members in a set**

---

## Zadd Command

```bash
Zadd key score member
```

**add a member to a sorted set with a score**
## Zremove
```bash
Zremove key member
```
**remove a member**
## Zrange Command

```bash
Zrange key start stop [WithScores]
```

**get members in a sorted set by rank (lowest to highest)**
## Zrank Command

```bash
Zrank key member
```

**get the rank of a member in a sorted set**
## ZRevRank Command

```bash
Zrevrank key member
```

**get the rank of a member in a sorted set**
## Zscore Command

```bash
Zscore key member
```

**get the score of a member in a sorted set**



