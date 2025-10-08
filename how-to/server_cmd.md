
# RapidStore Server Commands

## Commands to interact with the cache server

### Basic Connection Test
```bash
printf "hello" | nc localhost 6380
```
**Expected Response:** `Echo: hello`

### Ping Command
```bash
printf "PING" | nc localhost 6380
```
**Expected Response:** `PONG`

### Set Key-Value Pairs
```bash
# Set a simple key-value
printf "SET mykey myvalue" | nc localhost 6380

# Set a key with multiple words as value
printf "SET username john doe" | nc localhost 6380

# Set a key with spaces in value
printf "SET message hello world from rapidstore" | nc localhost 6380
```
**Expected Response:** Server processes silently (no response for SET)

### Get Values
```bash
# Get an existing key
printf "GET mykey" | nc localhost 6380
```
**Expected Response:** `myvalue`

```bash
# Get a non-existent key
printf "GET nonexistent" | nc localhost 6380
```
**Expected Response:** `(nil)`

```bash
# Get a multi-word value
printf "GET username" | nc localhost 6380
```
**Expected Response:** `john doe`

### Stop Server
```bash
printf "Close" | nc localhost 6380
```
**Expected Response:** Server shuts down gracefully

## Usage Notes

- Commands are case-insensitive for SET/GET operations
- Values can contain multiple words separated by spaces
- Missing keys return `(nil)`
- Any unrecognized command will be echoed back
- The server runs on port 6380 by default

## Example Session
```bash
# Start a session
printf "PING" | nc localhost 6380                    # -> PONG
printf "SET name alice" | nc localhost 6380          # -> (no response)
printf "GET name" | nc localhost 6380                # -> alice
printf "GET missing" | nc localhost 6380             # -> (nil)
printf "SET greeting hello world" | nc localhost 6380 # -> (no response)  
printf "GET greeting" | nc localhost 6380            # -> hello world
printf "unknown command" | nc localhost 6380         # -> Echo: unknown command
```