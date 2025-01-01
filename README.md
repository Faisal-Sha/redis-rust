# Build Your Own Redis Server
This challenge is to build your own Redis Server.

Redis is an in-memory data structure server, which supports storing strings, hashes, lists, sets, sorted sets and more.

The name Redis reflects the original goal to be a Remote Dictionary Server. Salvatore Sanfilippo the creator of Redis originally wrote it in just over 300 lines of TCL, you can see that original version in a gist he posted here.

Since the first version in 2009, Redis has been ported to C and released as open source. It’s also become one of the most widely used key-value / NoSQL databases.

The Challenge - Building A Redis Server
In this challenge you’re going to build your own lite version of Redis which will support the same operations as the original version of Redis.

Like all the challenges, you can tackle it in any programming language you like. Building a Redis clone in Rust was what inspired me to start Coding Challenges.

note
If you'd like help tackling this coding challenge I have a course available for purchase that guide you through it: Become a Better Software Developer by Building Your Own Redis Server available in Python and soon to be available in Go.

## Step Zero
In this introductory step you’re going to set your environment up ready to begin developing and testing your solution.

I’ll leave you to choose your target platform, setup your editor and programming language of choice. I’d encourage you to pick a tech stack that you’re comfortable doing both network programming (we’re building a server) and test driven development (TDD) with.

Once you’ve done that you might like to install Redis itself so you can use it’s CLI client as a test client for your implementation.

## Step 1
In this step your goal is to build the functionality to serialise and de-serialise Redis Serialisation Protocol (RESP) messages. This is the protocol used to communicate with a Redis Server. You may want to refer to the RESP protocol specification.

Redis uses RESP as a request-response protocol in the following way:

Clients send commands to a Redis Server as a RESP Array of Bulk Strings.
The server replies with one of the RESP types according to the command implementation.
In RESP, the first byte determines the data type:

For Simple Strings, the first byte of the reply is "+"
For Errors, the first byte of the reply is "-"
For Integers, the first byte of the reply is ":"
For Bulk Strings, the first byte of the reply is "$"
For Arrays, the first byte of the reply is "*"
RESP can represent a Null value using a special variation of Bulk Strings: "$-1\r\n" or Array: "*-1\r\n".

Now that we have the basics of the protocol, your challenge is to write the code required to serialise and de-serialise messages. My personal approach to this would be to use test-driven development (TDD) to build tests for some example messages, i.e.:

"$-1\r\n"
"*1\r\n$4\r\nping\r\n”
"*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n”
"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n”
"+OK\r\n"
"-Error message\r\n"
"$0\r\n\r\n"
"+hello world\r\n”
Plus some invalid test cases to test outside the happy path.

## Step 2
In this step your goal is to create the Redis Lite server. It should start up and begin listening for clients on the port: 6379.

When a client connects, you will want to accept the connection and then begin handling commands sent via the RESP protocol - using the serialiser and deserialiser you built in Step 1.

The simplest and most obvious command to implement is PING. When your Redis Lite server receives the command ping, it should response with PONG.

% redis-cli PING
PONG

After that you should implement the ECHO command so we can be terribly traditional and do “Hello World”:

% redis-cli ECHO "Hello World"
"Hello World"

Once you’ve done that you’re ready to proceed to Step 3 and begin making the server into a Remote Dictionary Server!

## Step 3
In this step your goal is to set and get the value of a key. You’re going to implement the core functionality of Redis.

To set a key in Redis we use the command SET. For this step we’re just going to implement the simplest version, i.e.:

% redis-cli set Name John
OK

We’ll ignore all the expiry options that Redis supports for now. Once a key has been set you’ll want to implement and test the related command GET.


% redis-cli get Name
"John"

What data structure you use to store the keys in is up to you. You can probably guess what Redis uses based on it’s name!

When you are implementing the commands don’t forget to read the specification for each command to check that you are returning the correct results to the client and that the results are correctly serialised according to the RESP protocol.

You might also like to use the Redis client library for your tech stack to build some automated tests for your server. Don’t forget to test error cases as well as the happy path.

## Step 4
In this step your goal is to handle multiple concurrent clients. If you haven’t already done so, now is the time to make your Redis Lite server handle concurrent clients. You have two basic options here; have one thread per client or use the asynchronous programming support offered by your chosen programming language. If you have time, give both a go, both have pros and cons.

You can use redis-benchmark as a client to check how you handle concurrency:

% redis-benchmark -t SET,GET -q
SET: 108225.10 requests per second, p50=0.223 msec
GET: 115606.94 requests per second, p50=0.215 msec

Note that by default this will create 50 client connections to your server. Check the full documentation for Redis Benchmark to see how else you can use it.

You should move on to Step 5 when your server can handle the above command without crashing. If the command is taking too long to run add -n 1000 onto the end to reduce the number of requests. Play around with the number if needs be.

Don’t focus on performance, just check your server can handle concurrency without any errors.

## Step 5
In this step your goal is to extend your support for the SET command so that you also accept the EX, PX EAXT PXAT expiry options. Refer to the SET command documentation for details of each option.

I strongly encourage you to automate the testing of these so you can set the values and get them both before and after the expiry to verify they are both set and expired correctly.

When you’re implementing your expiry think about how you can make it low overhead - Redis itself guarantees that expiry is a constant time operation. You’ll find details of that and how it achieves that on the documentation for the EXPIRE command.

## Step 6
In this step your goal is to add support for the commands:

EXISTS - check if a key is present.
DEL - delete one or more keys.
INCR - increment a stored number by one.
DECR - decrement a stored number by one.
LPUSH - insert all the values at the head of a list.
RPUSH - insert all the values at the tail of a list.
SAVE - save the database state to disk, you should also implement load on startup alongside this.
Once you’ve built and tested all these operations you will have built a lite version of Redis that has all the functionality of the first actual version of Redis!

## Step 7
In this step your goal is to use Redis Benchmark to performance test your server’s GET and SET performance.

I suggest you install Redis and run a server locally and see how that performs, then compare it to your solution.

For example, here’s how Redis performs on my Macbook:

redis-benchmark -t set,get, -n 100000 -q
SET: 110497.24 requests per second, p50=0.215 msec
GET: 117647.05 requests per second, p50=0.215 msec

And here’s how my Rust implementation does:

redis-benchmark -t set,get, -n 100000 -q
SET: 100603.62 requests per second, p50=0.247 msec
GET: 95785.44 requests per second, p50=0.263 msec

Not far off, but there’s several optimisations that Redis does that I’ve not implemented. If you’re not close to Redis’s performance, here’s some things to consider:

Are you using the most appropriate data structure.
Are you handling concurrent access to it in the most efficient way?
Congratulations however, you’ve built a lite version of Redis.
