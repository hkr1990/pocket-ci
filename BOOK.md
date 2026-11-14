# Pocket CI: HTTP, Streaming, and gRPC in Six Hours

## A practical textbook in 24 lessons

Learn the protocol, inspect the bytes, and design a streaming gateway.

This book follows one application from an ordinary HTTP request to a streaming
HTTP-to-gRPC gateway. Each lesson introduces a problem, explains the underlying
model, works through a concrete example, and asks you to apply the idea yourself.
The application stays small so that the networking remains visible.

You will not merely memorize that HTTP/2 is "faster" or that streaming "uses less
memory." You will learn when those statements are useful, when they are
misleading, and how to prove that an implementation has the behavior you intended.

### Contents

- [How to study this book](#before-you-begin)
- [Chapter 1: A request is not a build](#1-a-request-is-not-a-build)
  - [Lesson 1: Follow a request through the system](#lesson-1-follow-a-request-through-the-system)
  - [Lesson 2: Read HTTP messages as structured documents](#lesson-2-read-http-messages-as-structured-documents)
  - [Lesson 3: Investigate your first real exchange](#lesson-3-investigate-your-first-real-exchange)
- [Chapter 2: HTTP/1.1 versus HTTP/2](#2-http11-versus-http2)
  - [Lesson 4: Understand connections and multiplexing](#lesson-4-understand-connections-and-multiplexing)
  - [Lesson 5: Find the end of an HTTP body](#lesson-5-find-the-end-of-an-http-body)
  - [Lesson 6: Inspect transport framing yourself](#lesson-6-inspect-transport-framing-yourself)
- [Chapter 3: Unary, polling, and streaming](#3-unary-polling-and-streaming)
  - [Lesson 7: Choose between a document and a sequence](#lesson-7-choose-between-a-document-and-a-sequence)
  - [Lesson 8: Wait for change without missing it](#lesson-8-wait-for-change-without-missing-it)
  - [Lesson 9: Put streaming in the correct direction](#lesson-9-put-streaming-in-the-correct-direction)
- [Chapter 4: Implement HTTP streaming](#4-implement-http-streaming)
  - [Lesson 10: Design records before writing the handler](#lesson-10-design-records-before-writing-the-handler)
  - [Lesson 11: Turn a producer into an axum response body](#lesson-11-turn-a-producer-into-an-axum-response-body)
  - [Lesson 12: Write a client that survives arbitrary reads](#lesson-12-write-a-client-that-survives-arbitrary-reads)
  - [Lesson 13: Keep execution independent of observation](#lesson-13-keep-execution-independent-of-observation)
- [Chapter 5: gRPC is messages over HTTP/2](#5-grpc-is-messages-over-http2)
  - [Lesson 14: Move a process boundary without moving the truth](#lesson-14-move-a-process-boundary-without-moving-the-truth)
  - [Lesson 15: Match the RPC shape to the operation](#lesson-15-match-the-rpc-shape-to-the-operation)
  - [Lesson 16: Read protobuf as a compatibility contract](#lesson-16-read-protobuf-as-a-compatibility-contract)
  - [Lesson 17: Read through the final gRPC status](#lesson-17-read-through-the-final-grpc-status)
- [Chapter 6: Translate streams, not whole bodies](#6-translate-streams-not-whole-bodies)
  - [Lesson 18: Translate application meaning at a boundary](#lesson-18-translate-application-meaning-at-a-boundary)
  - [Lesson 19: Build a terminal-frame gate](#lesson-19-build-a-terminal-frame-gate)
  - [Lesson 20: Map errors before and after commitment](#lesson-20-map-errors-before-and-after-commitment)
  - [Lesson 21: Connect backpressure, budgets, and cancellation](#lesson-21-connect-backpressure-budgets-and-cancellation)
  - [Lesson 22: Forward an upload without inventing success](#lesson-22-forward-an-upload-without-inventing-success)
- [Chapter 7: Failures and your final check](#7-failures-and-your-final-check)
  - [Lesson 23: Break the observation on purpose](#lesson-23-break-the-observation-on-purpose)
  - [Lesson 24: Review the system like an implementer](#lesson-24-review-the-system-like-an-implementer)
- [Appendix A: Exercise answers](#appendix-a-exercise-answers)
- [Appendix B: Glossary](#appendix-b-glossary)
- [Appendix C: Troubleshooting the lab](#appendix-c-troubleshooting-the-lab)
- [Further reading](#optional-references-outside-the-six-hour-route)

## Before you begin

You are building a tiny CI service: submit a simulated build, inspect its state,
follow its logs, and eventually upload a source file. That small product is enough
to explain some surprisingly important networking ideas.

**Repository reality:** Pocket CI is at the design stage. The Rust executable only
prints `Hello, world!`; the proposed `client` commands are not implemented.
This book does not pretend otherwise.

There are two kinds of exercises:

- **Run now:** a Python-standard-library HTTP lab and `curl.exe`. These require no
  Rust dependencies and demonstrate actual bytes arriving incrementally.
- **Implementation workshop:** Rust sketches and protocol contracts for the
  intended Pocket CI. These are deliberately incomplete excerpts, not a finished
  application or copy-and-run crate. A full Rust implementation is beyond this
  six-hour reading lab.

The companion [HTTP lab](examples/http_stream_lab.py) is a framing probe, **not**
the Pocket CI engine. Every log request generates its own six-line demonstration.
It has no shared build, background execution, durable history, admission limits,
or upload endpoint. Do not copy its per-request execution model into Pocket CI.
Keep it on loopback and stop it when finished.

You need Python 3 and curl. Commands below use Windows PowerShell, from the
repository root. On systems where `python` is unavailable, try `py -3`.
Only the implementation workshops need a Rust toolchain.

### Who this book is for

You should be comfortable with variables, functions, loops, and running terminal
commands. Rust knowledge helps with the implementation listings, but you do not
need to know axum, tonic, protobuf, or HTTP/2 beforehand. Every protocol concept
used by the gateway is introduced before the gateway itself.

This is not a production deployment manual. It deliberately omits authentication,
TLS configuration, durable storage, scheduling, and execution of untrusted code.
Those are separate engineering projects, not details made safe by adding gRPC.

### Your six-hour route

The time budget is for the main explanations, worked examples, guided labs, and
one exercise per lesson. Sections marked **Optional deep dive** and the
appendices are reference material outside that route. Writing a complete Rust
service is a follow-on project, not an assignment hidden inside the reading time.

| Chapter | Lessons and individual budgets | Time |
| --- | --- | --- |
| 1. A request is not a build | 1: 10 min; 2: 15 min; 3: 15 min | 40 min |
| 2. HTTP/1.1 versus HTTP/2 | 4: 15 min; 5: 15 min; 6: 10 min | 40 min |
| 3. Unary, polling, and streaming | 7: 10 min; 8: 15 min; 9: 10 min | 35 min |
| 4. Implement HTTP streaming | 10: 15 min; 11: 20 min; 12: 20 min; 13: 10 min | 65 min |
| 5. gRPC is messages over HTTP/2 | 14: 10 min; 15: 10 min; 16: 15 min; 17: 10 min | 45 min |
| 6. Translate streams, not whole bodies | 18: 15 min; 19: 20 min; 20: 15 min; 21: 15 min; 22: 30 min | 95 min |
| 7. Failures and your final check | 23: 20 min; 24: 20 min | 40 min |
| **Total, including practice** | **24 lessons** | **360 min** |

Each numbered exercise has a worked answer in Appendix A. Try it before reading
the answer. The lab tasks have explicit expected observations so you can tell
whether you demonstrated the concept or merely ran a command without errors.

If you are short of time, finish the main route before attempting optional deep
dives. If you already know HTTP, still do the framing exercises: many streaming
bugs come from assumptions that appear reasonable until bytes arrive differently.

### The running case study

Imagine that your CLI submits build `session-a-1`. The server immediately accepts
it, runs checkout, compile, and test stages, and produces logs over several
seconds. Your CLI may disconnect and reconnect. Later you move the engine to a
worker without changing the public HTTP commands.

That story creates the questions that drive this book:

- How can a request finish before the work finishes?
- How can a response begin before all of its output exists?
- How does a client distinguish one log record from another?
- How can two different protocols preserve the same application meaning?
- How does either side know that a stream really completed successfully?

By the final lesson, you should be able to answer each question with a diagram,
a concrete wire example, and a test.

---

## 1. A request is not a build

**Chapter objectives**

By the end of this chapter, you can explain what a connection connects, read an
HTTP/1.1 message, and distinguish a successful request from a successful build.
You will also establish a repeatable local environment for the remaining labs.

### Lesson 1: Follow a request through the system

**Goal:** separate transport, protocol, and application lifetimes.

#### Three layers to keep separate

Imagine asking a restaurant to prepare dinner:

1. **Transport:** can your request reach the restaurant?
2. **Protocol:** did the restaurant accept and understand the order?
3. **Application:** did the meal turn out well?

Pocket CI has the same separation. TCP carries bytes. HTTP describes requests and
responses. The build application tracks `queued`, `running`, `succeeded`, or
`failed`.

A build with failing tests can have perfectly successful HTTP communication.
A disconnected network can prevent you from learning the result of a successful
build.

#### From a command to a handler

When you enter a CLI command, the operating system starts a client process. That
process prepares a request and asks a networking library to send it. For our local
HTTP/1.1 lab, the library connects a TCP socket to `127.0.0.1:3000`. The IP address
identifies loopback on your own machine; the port identifies a listening service.

The server process accepts the connection. Its HTTP implementation parses bytes
into a request before your application handler sees it. The handler works with a
method, path, headers, and body rather than interpreting individual TCP packets.
The response travels back through the same layers in reverse.

```text
Client process                                        Server process
command -> HTTP client -> socket -> TCP -> socket -> HTTP parser -> handler
        <- response    <-        <-     <-        <- serializer <-
```

TCP offers an ordered byte stream while the connection works. It does not expose
your function calls or preserve the boundaries between your writes. Writing
`hello` once does not promise that the receiver reads `hello` once. It may read
`he` and then `llo`, or combine it with subsequent bytes.

HTTP gives those bytes message structure. Your application adds meaning to those
messages. Keep those responsibilities separate and the later framing lessons
become much easier.

#### Worked example: three independent clocks

Suppose submission reaches the server at time zero:

```text
t=0 ms       Server receives POST /builds.
t=4 ms       Engine reserves capacity and creates the build.
t=6 ms       Client receives HTTP 202.
t=8000 ms    Simulated tests finish.
```

The HTTP operation took approximately six milliseconds. The build took about eight
seconds. A separate log observation might begin at two seconds and end at eight.
These are three different lifetimes: submission, execution, and observation.

If the log viewer closes at three seconds, only the observation should end. If
execution were tied to the handler's lifetime, a terminal window could
accidentally determine whether someone else's build completes.

**Common mistake:** assuming "asynchronous" means "streaming." The submission is
asynchronous at the application level because work continues after `202`. Its
response can still be one small, non-streaming JSON document.

**Exercise 1.** A CLI loses its network connection immediately after sending a
submission. List two possible server-side outcomes. Explain why the CLI cannot
choose between them from the disconnect alone.

### Lesson 2: Read HTTP messages as structured documents

**Goal:** identify what the method, target, headers, body, and status each say.

The proposed submission request looks like this:

```http
POST /builds HTTP/1.1
Host: 127.0.0.1:3000
Content-Type: application/json
Content-Length: 41

{"label":"learn-http","fail_tests":false}
```

The request line identifies the method, target, and protocol version. Headers
carry metadata. The blank line ends the headers. The body carries the JSON.
Here, the body is 41 UTF-8 bytes with no trailing newline. HTTP/1.1 lines use
CRLF line endings; this Markdown rendering does not show the carriage returns.

`Content-Type` describes the representation, not whether it streams.
`Content-Length` counts **bytes**, not characters.

The intended response is:

```http
HTTP/1.1 202 Accepted
Location: /builds/session-a-1
Content-Type: application/json

{"id":"session-a-1","state":"queued","version":1}
```

This response is abbreviated: the HTTP library also supplies appropriate body
framing. `Location` tells the client where to inspect the accepted resource.
`202` means **accepted for processing**, not "the tests passed."

Useful status codes in this project:

| Code | Interpretation |
| --- | --- |
| `200` | Snapshot or log stream accepted |
| `201` | Complete uploaded source registered |
| `202` | Build accepted |
| `204` | Long-poll wait expired; no response body |
| `400` | Invalid input or cursor |
| `404` | Unknown build |
| `429` | Admission capacity exhausted |
| `503` | Worker unavailable |

Do not automatically retry a lost `POST /builds` response: the build might already
exist. Without an idempotency mechanism, retrying can create a second build.

#### Methods describe operations, not implementation details

`GET /builds/session-a-1` asks for a representation of an existing resource.
`POST /builds` asks the server to process a submission and create work. Both can
call ordinary Rust functions internally; the method is a public contract, not
the name of a programming-language operation.

HTTP calls GET **safe** because the requested semantics should not change
application state. Logging access or updating a metric does not turn it into a
submission endpoint. Do not start a build as the intended effect of a status GET:
clients and intermediaries may repeat safe requests.

**Idempotent** means repeating the same operation has the same intended effect
as performing it once. It does not mean every response must have identical bytes.
GET is idempotent even though a build snapshot changes over time. An ordinary
build-creating POST is not: sending it twice may create two builds.

#### The header section is not the application body

A common beginner confusion is to put application metadata in whichever place is
convenient. Use the layers intentionally:

| Question | Appropriate location |
| --- | --- |
| Which operation and resource? | Method and request target |
| What media type is this body? | `Content-Type` |
| How is this HTTP message delimited? | HTTP framing |
| What label should this build have? | JSON body |
| Where can I inspect the accepted build? | `Location` response header |
| Did simulated tests pass? | Build snapshot or terminal log frame |

The server should reject unsupported request content types instead of guessing.
An HTML proxy error page is not an NDJSON log stream just because the CLI asked
for logs. The client must inspect response status and representation metadata
before choosing a parser.

#### Worked example: bytes are not characters

For ASCII text, one character is one UTF-8 byte. That coincidence makes examples
look simpler than the general case. A label containing a non-ASCII character
may use more bytes than visible characters. JSON escaping can change the count
again: a serializer may emit an escape sequence rather than the original character.

Therefore compute limits and `Content-Length` from the actual serialized bytes,
not the source string's apparent width. For the request above, 41 is correct only
for the exact body shown, without a trailing newline.

**Exercise 2.** A build status GET returns HTTP `200` with
`{"state":"failed"}`. Was the HTTP operation successful? Should a build-waiting
CLI report a successful build? Give separate answers.

### Lesson 3: Investigate your first real exchange

**Goal:** connect the message model to observable output.

#### Guided lab: inspect headers and a body

In terminal A:

```powershell
python .\examples\http_stream_lab.py
```

Leave it running. In terminal B:

```powershell
curl.exe --http1.1 -i http://127.0.0.1:3000/snapshot
```

Observe the status line, JSON content type, content length, blank line, and body.
This response returns one complete JSON document.

**Check yourself:** does HTTP `200` inherently mean a build passed?
No. It means this HTTP operation succeeded; inspect the application result.

#### Read the result in order

First locate `HTTP/1.1 200 OK`. That line describes this exchange; it says nothing
about a CI build. Next locate the JSON media type and `Content-Length`. Finally
locate the blank line and the JSON object. The Python server may add `Server` and
`Date` headers; those are normal and are not body fields.

Repeat the command with `-v` instead of `-i`:

```powershell
curl.exe --http1.1 -v http://127.0.0.1:3000/snapshot
```

Curl's verbose diagnostics include the outgoing request and connection details.
Lines prefixed with `>` describe sent headers; lines prefixed with `<` describe
received headers. Those diagnostic prefixes are curl annotations, not actual HTTP
bytes.

Now request an endpoint that does not exist:

```powershell
curl.exe --http1.1 -i http://127.0.0.1:3000/not-a-route
```

Expect HTTP `404` and a JSON error. Notice that an error response is still a valid
HTTP response. "The server returned an error" and "no server could be reached"
describe different events.

**Exercise 3.** Identify the request target, response status, response content
type, and body in the successful transcript. Then explain which of those would
be absent if the TCP connection could not be established.

#### Chapter summary

- A connection carries bytes between endpoints, not function calls.
- HTTP success and build success belong to different layers.
- A build's lifetime must be independent of any one observation request.
- Headers describe the HTTP representation and exchange; the body carries the
  application document.

---

## 2. HTTP/1.1 versus HTTP/2

**Chapter objectives**

You will distinguish semantics from wire format, explain why HTTP/2 helps with
concurrent exchanges, and inspect unknown-length HTTP/1.1 response framing.
You will also learn what HTTP/2 does not fix.

### Lesson 4: Understand connections and multiplexing

**Goal:** explain the version difference without treating HTTP/2 as magic.

#### Same semantics, different wire format

Both versions have methods, headers, status codes, requests, and responses.
HTTP/2 changes how those messages travel, not the meaning of `GET` or `202`.

| Property | HTTP/1.1 | HTTP/2 |
| --- | --- | --- |
| Wire structure | Text start line/headers, framed body | Binary frames |
| Concurrent exchanges | Usually multiple connections | Multiplexed streams on a connection |
| Unknown-length body | Often chunked transfer encoding | DATA frames |
| Header compression | No HPACK | HPACK |
| Protocol flow control | TCP flow control | Connection and stream flow control, plus TCP |
| Application streaming | Yes | Yes |

HTTP/1.1 supports persistent connections. It is not necessarily "one TCP
connection per request." Pipelining exists, but response ordering makes it a poor
substitute for HTTP/2 multiplexing and it is not commonly used by modern clients.

HTTP/2 streams interleave frames so one response need not finish before another
response sends data on the same connection. But HTTP/2 normally runs over TCP:
loss of a TCP segment can still delay all streams on that connection.
Multiplexing does not remove TCP-level head-of-line blocking.

TLS and HTTP version are separate ideas. HTTPS usually negotiates HTTP/2 through
ALPN; cleartext HTTP/2 is possible with appropriate client/server configuration.
Our small Python lab supports **HTTP/1.1 only**. It cannot demonstrate HTTP/2
negotiation or real HTTP/2 frame captures.

#### Worked example: one long response and one short response

Imagine a client following logs while also requesting a snapshot. On an HTTP/1.1
connection, a long-lived response occupies its response sequence. Sending another
request on that connection does not let the next response's bytes jump into the
middle of the log response. In practice the client uses another pooled connection.

HTTP/2 assigns each exchange a stream identifier. The server can interleave frames
for a log stream and a snapshot stream:

```text
One HTTP/2 connection:
  HEADERS(stream 1, logs)
  DATA(stream 1, log bytes)
  HEADERS(stream 3, snapshot)
  DATA(stream 3, snapshot bytes, END_STREAM)
  DATA(stream 1, more log bytes)
```

The ordering above is illustrative, not a required scheduling policy. Its
important property is that the snapshot can finish without waiting for the log
response to finish. The receiver uses stream identifiers to route frames to the
right response body.

This can reduce connection overhead and improve concurrent use of a connection.
It does not guarantee every HTTP/2 request is faster. Your application might
still wait on a lock, collect a whole response, or contend for CPU.

#### Different meanings of "stream"

Throughout this book, distinguish three things:

1. A **TCP byte stream** is the ordered transport abstraction.
2. An **HTTP/2 stream** is one multiplexed HTTP exchange.
3. An **application stream** is a sequence of useful records over time.

An HTTP/2 stream can contain one ordinary JSON snapshot. An HTTP/1.1 response can
contain a long application stream. The shared word does not make these concepts
interchangeable.

**Optional deep dive: flow control is not scheduling.** HTTP/2 windows constrain
how much DATA can be sent before the peer grants more credit. They do not decide
which application task gets CPU time or guarantee a particular fairness policy.
If a gateway eagerly drains an upstream stream into its own unlimited queue,
protocol flow control cannot bound that queue for you.

**Exercise 4.** A gateway serves live logs correctly over HTTP/1.1 but a colleague
claims it must be upgraded to HTTP/2 before it can stream. Explain the mistake.
Then name one genuine benefit HTTP/2 could provide.

### Lesson 5: Find the end of an HTTP body

**Goal:** distinguish HTTP message boundaries from application record boundaries.

#### Chunked transfer is transport framing

For an unknown-length HTTP/1.1 response, a server can send:

```text
Transfer-Encoding: chunked

3\r\n
abc\r\n
2\r\n
de\r\n
0\r\n
\r\n
```

The chunk lengths are hexadecimal. This body carries `abcde`; the hexadecimal
lengths and delimiters are not application data. The zero-length chunk ends this
HTTP message; it need not close the persistent TCP connection.

In HTTP/2, body bytes travel in DATA frames, and an END_STREAM flag ends that
direction of the stream. **Do not send `Transfer-Encoding: chunked` over HTTP/2.**

Neither HTTP/1.1 chunks nor HTTP/2 DATA frames define your JSON message boundaries.
One record might span multiple transport chunks; several records might arrive in
one client read.

#### Why a length is not always available

For the snapshot response, the server can serialize the complete JSON object
before sending headers. It knows the exact byte count and can send
`Content-Length`.

For live logs, future output does not exist yet. The server could wait, collect
everything, and compute a length, but that defeats live observation. Instead it
chooses framing that allows the body to grow until completion.

HTTP/1.1 also permits some response bodies to be delimited by closing the
connection. That prevents reuse and makes truncation harder to distinguish from
intended completion. Chunked framing provides an explicit message terminator.
We still need an application `end` record because HTTP completion alone cannot
state the build result or confirm our application protocol was followed.

#### Worked example: two independent boundaries

Suppose an application record is:

```text
{"seq":1}\n
```

Those ten bytes could be transported as two HTTP/1.1 chunks:

```text
4\r\n{"se\r\n6\r\nq":1}\n\r\n0\r\n\r\n
```

The HTTP implementation removes the chunk lengths and CRLF delimiters. Your
application then sees the ten-byte JSON line and uses LF to find its own record
boundary. Neither parser can replace the other.

The same record could also be part of a larger HTTP chunk containing two lines.
Your client must work for all of these arrangements. Choosing a convenient
arrangement on the server is not a contract with the network.

**Exercise 5.** An HTTP library delivers `{"seq":` in one body read and `1}\n` in
the next. Which part of the client must preserve the first bytes? Why is calling
a JSON parser independently on each read incorrect?

### Lesson 6: Inspect transport framing yourself

**Goal:** recognize what curl removes and what your application must still parse.

#### Guided lab: separate transfer framing from application framing

```powershell
curl.exe --http1.1 -N -i http://127.0.0.1:3000/logs
curl.exe --http1.1 --raw -N -i http://127.0.0.1:3000/logs
```

The first command normally removes chunked transfer framing for you.
The second exposes chunk sizes and terminators. `-N` disables curl's output
buffering; it does not switch on streaming at the server or eliminate OS buffers.

The lab deliberately writes one JSON record per HTTP chunk. That is a teaching
choice, **not a guarantee your client may rely on**.

**Check yourself:** do you need HTTP/2 for live logs? No. HTTP/1.1 can stream.

#### Explain what you observed

In raw output, look for a short hexadecimal line before each JSON line. After the
terminal application record, find the HTTP zero chunk. These are separate ends:
the JSON `end` belongs to the CI observation protocol; the zero chunk belongs to
HTTP/1.1 message framing.

Now run:

```powershell
curl.exe -V
```

This reports your curl build's capabilities. An `HTTP2` feature, if present, says
the client supports HTTP/2. It does not say the Python server supports it, nor does
it prove any particular request negotiated it.

We deliberately do not use a public Internet endpoint to "prove" the local
application works over HTTP/2. That would demonstrate someone else's server,
not the Pocket CI worker. A future HTTP/2 integration test needs a real local
HTTP/2-capable server and confirmation of the negotiated version.

**Exercise 6.** Raw curl output shows an `end` JSON record followed by `0`.
Identify which layer owns each. Would an HTTP/2 response use the same zero-chunk
terminator?

#### Chapter summary

HTTP/2 changes framing and multiplexing, not the meaning of your endpoints.
Both versions can carry application streams. Never use packet, DATA-frame,
transfer-chunk, and JSON-record boundaries as interchangeable concepts.

---

## 3. Unary, polling, and streaming

**Chapter objectives**

You will choose the interaction pattern that fits a product requirement and
explain why status snapshots, log events, and upload bytes need different
contracts.

### Lesson 7: Choose between a document and a sequence

**Goal:** classify an operation by what the application needs to observe.

#### "Unary HTTP" is an interaction pattern

Unary is formal gRPC terminology. For an ordinary HTTP API, we often use it
informally to mean one request followed by one complete result, such as a build
snapshot.

HTTP bodies are already byte streams. A "unary" JSON response can arrive across
many packets. Calling it unary describes the **application result**, not a
promise that it fits in one network read.

Conversely, an HTTP log stream still has one response, usually with one status
line and one header section. Its body contains multiple application records
delivered over time. Each log line is not a new HTTP response.

#### Choose the interaction to match the question

| Question | Pattern | Pocket CI endpoint |
| --- | --- | --- |
| What is the state now? | Unary snapshot | `GET /builds/{id}` |
| Has it changed yet? | Periodic polling | Repeated snapshot requests |
| Wait until it changes | Long polling | `GET /builds/{id}/updates?after=V` |
| Show output as it happens | Response streaming | `GET /builds/{id}/logs?after=S` |
| Receive a large file incrementally | Request streaming | `POST /sources` |

#### Worked example: the same build, three user experiences

Alice runs a status command once. She wants a document that says `running`.
Bob wants his terminal to update whenever the stage changes. He needs newer
snapshots, not necessarily every intermediate snapshot. Carol is investigating a
test failure. She needs ordered log records and an explicit indication if some
history is unavailable.

All three refer to the same build, but "use streaming everywhere" is not a useful
design rule. A snapshot is easy to cache in a local variable, render, test, and
retry. A stream adds incremental parsing, observation lifetime, and finalization
rules. Use that complexity when partial results are useful before completion.

Also distinguish **large** from **streaming**. A large file can be one application
result delivered incrementally. A sequence of tiny log records can run for hours.
Body size and result cardinality are independent dimensions.

**Exercise 7.** Choose unary, long polling, response streaming, or request
streaming for: reading current status, waiting for a changed stage, tailing logs,
and sending an input bundle. State what would count as completion in each case.

### Lesson 8: Wait for change without missing it

**Goal:** design a cursor-based long poll with explicit timeout behavior.

For a 60-second build, polling every second makes about 60 requests per observer.
Long polling sends a request and holds its response until a newer snapshot exists
or the wait expires. It can reduce idle requests, but is still one final response
per wait, not a sequence of live log messages.

Long polling needs a version cursor. If the client has version `4`, it asks for
something newer than `4`. A normal timeout becomes `204`, and the client asks
again. Snapshots can skip intermediate states; they are not an event archive.

Avoid the check-then-subscribe race:

```text
Wrong: read version -> state changes -> subscribe -> wait forever for next change
Right: subscribe -> read current version -> compare -> wait only if necessary
```

Tokio `watch` fits latest-value snapshots. It does not retain every log line.

#### Worked example: a version cursor is not a timestamp

Suppose the current snapshot has version 7:

| Client's `after` | Server behavior |
| --- | --- |
| `5` | Return the current version 7 immediately |
| `7` | Wait for a newer version or the requested wait deadline |
| `9` | Reject the future cursor with `400` |

A cursor is a comparison token within a particular build. It is not a count of
seconds or a promise to replay versions 6 and 7 separately. If the engine rapidly
advances from version 7 to 10, returning version 10 satisfies "newer than 7."

The planned endpoint requires `after` and accepts waits from 1 ms to 30 seconds,
defaulting to 10 seconds. An equal cursor follows the same timeout contract even
if the build is already terminal. The client normally avoids that unnecessary
request by stopping when it sees terminal state.

#### Why subscribe first?

An asynchronous notification is a wakeup mechanism, not the state itself. A
correct waiter subscribes, reads a coherent snapshot, and then compares versions.
If a transition happens before the read, the read sees it. If it happens after,
the subscription notices it. A loop rechecks the predicate after every wakeup.

Tokio's `borrow_and_update()` helps establish what the receiver has observed.
Clone the snapshot while borrowing, release the borrow, and only then await
`changed()`. A watch borrow held across an await can unnecessarily block writers.

After worker extraction, this operation becomes a delayed **unary** RPC. The
gateway should not hide a second periodic polling loop behind the HTTP long
poll. Give the RPC a little more time than the requested application wait so an
ordinary timeout maps to `204`, not a gateway timeout.

**Exercise 8.** A client has version 3. The server moves to version 4 between a
state check and subscription. Explain the failure in the wrong algorithm and why
subscribing first closes that window.

### Lesson 9: Put streaming in the correct direction

**Goal:** distinguish uploads from live responses and from bidirectional RPCs.

#### Request streaming is a different direction

Live logs use one small request and a response body that grows over time.
Uploads use a request body arriving over time and one final receipt.

HTTP can support other exchange patterns, but Pocket CI deliberately uploads
first and submits the build second. It does not need bidirectional application
streaming, WebSockets, server-sent events, or gRPC-Web.

**Recall:** choose one mechanism for each: current status, continuous logs,
source bytes. Explain why a snapshot channel cannot promise lossless logs.

#### Direction is part of the API contract

An HTTP request and an HTTP response each have a body. "Streaming HTTP" does not
say which body you mean. Before choosing a library helper, complete this sentence:
"The producer of incremental data is ____; the consumer is ____."

For logs, the worker is the producer and the terminal is the consumer. For
uploads, the file-reading CLI is the producer and worker storage is the consumer.
The gateway stands in the middle and must remain incremental in either direction.

A bidirectional gRPC method permits independent message sequences in both
directions during one RPC. That capability does not mean every upload needs it.
Pocket CI needs one receipt only after the input is complete, so client streaming
is enough. Simpler interaction patterns make completion and cancellation easier
to reason about.

**Exercise 9.** Someone proposes returning upload progress as JSON in the same
response while the request is still arriving. Explain why this changes the
interaction contract and why it is unnecessary for the sequential upload-then-
submit workflow.

#### Chapter summary

A snapshot answers "what now," a long poll answers "has it changed," a log stream
answers "what happened," and an upload carries input. Choosing among them is an
application-design decision, not a choice between HTTP/1.1 and HTTP/2.

---

## 4. Implement HTTP streaming

**Chapter objectives**

You will design a line-framed application protocol, understand axum's streaming
body model, implement incremental client parsing, and separate a build producer
from its observers. This is the bridge from protocol vocabulary to program
structure.

### Lesson 10: Design records before writing the handler

**Goal:** define what a client must receive to interpret a log observation.

#### Give your bytes application boundaries

Pocket CI uses newline-delimited JSON, or NDJSON:

```json
{"type":"log","seq":1,"stage":"checkout","text":"Checking out source"}
{"type":"log","seq":2,"stage":"compile","text":"Compiling"}
{"type":"end","state":"succeeded","last_seq":2}
```

Each record is a complete JSON value followed by LF. A JSON serializer escapes
newlines inside strings, so those do not act as record delimiters.

The production design also has:

```json
{"type":"gap","from":3,"to":9}
{"type":"error","code":"worker_unavailable","message":"Worker connection was lost"}
```

`gap` reports intentionally expired history. `error` reports an unsuccessful
observation. `end` reports a complete build result, including a normal failed
build. These have different meanings.

#### Work through a reconnect

Suppose the terminal has processed log sequence 12 and then loses its connection.
It reconnects with `after=12`, meaning "I have already consumed everything through
12; send something later." If retained history begins at 13, the observer simply
continues. If retained history begins at 20, the worker first sends:

```json
{"type":"gap","from":13,"to":19}
{"type":"log","seq":20,"stage":"test","text":"Resuming retained output"}
```

The client should visibly report the gap rather than presenting the later lines
as an uninterrupted transcript. After processing the gap it can advance its
cursor to 19, then to 20 after processing the next line.

Advance the cursor only after handling the record. Advancing before processing
creates a failure window: the client could crash or fail to print a line and
subsequently ask the server to skip it.

This design is bounded replay, not durable delivery. A restart can remove the
whole build. A disconnected observer can miss expired history. The protocol is
honest about both limitations.

#### Build a frame vocabulary, not a string convention

Without a tagged frame type, a client might look for a text line such as
`BUILD DONE`. But a build could legitimately print that text, and a human-readable
message can change. Explicit fields separate machine meaning from display text.

In Rust, the public representation naturally fits a tagged enum:

```rust
#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HttpFrame {
    Log { seq: u64, stage: String, text: String },
    Gap { from: u64, to: u64 },
    End { state: String, last_seq: u64 },
    Error { code: String, message: String },
}
```

This excerpt describes output shape, not complete validation. A real domain type
can restrict `state`, enforce valid sequence relationships, and bound text before
serialization. Serialized NDJSON also needs its own 8 KiB limit: escaped JSON
text can be longer than the original string.

**Exercise 10.** A client processed sequence 8, but the worker retains only
15..30. Write the gap record and the first log sequence it should expect. What
should the terminal tell the user?

### Lesson 11: Turn a producer into an axum response body

**Goal:** identify the exact point where collecting defeats incremental output.

#### Server: yield records, not an accumulated body

The following is an **axum implementation sketch**, not a complete handler:

```rust
use axum::{
    body::Body,
    http::{header, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde::Serialize;

fn ndjson_response<S, T>(frames: S) -> impl IntoResponse
where
    S: Stream<Item = T> + Send + 'static,
    T: Serialize + 'static,
{
    let encoded = frames.map(|frame| {
        let mut bytes = serde_json::to_vec(&frame)?;
        bytes.push(b'\n');
        Ok::<Bytes, serde_json::Error>(Bytes::from(bytes))
    });

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-ndjson"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::from_stream(encoded),
    )
}
```

This illustrates transport wiring only. The caller must enforce frame-size bounds,
map domain/upstream errors, and produce the correct final frame. Serialization
failure here becomes a body failure, never a successful `end`.

Do not collect the stream into a `Vec`, serialize all output, then return it.
Do not manually set chunked transfer encoding in axum; its HTTP implementation
chooses protocol-appropriate framing. Do not invent a total `Content-Length` for
an unknown-length stream.

Avoid compression while learning: compressor and intermediary buffers can delay
visible records. A successful `yield` is not proof the client has already seen it.

#### Read the Rust listing as a data pipeline

The `frames` argument is not a completed collection. It is an asynchronous source
whose next item may not exist yet. `map` describes how to encode each future item;
it does not loop through the entire build immediately. `Body::from_stream`
connects that source to the HTTP response body.

When the HTTP implementation asks for the next body item, the stream either
produces bytes, reports that it is not ready yet, or completes. The async runtime
can run other work while it waits. That is why a long-lived log response does not
need to occupy a CPU core in a busy loop.

The distinction between **constructing a stream** and **consuming a stream**
matters. A function can return a response promptly even though the body will take
seconds to produce. A `collect().await` inserted before the return changes the
operation: now it waits for completion before any body is available.

#### Follow one record from engine to socket

```text
Engine appends LogLine to bounded history.
Observer wakes and copies one available frame.
Serializer produces JSON bytes plus LF.
Body yields those bytes to the HTTP implementation.
HTTP implementation adds version-appropriate framing.
Socket and OS send bytes when permitted.
Client HTTP library removes transport framing.
Client application parser finds LF.
Terminal prints the decoded record.
```

Each boundary can introduce delay. If logs arrive in batches, investigate the
pipeline rather than assuming the HTTP version is wrong. The engine may produce
in batches, the gateway may collect, a proxy may buffer, or the terminal may not
flush.

#### Error handling belongs in the design

The small encoder's serialization error becomes a failed response body. That is
safe in the sense that it does not fabricate success, but it does not deliver a
friendly application `error` record. Later, the gateway will explicitly translate
known worker failures into a serializable public error frame.

Prevalidate fields and sizes as early as practical. Still preserve a final
transport-failure path: sockets can fail even when your application data is valid.
No handler can promise that its final explanatory error reaches a disconnected
client.

**Exercise 11.** Compare these two designs:

```text
A: worker stream -> collect all frames -> serialize all frames -> HTTP body
B: worker stream -> serialize next frame -> yield -> repeat
```

Which can produce a log before the build finishes? Which retains memory
proportional to all observed output? Explain rather than just choosing a letter.

### Lesson 12: Write a client that survives arbitrary reads

**Goal:** preserve partial records and reject incomplete success.

#### Client: parse records, not transport chunks

A reqwest CLI should use `response.bytes_stream()`, not
`response.text().await` or `response.json().await` on the entire live response.

The parser algorithm is:

```text
Check HTTP status and NDJSON content type before parsing.
For each received byte:
    If LF:
        parse the pending bytes as one UTF-8 JSON frame
        validate its type and required fields
        process the frame
        clear pending bytes
    Otherwise:
        append the byte, rejecting a record above 8 KiB
At EOF:
    reject unfinished record bytes
    reject a stream without a valid end frame
```

The byte-at-a-time description makes the invariant obvious; production code can
scan chunks for LF efficiently. Bound unfinished data **while appending**, not
only after finding a newline. Otherwise a peer can send an unlimited record.

Decode UTF-8 only after assembling the complete record. A Unicode character can
be split across network reads. Treat bytes after `end`, duplicate terminal
records, invalid JSON, and unknown required frame variants as protocol errors.
Do not silently skip a broken record and print success.

#### Guided lab: a bounded incremental client

This PowerShell command runs a small Python client without installing packages:

```powershell
@'
import json
import urllib.request

ended = False
with urllib.request.urlopen("http://127.0.0.1:3000/logs", timeout=15) as response:
    if response.headers.get_content_type() != "application/x-ndjson":
        raise RuntimeError("unexpected content type")
    while True:
        line = response.readline(8193)
        if not line:
            break
        if ended:
            raise RuntimeError("data after end")
        if len(line) > 8192 or not line.endswith(b"\n"):
            raise RuntimeError("oversized or incomplete record")
        frame = json.loads(line)
        if frame["type"] == "log":
            print(frame["seq"], frame["text"], flush=True)
        elif frame["type"] == "end":
            if frame["state"] not in ("succeeded", "failed"):
                raise RuntimeError("invalid terminal state")
            print("Build result:", frame["state"], flush=True)
            ended = True
        else:
            raise RuntimeError(f"unexpected frame: {frame}")
if not ended:
    raise RuntimeError("EOF without end")
'@ | python -
```

You should see line 1 immediately, then another line roughly every second, and
the terminal frame about six seconds after the first line. This probe client
handles only the lab's `log` and `end` frames. A real CLI also validates schemas
and sequences, handles `gap`/`error`, tracks its cursor, and reports failed builds
with an appropriate process exit code.

#### Worked example: walk the parser state

Imagine the HTTP library delivers these three chunks:

```text
Chunk A: {"type":"log","seq":
Chunk B: 1,"stage":"test","text":"ok"}\n{"type":
Chunk C: "end","state":"succeeded","last_seq":1}\n
```

After A, there is no newline, so the parser retains the bytes and emits nothing.
During B, it encounters a newline, parses one complete log, clears the consumed
bytes, and retains the beginning of the next record. During C, it completes the
terminal record.

The parser's state is therefore not "current HTTP chunk." It is "unfinished
application record plus protocol progress." The protocol progress includes the
last processed sequence and whether a terminal record has already occurred.

#### Guided lab: prove independence from chunk boundaries

The live lab happens to send convenient chunks. This deterministic exercise is
stronger evidence about framing. Run it in terminal B; it needs no server:

```powershell
@'
import json

def decode_records(chunks):
    pending = bytearray()
    for chunk in chunks:
        for byte in chunk:
            pending.append(byte)
            if len(pending) > 8192:
                raise ValueError("record exceeds 8 KiB including LF")
            if byte == 10:
                yield json.loads(pending)
                pending.clear()
    if pending:
        raise ValueError("unfinished record at EOF")

sample = '{"text":"caf\u00e9"}\n{"text":"next"}\n'.encode("utf-8")
expected = [{"text": "caf\u00e9"}, {"text": "next"}]
for cut in range(len(sample) + 1):
    assert list(decode_records([sample[:cut], sample[cut:]])) == expected
assert list(decode_records(bytes([byte]) for byte in sample)) == expected

for invalid in [b'{"text":"partial"}', b"x" * 8193, b"\n"]:
    try:
        list(decode_records([invalid]))
    except ValueError:
        pass
    else:
        raise AssertionError("invalid input was accepted")

print("PASS: every two-part split, byte-at-a-time UTF-8, and invalid records")
'@ | python -
```

This is a framing exercise, not the complete CI protocol decoder. It deliberately
uses `list` in the assertions to compare a tiny fixed test sample; the generator
itself yields records incrementally. Production network code must not collect an
unbounded response this way.

The sample contains a multi-byte UTF-8 character constructed with a Python Unicode
escape. Testing every split includes a split inside that character. The JSON
decoder sees only complete records, so partial UTF-8 does not cause premature
decoding errors.

#### From framing to a complete CLI

A robust CLI adds a second layer after line decoding:

| Layer | Responsibility |
| --- | --- |
| HTTP client | Status, content type, transfer completion, connection errors |
| NDJSON decoder | Bounded records and UTF-8/JSON decoding |
| Frame validator | Required fields, known variants, legal sequence progression |
| Workflow controller | Reconnect policy, final build result, exit status |
| Terminal renderer | Human-readable output without hiding gaps or errors |

These can be small functions rather than a framework. The separation is useful
because each failure has a clear home. A malformed record is not a failed build;
a failed build is not a malformed record.

**Exercise 12.** Your framing decoder accepted a complete `end` record and then
receives another log. Should the workflow controller ignore it, print it, or
reject the observation? Explain the protocol invariant being protected.

### Lesson 13: Keep execution independent of observation

**Goal:** design shared state without slow readers blocking the build.

#### Shared execution and bounded observation

Unlike the lab, real builds run in service-owned Tokio tasks. Dropping a log
request must not abort the build. The worker stores a bounded history, proposed
as 128 lines per build, and notifies observers when new data exists.

For each observer: subscribe before inspecting history; copy the next bounded
record under a short lock; release the lock; yield or wait. Never hold a mutex
guard or a `watch` borrow across `.await` or a stream yield.

If a slow observer falls behind, report an inclusive gap, then continue with
retained history. Do not let one terminal window create an unlimited queue or
block the simulated build.

**Workshop:** draw how `after=5` behaves if retained sequence numbers are 10..20.
Answer: emit gap 6..9, then logs 10..20, then wait or emit `end` if terminal.

#### Worked example: a reader falls behind

Assume a log ring retains three records. The engine emits sequences 1, 2, and 3.
An observer consumes only sequence 1 before becoming slow. The engine continues
with 4 and 5, so retained history is now 3, 4, 5.

When the observer resumes, its cursor is 1. It must see a gap for 2, then receive
3 onward. Nothing requires the engine to wait for the slow terminal.

A watch notification is just a hint that the ring may have changed. The ring and
its sequence counters decide what the observer should send. Counting wakeups is
not equivalent to counting log records because notifications can coalesce.

#### Completion is also a state change

Suppose the final log is appended and an observer drains it. If the build becomes
terminal afterward without waking the observer, the observer may wait forever for
another log that will never come. Publish completion consistently and wake log
observers on the terminal transition as well as on new lines.

Do not yield while holding the build-record lock. A yield can suspend until the
reader wants more bytes. If the lock remains held, observation can accidentally
stop the producer from appending the next line.

**Exercise 13.** A ring retains 3, 4, and 5, and a reader last consumed 1. Write
the complete remaining application-frame sequence if the build has completed
successfully at 5.

#### Chapter summary

NDJSON gives application boundaries; HTTP gives transport framing. A streaming
server yields bounded records as they become available. A streaming client retains
only incomplete records, validates progression, and requires explicit completion.
The build producer remains independent of every observer.

---

## 5. gRPC is messages over HTTP/2

**Chapter objectives**

You will explain what gRPC adds beyond HTTP/2, choose RPC shapes for Pocket CI,
read a protobuf contract, and distinguish receiving a message from successfully
finishing an RPC.

### Lesson 14: Move a process boundary without moving the truth

**Goal:** extract a worker without creating two competing state owners.

#### What changes when the engine becomes a worker?

Initially, axum handlers call the build engine directly. Later, the engine and its
state move into a tonic worker:

```text
CLI -- HTTP/JSON --> axum gateway -- gRPC/protobuf --> tonic worker
                                                        |
                                                  build state
```

Keep one authoritative owner: the worker. The gateway translates protocols, not
maintains a competing build registry.

gRPC gives you named methods, generated message types, protobuf serialization,
streaming RPC shapes, and structured final status. Native gRPC normally uses
HTTP/2, but it is not simply "JSON HTTP with HTTP/2 enabled."

#### What used to be a function call becomes a fallible remote call

Before extraction, the handler can call a domain function in its own process.
After extraction, the gateway must serialize a request, send it over a connection,
wait for the worker, and interpret the outcome. The worker may be unavailable even
when the HTTP gateway is healthy.

That introduces uncertainty, not just extra latency. If the gateway loses a
submission response, it may not know whether the worker accepted the build.
Changing transports does not solve duplicate-submission semantics.

The engine should continue owning admission, identifiers, snapshots, logs, and
source receipts. The gateway should not independently decide that a build is
`running` because it recently observed a log line. That would be a second,
potentially contradictory model.

#### Worked example: restart the gateway

In the final architecture, restarting only the gateway should not erase a
worker-owned build. Clients lose observations and reconnect, but execution remains
in the worker. Restarting the worker is different: this learning design is
ephemeral, so old builds disappear.

Generation-prefixed build identifiers prevent a fresh worker from accidentally
reusing an old process's identifier for an unrelated build. A stale ID should
return not found, not silently point to a new result.

Reuse reqwest clients and tonic channels instead of reconnecting per message.
Do not put the entire tonic client behind a mutex held for the lifetime of a log
stream; that could serialize unrelated requests behind a long observer.

**Exercise 14.** The gateway caches `running` and the worker says `succeeded`.
Which is authoritative? Why is it safer for this project not to maintain a
second gateway-owned build registry at all?

### Lesson 15: Match the RPC shape to the operation

**Goal:** understand message cardinality independently of execution time.

#### The four RPC shapes

| Shape | Request messages | Response messages | Example |
| --- | --- | --- | --- |
| Unary | One | One | Create or inspect a build |
| Server streaming | One | Many | Follow logs |
| Client streaming | Many | One | Upload source |
| Bidirectional streaming | Many | Many | Not needed here |

A unary RPC can wait several seconds before returning. "Unary" does not mean
instantaneous, synchronous execution, or a fresh connection per call.

#### Map the complete proposed worker service

| RPC | Why this shape fits |
| --- | --- |
| `CreateBuild` | One configuration produces one accepted snapshot |
| `GetBuild` | One identifier produces one current snapshot |
| `WaitBuildUpdate` | One cursor eventually produces one newer snapshot or wait-expired result |
| `StreamBuildLogs` | One cursor produces a sequence of log/gap/end messages |
| `UploadSource` | A sequence of source frames produces one receipt |

The delayed wait deserves particular attention. Waiting is about time; streaming
is about message cardinality. A unary wait can take ten seconds. A server stream
can send three messages in one millisecond. Neither name tells you how long an
operation lasts.

#### Worked example: two independent terminal conditions

For `StreamBuildLogs`, the last application message describes a terminal build
state. The RPC then finishes with gRPC status. Those are two distinct facts:

```text
Application result: simulated tests failed.
RPC result: every promised message was delivered successfully.
```

A client can legitimately report both "build failed" and "log observation
completed normally." That is clearer than calling both conditions "an error."

**Exercise 15.** A wait RPC holds for ten seconds and returns one snapshot.
Is it unary or server streaming? An upload sends 100 chunks and gets one receipt:
which shape is that?

### Lesson 16: Read protobuf as a compatibility contract

**Goal:** understand fields, variants, and validation across a process boundary.

#### A minimal log contract

This workshop fragment shows message shape; the complete proposed service also
needs build, wait, and upload methods:

```protobuf
syntax = "proto3";
package pocket;

service BuildWorker {
  rpc StreamBuildLogs(LogRequest) returns (stream LogFrame);
}

message LogRequest {
  string build_id = 1;
  uint64 after = 2;
}

message LogLine {
  uint64 seq = 1;
  string stage = 2;
  string text = 3;
}

message Gap {
  uint64 from = 1;
  uint64 to = 2;
}

message BuildEnd {
  string state = 1;
  uint64 last_seq = 2;
}

message LogFrame {
  oneof kind {
    LogLine log = 1;
    Gap gap = 2;
    BuildEnd end = 3;
  }
}
```

Use an enum or validated domain conversion for states in a full implementation.
A protobuf `oneof` makes variants explicit, but the receiver must still reject a
message with no variant or invalid field values.

#### Field numbers are not decoration

In protobuf, the numbers after `=` identify fields on the wire. Renaming a source
field is not the same kind of change as reassigning its number. Do not reuse the
number of a removed field for a different meaning; reserve removed numbers and
names when evolving a published schema.

Generated types eliminate a lot of hand-written serialization code, but they do
not establish every domain invariant. A string field can contain an unknown state.
A `uint64` can contain a future cursor. A `oneof` can be absent. Generated parsing
success is the start of validation, not the end.

This is a useful reason to keep transport types separate from validated domain
types. The conversion boundary can reject malformed worker frames before they
become public NDJSON.

#### Work through one translation

Imagine tonic returns a `LogFrame` whose variant is `LogLine`:

```text
protobuf:
  kind = log
  seq = 20
  stage = "test"
  text = "Assertion failed"

public NDJSON:
  {"type":"log","seq":20,"stage":"test","text":"Assertion failed"}\n
```

The JSON record is not protobuf bytes converted to text. It is a new encoding of
the same validated application meaning. That distinction lets the public HTTP
contract remain stable even if generated Rust type names change.

**Optional deep dive: choose contracts before dependencies.** In a complete Rust
implementation, protobuf code generation belongs in the build step, using
compatible tonic/prost tooling. The repository design recommends a vendored
`protoc` configured through an explicit executable path. That is an implementation
detail; it should not dictate whether the application needs a log gap or a
terminal frame. Start with the messages' meaning.

**Exercise 16.** A decoded protobuf message has no `kind` variant. Should the
gateway serialize an empty JSON object, invent an `end`, or reject the worker
message? Explain why successful deserialization is insufficient.

### Lesson 17: Read through the final gRPC status

**Goal:** identify the point at which a streaming RPC is known to have succeeded.

#### Three layers of framing, again

```text
HTTP/2 DATA frames
    contain parts of gRPC message envelopes
        contain serialized protobuf messages
```

A gRPC envelope has a one-byte compression flag and a four-byte big-endian
message length before its payload. HTTP/2 DATA frame boundaries need not match
gRPC message boundaries. Tonic handles that framing; application code receives
decoded messages.

Finally, gRPC status arrives in trailers, or a trailers-only response for some
failures. An initial HTTP `200` is not proof of gRPC success. In tonic, reading
`Streaming::message().await` until `Ok(None)` confirms clean completion;
an error can arrive after earlier valid messages.

**Check yourself:** is a protobuf `BuildEnd { state: "failed" }` a failed RPC?
No. It is a normal business result. An unavailable worker is an RPC failure.

#### Worked example: a valid message followed by failure

Consider this observation:

```text
HTTP/2 response headers: HTTP status 200
gRPC message: log sequence 1
gRPC message: log sequence 2
gRPC final status: UNAVAILABLE
```

The first two log messages can be valid. The overall RPC did not complete
successfully. A program that stops checking after the initial HTTP status loses
the most important completion signal.

Tonic exposes that signal through stream consumption. Keep requesting messages.
`Ok(Some(frame))` means one decoded message. `Ok(None)` means successful stream
completion. `Err(status)` means failure, including failures that occur after data.

Set deadlines on observations so a peer that never sends final status cannot
hold resources indefinitely. A deadline expiring while following logs stops the
observation, not the already accepted build.

**Exercise 17.** A native gRPC client receives `BuildEnd(succeeded)` and then
gets `Err(Unavailable)` on the next read. Which fact is provisional, and which
fact prevents the client from calling the observation complete?

#### Chapter summary

gRPC supplies typed method contracts, message framing, and final status over
HTTP/2. It does not replace domain validation or erase the difference between
business outcomes and transport failures. Always consume a stream through its
final status.

---

## 6. Translate streams, not whole bodies

**Chapter objectives**

You will design both directions of a streaming gateway: worker logs becoming an
HTTP response and an HTTP upload becoming worker messages. You will make final
status, cancellation, limits, and error mapping explicit rather than leaving them
to chance.

### Lesson 18: Translate application meaning at a boundary

**Goal:** draw the gateway as two protocol endpoints joined by bounded adapters.

#### There is no magic protocol switch

The gateway receives ordinary HTTP and originates a separate gRPC call. It must:

1. Validate the public path and cursor.
2. Construct a typed protobuf request.
3. Open the worker stream.
4. Read a bounded worker message.
5. Translate it into one NDJSON record.
6. Yield that record without waiting for the build to finish.
7. Preserve final success or failure semantics.

That is application translation, not byte-for-byte proxying, tunneling, or
gRPC-Web.

#### Start with a translation table

Before writing a handler, map every public operation to its worker operation:

| HTTP side | Worker side | What the adapter must preserve |
| --- | --- | --- |
| JSON build submission | Unary `CreateBuild` | Validation and accepted snapshot |
| Snapshot GET | Unary `GetBuild` | Identifier and current state |
| Long-poll GET | Delayed unary `WaitBuildUpdate` | Cursor and wait-expired outcome |
| NDJSON logs | Server-streaming `StreamBuildLogs` | Records, gaps, final status |
| Raw source request body | Client-streaming `UploadSource` | Bytes, ordering, completion |

This table exposes mistakes early. If log translation calls a unary method that
returns every line at once, the worker interface cannot support the intended
incremental behavior. If long polling is implemented with repeated hidden
snapshot calls, the adapter has changed the operation's cost and timing.

#### Worked example: preserve the client contract

Before extraction, the CLI sends:

```text
GET /builds/session-a-1/logs?after=12
```

After extraction, it sends exactly the same request. The gateway makes a worker
request with build ID `session-a-1` and cursor 12. It validates each returned frame,
serializes the matching public JSON, and yields it.

The CLI need not know which process owns execution. That is the practical value
of preserving an application contract across a transport change.

It also explains why forwarding only headers or changing a URL scheme is not
enough. The public body uses NDJSON; the worker body uses protobuf in gRPC
envelopes. Those are different representations with different finalization rules.

#### An implementation order that keeps bugs local

First implement the domain engine and test it without networking. Next implement
an HTTP adapter around the same operations. Then implement the worker around the
engine, and finally replace the gateway's direct calls with RPC calls.

At each step, the behavior of identifiers, sequences, and build outcomes should
remain the same. If all those rules move into handlers, extracting the worker
becomes a rewrite instead of a transport adaptation.

**Exercise 18.** Name three things the gateway translates and two things it must
not become authoritative for. Use the log endpoint as your example.

### Lesson 19: Build a terminal-frame gate

**Goal:** prevent a late worker error from being misreported as successful HTTP
completion.

#### Delay the terminal frame until final gRPC success

Consider this upstream sequence:

```text
log 1 -> log 2 -> BuildEnd(succeeded) -> gRPC UNAVAILABLE
```

If the gateway already sent HTTP `end`, it falsely told the client that observation
completed successfully. Hold just the pending terminal frame until the worker
stream finishes with successful final status.

The control flow below is an **algorithm sketch**, using domain-level `Log`,
`Gap`, and `End` variants rather than actual generated Rust types:

```text
Open worker stream before returning the HTTP body.
If opening fails: map gRPC error to an HTTP error response.

pending_end = none
Loop, driven by HTTP body demand:
    Read next worker message.
    If message is Log or Gap:
        Reject it if pending_end already exists.
        Validate, translate, serialize, and yield one NDJSON record.
    If message is End:
        Reject a second End.
        Validate it and save it as pending_end; do not yield yet.
    If gRPC ends successfully:
        If pending_end is missing, yield protocol error and stop.
        Otherwise yield the saved NDJSON end and stop.
    If gRPC fails:
        Discard pending_end.
        Yield a sanitized NDJSON error and stop.
```

In Rust, the body can be an `async_stream::stream!` consumed by
`Body::from_stream`. Inside it, tonic's `message().await` yields
`Ok(Some(frame))`, `Ok(None)`, or `Err(status)`. Convert through a shared,
validated domain frame type instead of serializing generated structs blindly.
Explicitly emit the public error variant on failure; an unhandled `?` that merely
truncates the HTTP body loses the structured error explanation.

Do not use `collect`, an all-stream `Vec`, or a detached unbounded producer.
Retaining one bounded terminal frame is intentional buffering, not a violation
of streaming.

#### Make the adapter's state machine explicit

The adapter needs very little state, but that state must be correct:

| Current state | Input | Action |
| --- | --- | --- |
| Open | Log or gap | Validate and yield it |
| Open | Build end | Save end; enter Pending end |
| Pending end | Another application message | Protocol error; no normal end |
| Open | Successful gRPC EOF | Protocol error: missing end |
| Pending end | Successful gRPC EOF | Yield saved end; finish |
| Either unfinished state | gRPC error | Yield public error; finish |

The important distinction is between "the worker says the build is done" and
"the stream that carries that claim completed according to its contract."
The pending frame bridges those two facts.

#### Worked example: four upstream traces

Trace A:

```text
Log(1), End(succeeded), gRPC OK
=> HTTP log 1, HTTP end(succeeded)
```

Trace B:

```text
Log(1), End(failed), gRPC OK
=> HTTP log 1, HTTP end(failed)
```

Trace C:

```text
Log(1), End(succeeded), gRPC UNAVAILABLE
=> HTTP log 1, HTTP error(worker_unavailable); no HTTP end
```

Trace D:

```text
Log(1), clean gRPC EOF without End
=> HTTP log 1, HTTP error(protocol_error); no HTTP end
```

Trace D matters because transport completion alone is not sufficient. The worker
promised a terminal build frame; a successful RPC that violates that promise is
an application protocol failure.

#### Rust control flow, without hiding errors

This fragment is the inner control flow of a tonic consumer, not a complete
gateway function. `worker_stream` is a `tonic::Streaming<LogFrame>`; the comments
stand for the validated conversion and state-machine actions above:

```rust
loop {
    match worker_stream.message().await {
        Ok(Some(frame)) => {
            // Validate and either yield a log/gap or hold the terminal frame.
        }
        Ok(None) => {
            // Yield held end, or an error if no valid terminal frame exists.
            break;
        }
        Err(status) => {
            // Log internal context, discard held end, yield a public error.
            break;
        }
    }
}
```

The empty branches are intentional workshop boundaries, not a compilable
solution. Notice where a shortcut would be dangerous: `while let Ok(Some(...))`
would stop on both EOF and error without forcing you to distinguish them.
An unexamined loop condition can silently collapse failure into success.

**Optional deep dive: terminal-frame latency.** Holding the terminal frame delays
only that frame, not the preceding logs. The gateway still streams useful output
immediately. It retains one bounded record rather than all output. If final
status never arrives, the observation deadline resolves the wait as failure.

**Exercise 19.** Translate this trace:
`Log(1), End(succeeded), Log(2), gRPC OK`.
Why is "just print everything" incorrect even though final gRPC status is OK?

### Lesson 20: Map errors before and after commitment

**Goal:** choose a truthful error representation at either stage of a response.

#### Once headers are sent, HTTP status is committed

Before the HTTP response commits, failures can become ordinary error responses:

| Worker result | Public HTTP result |
| --- | --- |
| Invalid argument | `400` |
| Not found | `404` |
| Capacity exhausted | `429` |
| Unavailable | `503` |
| Deadline exceeded | `504` |
| Unexpected upstream failure | `502` |

Opening the gRPC stream first catches some failures before commitment, not all.
After HTTP `200` has been sent, you cannot replace it with `503`. Emit an NDJSON
`error` and stop without `end`. If the connection itself has broken, even the
error frame may not arrive; EOF without `end` must remain a client error.

#### The point of commitment changes your options

Think of the handler in two phases. Before commitment, it can still choose the
status and headers. After commitment, it can only send more body data or stop.
That is why the public streaming protocol includes an `error` frame.

Do as much cheap validation as possible before creating the body: parse the ID
and cursor, enforce observer admission limits, and attempt to open the worker
stream. Do not promise that this catches every future failure. The worker can
disappear one millisecond after returning initial headers.

The HTTP layer and the application layer cooperate:

```text
Early not found:
  HTTP 404 + ordinary JSON error document

Late worker outage:
  HTTP 200 already sent
  NDJSON logs already sent
  NDJSON error if still writable
  no NDJSON end
```

Both outcomes are truthful. A client that looks only at HTTP `200` is incomplete
for this streaming application.

#### Worked example: sanitize without hiding

Suppose the worker returns an internal error containing a temporary file path.
The gateway should record useful diagnostic context in server-side logs and emit
a stable public code plus a safe message. It should not copy filesystem paths or
stack traces into a terminal-facing API.

Sanitizing does not mean returning success or an empty result. A public frame such
as `{"type":"error","code":"worker_error","message":"Build worker failed"}` tells
the truth without exposing implementation details.

The client should render the error visibly and return a nonzero process status.
If the body is truncated before that frame arrives, the missing `end` rule still
prevents a false success.

#### Deadline versus normal application timeout

A long-poll wait expiring is an expected application result: HTTP `204`. A worker
RPC exceeding its allowed execution deadline is a failure: usually HTTP `504`
before commitment or a streamed error after it.

Use distinct internal outcomes for those cases. If both become one generic
"timeout" string, the gateway cannot map them reliably. Likewise, do not infer
whether an upload was too large or capacity was exhausted by parsing English
error text.

**Exercise 20.** The worker disappears after three NDJSON records have reached
the client. Can the gateway replace HTTP `200` with `503`? Describe the best
available behavior and the client's fallback if even that cannot be delivered.

### Lesson 21: Connect backpressure, budgets, and cancellation

**Goal:** reason about retained work when either side is slow or disappears.

#### Backpressure is controlled waiting

When the HTTP reader slows, the body stream is polled less often. A pull-driven
gateway then reads fewer upstream messages. HTTP/2 flow control and bounded
buffers eventually reduce upstream sending.

This is not a zero-buffer, one-record-per-packet guarantee. Libraries, flow-control
windows, and sockets can prefetch. Bound application messages, queues, and
connection counts rather than assuming the network has no buffers.

There are two different policies:

- **Logs:** never block build execution on an observer; overwrite bounded history
  and signal gaps.
- **Uploads:** slow request consumption when disk or the worker is slow; do not
  consume an unlimited file into RAM.

Dropping the HTTP log body should release its owned tonic stream. Avoid spawning a
producer that outlives the observer. Cancelling observation must not cancel the
service-owned build.

#### Worked example: why an unbounded queue is not streaming safety

Assume the worker produces 100 messages per second but the HTTP reader consumes
10. If the gateway drains upstream into an unbounded queue, that queue grows by
90 messages per second. Yielding one message at a time to HTTP does not fix the
problem: an eager producer elsewhere is retaining the difference.

The whole pipeline matters. Either pull upstream only when downstream asks, or
use a deliberately bounded queue whose full state slows the pump. The log engine
itself still writes to bounded history and overwrites old output; observers can
receive gaps.

#### Count bytes as well as messages

A channel capacity of one bounds message count, not message size. One message
containing a 500 MiB buffer is still a 500 MiB allocation. Pair bounded queue
capacity with bounded payload size.

For uploads, a 32 KiB queued chunk plus another in the sending task and one in
the receiving task is already more than one chunk's worth of application data.
Add HTTP buffers, gRPC envelopes, and transport windows. The correct claim is
"memory is bounded by documented components," not "only 32 KiB is ever resident."

Total service memory also depends on concurrency. A 128-line ring per build is
not a global bound if the number of builds is unlimited. The design therefore
also limits active builds, retained records, observers, and uploads.

#### Cancellation must follow ownership

Ask what the request owns:

| Request | Owned work | Work that must survive disconnect |
| --- | --- | --- |
| Log observer | Cursor and upstream observation RPC | Accepted build execution |
| Long-poll observer | One wait operation | Accepted build execution |
| Upload | Pump, RPC, partial temporary file | No incomplete transfer needs to survive |
| Submission | Admission attempt and response | Build once admission has completed |

In Rust, dropping a future can stop work that lives inside that future. It does
not automatically stop an independently spawned Tokio task. If you spawn pumps,
use supervision that aborts them when the request scope ends; do not rely on
dropping an ordinary task handle to cancel them.

**Exercise 21.** A gateway has a bounded response stream but launches an upstream
pump into an unbounded channel. Explain why the overall design is still
unbounded and what should happen when the HTTP observer disconnects.

### Lesson 22: Forward an upload without inventing success

**Goal:** join clean HTTP input completion, successful forwarding, and successful
worker registration into one truthful receipt.

#### Reverse direction: HTTP upload to client-streaming gRPC

This is request-body streaming:

```text
HTTP client                  Gateway                         Worker
headers + file bytes   ->    Begin(expected_bytes)      ->
more bytes             ->    Chunk(up to 32 KiB)        ->    write file
more bytes             ->    Chunk(up to 32 KiB)        ->    write file
clean HTTP EOF               Finish(actual_bytes)      ->
                             clean request-stream EOF  ->
                       <-    receipt + successful gRPC status
201 + receipt          <-
```

The planned HTTP endpoint requires `Content-Type: application/octet-stream` and
`X-Source-Bytes`. The latter is an application declaration, **not** HTTP framing.
Validate it against actual received bytes.

The proposed RPC accepts `Begin`, zero or more `Chunk` messages, and `Finish`,
followed by clean EOF. Use 32 KiB payload chunks, a 32 MiB total source limit,
64 KiB gRPC message limits, and a forwarding channel with capacity one.
The extra space above 32 KiB accommodates protobuf overhead.

An incoming HTTP body data frame can be larger than 32 KiB. Split it before
forwarding; do not assume one HTTP chunk equals one protobuf message. Avoid
whole-body helpers. Use shared byte slices where practical instead of copying an
entire large incoming buffer into another allocation.

**Why not just turn a failed HTTP read into gRPC EOF?** Because tonic's outgoing
request stream carries messages, not your local read errors. The worker might see
ordinary completion. Communicate the pump result separately and supervise both
the pump and the RPC:

```text
Reserve upload capacity.
Start the RPC with a bounded receiver stream.
Run the HTTP-body pump and RPC under one cancellation scope.

Pump:
    send Begin
    read HTTP data incrementally
    enforce total size and split into bounded Chunk messages
    await each bounded channel send
    only after clean EOF and exact byte count: send Finish
    close sender and report local success

Supervisor:
    on either failure, cancel the other side and report an error
    return 201 only after pump success AND successful RPC receipt
    on observer drop/deadline, abort both tasks
```

Use structured worker error reasons to distinguish oversized uploads (`413`)
from exhausted capacity (`429`), rather than parsing human-readable error text.

The worker must validate message order and byte counts independently, require EOF
after `Finish`, flush the temporary file, and only then register a usable receipt.
Temporary-file ownership guards remove partial uploads even during cancellation.
Do not issue a receipt immediately upon receiving `Finish`: extra messages or a
late transport failure can still invalidate the transfer.

The uploaded bundle remains opaque: this learning project does not unpack or
execute it. A receipt proves transfer completion, not that source code compiled.

#### Worked example: split bytes without changing bytes

Suppose the gateway receives an HTTP data buffer containing 70,000 source bytes.
With a maximum protobuf payload of 32,768 bytes, it forwards:

| Chunk | Source byte range, inclusive | Payload length |
| --- | --- | --- |
| 1 | 0..32767 | 32768 |
| 2 | 32768..65535 | 32768 |
| 3 | 65536..69999 | 4464 |

The worker concatenates payloads in order. Chunk boundaries are not part of the
source file. Do not base64-encode the input merely because another endpoint uses
JSON; protobuf bytes fields can carry raw data.

If the next HTTP data buffer contains another 100 bytes, it simply becomes
another bounded chunk. It is not necessary to fill every protobuf chunk to
exactly 32 KiB. A maximum is a bound, not a required packet size.

#### Validate the upload as a small protocol

The worker can model receiving as:

```text
AwaitBegin -> ReceivingChunks -> AwaitCleanEOF -> Registered
                  |                  |
                  +------ error -----+----> Discarded
```

`Begin` reserves the expected length; `Finish` states the sender's final count.
The worker maintains its own count. All three must agree. Neither count replaces
the maximum-size check. Check the running total before writing beyond the limit.

An empty file is valid when declared count, received count, and final count are
all zero. It still needs Begin, Finish, and clean EOF. A nonempty declaration
followed by no chunks is not an empty-file success.

Reject a duplicate Begin, a Chunk before Begin, an oversized Chunk, a duplicate
Finish, any frame after Finish, a mismatched count, and a stream ending without
Finish. The point of the extra protocol is to make partial and malformed transfers
unambiguously unusable.

#### Understand the two-success rule

There are two asynchronous activities in the gateway:

1. The HTTP pump reads and forwards the source.
2. The gRPC call receives the worker's final receipt.

A returned receipt alone is not enough if the local pump failed. A completed pump
alone is not enough if the worker rejected the upload. A truthful HTTP `201`
requires both successes.

This rule also guides task supervision. If the worker rejects early, stop reading
the HTTP body and cancel the pump. If the HTTP body fails, stop the request stream
and cancel the RPC. If the client disconnects, abort both and let temporary-file
ownership clean up the partial transfer.

#### Receipt lifetime and admission

In the proposed service, a completed source receipt is temporary and single-use.
Creating a build atomically reserves build capacity and claims the receipt.
If admission fails, do not consume the receipt. Otherwise a transient capacity
error would destroy an upload the user could legitimately retry submitting.

Claiming a receipt is not the same as freeing its disk slot: the claimed file
still occupies disk until the associated build no longer needs it and cleanup
removes it.

**Exercise 22.** An upload declares 70,000 bytes and forwards all those bytes, but
the HTTP read then fails instead of reaching clean EOF. Should the gateway send
Finish or return `201`? State what happens to the partial worker file.

#### Workshop: trace both directions

On paper, draw the log translation and upload translation. Mark:

- Where application framing changes.
- Which component owns each bounded buffer.
- What happens if the HTTP client disappears.
- When a successful result becomes safe to emit.

For logs, disconnect means "stop observing." For an incomplete upload, disconnect
means "abort this transfer and clean up." These are intentionally different.

#### Chapter summary

A streaming gateway is a pair of protocol endpoints, not a whole-body converter.
It translates bounded application messages, waits for trustworthy final status,
maps errors according to response commitment, and connects cancellation to
ownership. Upload completion requires both local and remote success.

---

## 7. Failures and your final check

**Chapter objectives**

You will distinguish expected build failure from broken observation, run fault
experiments with predicted outcomes, and review a proposed implementation against
measurable invariants rather than a happy-path demonstration.

### Lesson 23: Break the observation on purpose

**Goal:** predict each failure before running the command.

Create a small observation table on paper with four columns: command, expected
HTTP result, expected terminal frame, and expected process result. Fill in the
predictions before running the following experiments. Keep the existing lab
server running; restart it first if you stopped it earlier.

#### Guided lab: business failure is not network failure

```powershell
curl.exe -N -i "http://127.0.0.1:3000/logs?fail=1"
```

Expected: HTTP `200`, six log frames, then
`{"type":"end","state":"failed","last_seq":6}`.
The JSON spacing may differ. Curl normally exits successfully: it does not
understand your build result. The eventual Pocket CI CLI must.

#### Guided lab: malformed input fails before streaming starts

```powershell
curl.exe -i "http://127.0.0.1:3000/logs?after=99"
```

Expected: HTTP `400` and a JSON error, not HTTP `200` followed by a broken stream.
The lab accepts only cursors 0..6.

#### Guided lab: incomplete observation

```powershell
curl.exe -N --max-time 2 http://127.0.0.1:3000/logs
```

Expected: a few records, then curl timeout/nonzero exit, with no terminal `end`.
The server logs the disconnected observer when its next write detects it.
Restart the bounded Python client from Lesson 12 and interrupt the lab server
mid-stream to see that incomplete transfer is not accepted as success.

#### Guided lab: understand the cursor, without claiming durable replay

```powershell
curl.exe -N "http://127.0.0.1:3000/logs?after=4"
```

Expected: lines 5 and 6, then `end`. The probe generates this suffix; it does not
replay a background build. In the actual service, sequences belong to one
worker-owned build, with bounded retained history and explicit gaps.

Stop terminal A with Ctrl+C after the experiments.

#### What these experiments prove, and what they do not

The first experiment proves that an HTTP operation can finish successfully while
its application result says `failed`. The timeout experiment proves that a client
can stop observing before receiving a terminal record. The malformed-cursor
experiment proves validation happens before stream headers in the lab.

None proves that a real build survives an observer disconnect: the probe does not
have a service-owned build engine. None proves HTTP/2 or gRPC translation:
the probe implements neither. Good experiments state their boundaries as clearly
as their results.

In the eventual gateway, add worker-outage experiments at three points: before
opening the RPC, after some log messages, and after the worker's terminal frame
but before successful final gRPC status. Those positions exercise different
adapter states and prevent a single happy-path test from creating false confidence.

**Exercise 23.** Fill in the expected distinction among:
normal `end(failed)`, NDJSON `error`, and EOF without `end`. Which one confirms a
completed observation of an unsuccessful build?

### Lesson 24: Review the system like an implementer

**Goal:** turn the lessons into an implementation plan with observable acceptance
criteria.

#### Proposed Pocket CI CLI workflow: not runnable yet

Once implemented, the [design](DESIGN.md) calls for:

```text
client submit --label demo
client submit --label failing-demo --fail-tests
client status BUILD_ID --follow
client logs BUILD_ID --follow
client logs BUILD_ID --transport grpc --follow
client submit --label uploaded-demo --source .\source.bundle
```

`submit --source` means upload completely, receive a receipt, then submit.
The default public API remains HTTP even after the engine moves to gRPC.
The native gRPC log command is an additional inspection path, not an excuse to
change the HTTP workflow.

#### What a correct implementation must prove

These are workshop acceptance criteria, **not current starter capabilities**:

| Claim | Evidence that actually proves it |
| --- | --- |
| Logs stream | Receive first log while producer is blocked before completion |
| Parser handles real networking | Split a record and a UTF-8 character; coalesce records |
| Memory is bounded | Enforce per-record, history, queue, and concurrency limits |
| Gateway preserves errors | Inject gRPC error after terminal frame; HTTP must not emit `end` |
| Disconnect is safe | Drop viewer; build still reaches terminal state |
| Upload is incremental | Observe worker write before allowing HTTP EOF |
| Partial upload is unusable | Interrupt transfer; no receipt and no retained partial file |

Gated producers and real loopback sockets give stronger tests than arbitrary
sleeps. "It eventually printed everything" does not prove streaming.

#### Worked example: prove streaming, not just eventual output

A weak test starts a build, waits ten seconds, and asserts that all logs arrived.
A buffering implementation passes: it can return every line only at completion.

A stronger test creates a controlled producer:

```text
Producer: emit first log -> wait at a gate -> emit remaining logs -> finish
Test:     receive first log -> assert build not complete -> open gate
```

If the server collects the whole stream, the test cannot receive the first log
while the gate is closed. That directly tests the requirement. Use a test timeout
to fail cleanly rather than hang forever, but do not use elapsed time as the only
evidence.

The upload equivalent gates client EOF. Send an initial source chunk, hold the
request open, and verify that the worker has written those bytes before allowing
the rest of the request to finish. A handler that reads the entire file first
cannot pass.

#### A practical implementation sequence

1. Implement validated domain types and the simulated engine.
2. Add submission and snapshot HTTP operations.
3. Add cursor-based waits and test the check/subscribe race.
4. Add bounded history, NDJSON output, and a fragmented-input client parser.
5. Put the existing engine behind tonic and verify native RPC behavior.
6. Replace gateway domain calls with RPC adapters, preserving public contracts.
7. Add upload pumping, supervision, receipt lifecycle, and cleanup tests.

This sequence is not a request to generate every future module immediately.
Finish and test one observable capability before adding the next transport or
lifecycle concern.

**Exercise 24.** You are reviewing an implementation that passes eventual-log
tests but has no early-delivery test, uses a queue with no capacity, and returns
success after a protobuf terminal frame without reading final gRPC status.
Identify the three violated invariants and propose one focused test for each.

#### Final self-test

1. Does HTTP/2 make every response application-streaming?
2. Can HTTP/1.1 deliver live logs?
3. Is one body read always one JSON or protobuf message?
4. Can HTTP status change after response headers commit?
5. Why hold a terminal protobuf frame until final gRPC status?
6. Why does an upload need local pump success as well as an RPC receipt?
7. Should a slow log observer stop a build?
8. Does `202` mean tests passed?

**Answers:** 1 no; 2 yes; 3 no; 4 no; 5 to avoid false success after a late RPC
failure; 6 a local body-read failure must not masquerade as clean completion;
7 no, use bounded history and gaps; 8 no, only admission.

#### The one-page mental model

> HTTP versions describe transport mechanics. Unary and streaming describe
> application interaction. Application records need framing independent of
> transport reads. A gateway translates bounded records incrementally, preserves
> final error semantics, and connects cancellation and backpressure deliberately.

That is the foundation. The remaining Rust work is turning those invariants into
types, handlers, tasks, and tests.

---

## Appendix A: Exercise answers

These are worked answers, not extra prerequisites. Use them to check the model
you developed in each lesson. If your wording differs but preserves the
invariant, your answer may be equally valid.

### Answer 1: Submission uncertainty

The server may never have received a complete request, so no build exists.
Alternatively it may have accepted the build and sent a response that was lost,
so the build exists and continues independently. A disconnect describes the
communication failure, not the exact point reached by remote application logic.
Blindly repeating a non-idempotent submission can create a duplicate.

### Answer 2: Two outcomes

The status-reading HTTP operation succeeded: the server returned a valid current
representation with `200`. The build did not succeed: its state is `failed`.
A CLI whose purpose is to wait for a successful build should report failure and
return an appropriate nonzero process status, even though its status request
worked normally.

### Answer 3: Reading the transcript

The request target is `/snapshot`. The response status is `200`, the content type
is `application/json`, and the body is the JSON document describing the lab
lesson. If connection establishment fails, there is no response status, no
response content type, and no response body. The client can still know the target
it intended to request; it just has no server response to interpret.

### Answer 4: Versions versus application streaming

HTTP/1.1 already supports incrementally delivered response bodies, commonly using
chunked framing for unknown lengths. HTTP/2 is not a prerequisite for live logs.
One genuine benefit of HTTP/2 is multiplexing a long log observation and a short
snapshot exchange on one connection without HTTP/1.1 response ordering forcing
the snapshot behind the logs. It still does not remove TCP loss effects.

### Answer 5: Partial-record ownership

The application NDJSON decoder preserves unfinished bytes between HTTP body
reads. Neither read is a complete JSON document, so parsing each separately would
reject valid input. The HTTP library has already handled transfer framing; the
application still needs to reconstruct its own record boundaries.

### Answer 6: Two endings

The JSON `end` is an application frame confirming a terminal build result.
The zero chunk ends the HTTP/1.1 chunked message. HTTP/2 instead uses its frame
mechanism, including END_STREAM, and does not send the HTTP/1.1 zero-chunk
terminator. The application `end` remains useful in either version.

### Answer 7: Select a pattern

Current status fits a unary snapshot, completed by receiving the representation.
Waiting for a stage change fits long polling, completed by a newer snapshot or a
normal wait-expired result. Logs fit response streaming, completed by a valid
terminal frame and clean underlying completion. An input bundle fits request
streaming, completed only after clean input and a successful registered receipt.

### Answer 8: The missed transition

If the server checks version 3, then transitions to 4, then subscribes, a waiter
that relies only on future notifications can miss the already-completed change.
Subscribing before reading means a transition either appears in the state read
or is visible through the established subscription. The waiter loops on the
version predicate rather than treating a wakeup itself as the result.

### Answer 9: Changing the upload contract

Sending progress while input is arriving requires the client and gateway to
support overlapping request production and response consumption, define progress
framing, and handle two directions of application state. It is not automatically
wrong, but it is a different contract with more lifecycle concerns. Pocket CI
only needs a final receipt, then a separate build submission, so this complexity
is unnecessary.

### Answer 10: An honest gap

The worker sends `{"type":"gap","from":9,"to":14}`, then a log with sequence 15.
The terminal should tell the user that some earlier output is no longer retained.
It should not print an apparently continuous transcript or silently move its
cursor past the missing range. Gap bounds are inclusive.

### Answer 11: Incrementality and retention

Design B can deliver a frame before build completion because it only needs the
next available frame. Design A must finish collecting before producing its body,
so it cannot satisfy live output and retains memory proportional to all collected
output. B still needs bounded frame sizes and correctly bounded surrounding
buffers; "uses a stream" alone is not a total-memory proof.

### Answer 12: Data after end

Reject the observation as a protocol violation. In this application `end` is
terminal, so later records contradict the agreed sequence. Ignoring them hides a
worker or adapter bug, while printing them makes the meaning of completion
ambiguous. A complete client also waits for underlying transfer completion so a
late failure cannot masquerade as success.

### Answer 13: Slow-reader recovery

The frames are a gap covering 2..2, log 3, log 4, log 5, and finally `end` with state
`succeeded` and `last_seq` 5. The producer did not wait for this slow reader, and
the explicit gap prevents missing output from being hidden.

### Answer 14: One source of truth

The worker is authoritative. Its engine owns execution and the associated state
transitions. A gateway-owned registry adds invalidation, ordering, restart, and
consistency problems without helping this small learning project. Adapting each
operation to the worker preserves a single clear owner.

### Answer 15: Time is not cardinality

The ten-second wait is unary because it receives one request message and returns
one response message. The upload is client streaming because it receives multiple
request messages and returns one receipt. Neither answer depends on how quickly
the messages arrive.

### Answer 16: Missing protobuf variant

Reject the frame as a worker protocol error. Protobuf can successfully decode a
message whose optional `oneof` is unset, but the application requires one of its
meaningful variants. An empty JSON object is not a valid substitute, and inventing
`end` would fabricate successful completion.

### Answer 17: Late RPC failure

The terminal build frame has been received, but the observation has not yet been
confirmed complete. The subsequent gRPC error prevents claiming successful RPC
completion. A gateway discards its pending terminal frame and emits an error if
possible. It must not turn a late transport failure into a clean HTTP `end`.

### Answer 18: Adapter responsibility

The gateway translates the HTTP path/query into a typed RPC request, worker
protobuf frames into public NDJSON frames, and worker errors into public HTTP or
streamed errors. It must not become authoritative for build execution state or
log history. Those remain worker-owned.

### Answer 19: A message after the terminal frame

Yield log 1, hold the terminal frame, then treat log 2 as a protocol violation.
Discard the held end and emit an error if writable. Final gRPC OK confirms the
transport completed according to gRPC, not that the application's message order
was valid. Application validation remains necessary.

### Answer 20: A late outage

Once HTTP headers have committed, the gateway cannot replace `200` with `503`.
It should emit a sanitized NDJSON error and stop without normal `end`. If the
connection cannot carry that record, the client sees incomplete EOF or a body
read failure and must treat the observation as unsuccessful.

### Answer 21: A hidden unbounded buffer

The unbounded channel decouples production from consumption without limiting the
retained difference. The HTTP body can be perfectly incremental while memory in
the pump grows indefinitely. Pull upstream on demand or use a bounded channel
with bounded messages. On observer disconnect, release the upstream stream and
cancel any observation-specific pump, but keep the accepted build running.

### Answer 22: Bytes are not clean completion

Do not send Finish and do not return `201`. Reaching the declared byte count is
necessary but does not replace clean HTTP EOF. Cancel forwarding and the RPC;
the worker must discard the unregistered temporary file. Otherwise an incomplete
or malformed request could produce a usable source receipt.

### Answer 23: Classify the ending

`end(failed)` confirms a completed observation of an unsuccessful build, assuming
the underlying stream also completes cleanly. An `error` frame says observation
failed, and EOF without `end` is incomplete even when no explanatory record
arrived. Neither observation failure should be silently converted into a normal
failed-build result.

### Answer 24: Review three invariants

First, early delivery is unproven: add a gated producer that emits one frame
before waiting for the test to permit completion. Second, memory is unbounded:
test the bounded forwarding mechanism under a blocked consumer and inspect the
documented message and concurrency limits. A finite test alone cannot prove
every global memory bound; combine behavioral tests with explicit ownership and
capacity reasoning.

Third, final RPC status is ignored: inject an error after the terminal protobuf
frame and assert that the HTTP adapter emits no normal `end`. These tests target
the actual requirements instead of merely rerunning a happy path.

## Appendix B: Glossary

| Term | Meaning in this book |
| --- | --- |
| Admission | Reserving capacity before accepting work |
| ALPN | TLS negotiation mechanism used to select an application protocol |
| Application frame | One meaningful record such as log, gap, end, or error |
| Backpressure | Slowing upstream consumption or production when downstream cannot keep up |
| Bounded replay | Reconnection from a cursor while retained history is available |
| Chunked transfer encoding | HTTP/1.1 framing for a body sent as length-prefixed chunks |
| Client streaming | Multiple request messages and one response message in an RPC |
| Commitment | The point after which response status and initial headers cannot be replaced |
| Cursor | A token representing the client's last observed position |
| DATA frame | An HTTP/2 frame carrying body bytes, not necessarily a whole application message |
| Deadline | A time bound after which an operation is cancelled or fails |
| Domain type | A representation of validated application meaning independent of transport |
| EOF | End of input; its significance depends on the surrounding protocol |
| Flow control | Protocol mechanisms that limit outstanding data |
| Framing | Rules that identify message or record boundaries in a byte sequence |
| gRPC status | The RPC-level success or failure result, distinct from a build outcome |
| HPACK | HTTP/2 header compression |
| HTTP semantics | The meaning of methods, statuses, headers, and representations |
| Idempotent | Having the same intended effect when repeated as when performed once |
| Long polling | Holding one request until a change or an application wait timeout |
| Multiplexing | Interleaving independent exchanges on one connection |
| NDJSON | Complete JSON values separated by newline delimiters |
| Observer | A consumer of status or logs that does not own execution |
| Protobuf | A schema-based binary serialization format used by these gRPC contracts |
| Receipt | Evidence that a complete source transfer was registered |
| Server streaming | One request message and a sequence of response messages in an RPC |
| Snapshot | The current state, not a durable history of every transition |
| Supervision | Owning and coordinating child tasks, including failure and cancellation |
| Trailer | Metadata sent at the end of a message/stream, used by gRPC for final status |
| Unary | One request message and one response message |

## Appendix C: Troubleshooting the lab

### Python is not found

Try `py -3 --version`. If that succeeds, use `py -3` in place of `python` in the
commands, including after the PowerShell here-string pipe. Install Python only
if neither command is available. The lab needs no third-party packages.

### Curl behaves like a PowerShell command instead of curl

Use the explicit executable name `curl.exe`, as in the lessons. Older PowerShell
environments may map the short name `curl` to another command with different
arguments.

### Port 3000 is already in use

Do not stop an unrelated process. Start the lab on another port:

```powershell
python .\examples\http_stream_lab.py --port 3001
```

Replace `3000` with `3001` in every URL you use, including the Python client
snippet. A server starting successfully on one port does not make another port's
responses part of your experiment.

### All output appears at the end

Use curl's `-N` option and run the command directly in the terminal rather than
piping through a tool that may buffer. With the provided lab, you should normally
see one line promptly, more lines at roughly one-second intervals, and terminal
output after about six seconds.

Timing is a useful observation here, not a production correctness proof. The
gated-producer tests in Lesson 24 are stronger because they establish ordering
without relying on a particular machine's scheduling speed.

### Hexadecimal numbers appear between JSON lines

You probably used `--raw`. Those are HTTP/1.1 chunk lengths, not corrupt JSON.
Remove `--raw` for ordinary application output. Do not feed raw transfer framing
directly into the JSON parser.

### A timeout command returns a nonzero exit code

That is expected for the interrupted-observation exercise. It demonstrates that
the operation did not complete. Do not change the command to hide the failure and
then call the experiment successful.

### The server prints a disconnected-observer message

The lab reports connection-reset and broken-pipe conditions when its next write
detects that the reader went away. A disconnect is not always detected
instantaneously because the OS can temporarily accept buffered writes. The
message is expected after a deliberately interrupted stream.

### An HTTP/2 command does not work

The companion server speaks HTTP/1.1 only. A curl build with HTTP/2 support cannot
upgrade a server that does not implement it. Read the HTTP/2 diagrams as protocol
instruction and use a future tonic integration test for actual local HTTP/2
evidence.

### A proposed `client` command does not exist

That is the repository's current state, not an installation mistake. The book
labels those commands as the target workflow. Run the provided Python/curl labs
now; treat the Rust workshops as the implementation path toward that CLI.

## Optional references: outside the six-hour route

- [Project overview](README.md) and [full proposed contracts](DESIGN.md).
- [HTTP semantics: RFC 9110](https://www.rfc-editor.org/rfc/rfc9110).
- [HTTP/1.1 framing: RFC 9112](https://www.rfc-editor.org/rfc/rfc9112).
- [HTTP/2: RFC 9113](https://www.rfc-editor.org/rfc/rfc9113).
- [gRPC concepts](https://grpc.io/docs/what-is-grpc/core-concepts/).
- [axum streaming bodies](https://docs.rs/axum/latest/axum/body/struct.Body.html).
- [reqwest byte streams](https://docs.rs/reqwest/latest/reqwest/struct.Response.html#method.bytes_stream).
- [tonic streaming](https://docs.rs/tonic/latest/tonic/struct.Streaming.html).
