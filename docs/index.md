---
template: home.html
description: "Narad is a message broker in one Go binary. POST a message to any node and get 202 once it is fsynced to disk; consumers pull it, work under a lease and ack."
hide:
  - navigation
  - toc
  - path
---

<div class="nr-hero" markdown>

# <span class="nr-line">A small, sturdy message broker.</span> <span class="nr-line">HTTP in, <span class="nr-nowrap">at-least-once out.</span></span>

<div class="nr-hero__pitch" markdown>

<p class="nr-lead">When Narad answers <code>202</code>, your message is already fsynced to disk. It is one Go binary: POST to any node, pull the message, work on it under a lease, ack it. Anything left unacked comes back.</p>

[Run it locally](client/cli.md#the-sixty-second-demo){ .md-button .md-button--primary }

</div>

<figure class="nr-term" aria-labelledby="nr-term-caption">
<div class="nr-term__bar" aria-hidden="true"><span>Terminal</span></div>
<pre tabindex="0"><code><span class="p">$</span> NARAD=http://127.0.0.1:7942
<span class="p">$</span> curl -i -X POST "$NARAD/v1/topics/orders/produce?key=customer-42" \
    -H 'Content-Type: application/json' \
    -d '<span class="payload">{"order_id":"ord_123","amount":4999}</span>'
<span class="s">HTTP/1.1 202 Accepted</span>
<span class="h">Date: Mon, 28 Sep 2026 10:49:16 GMT</span>
<span class="h">Content-Length: 0</span>

<span class="p">$</span> curl "$NARAD/v1/topics/orders/consume?wait=10s"
{"topic":"orders","partition":1,"offset":0,"key":"customer-42",
 "payload":<span class="payload">{"order_id":"ord_123","amount":4999}</span>,
 "timestamp":1790592556,"receipt_handle":"1:0:8874198393259169482"}

<span class="p">$</span> curl -i -X POST -H 'Content-Type: application/json' \
    "$NARAD/v1/topics/orders/ack?receipt_handle=1:0:8874198393259169482"
<span class="s">HTTP/1.1 204 No Content</span>
<span class="h">Date: Mon, 28 Sep 2026 10:49:21 GMT</span></code></pre>
<figcaption id="nr-term-caption">A recorded session against <code>narad server start --dev</code>, after creating the topic <code>orders</code>. The consume response is one line; it is wrapped here to fit.</figcaption>
</figure>

<div class="nr-doors" markdown>

- [<span class="nr-doors__tab">Build<span class="nr-sr">:</span></span> <span class="nr-doors__what">Produce, consume and ack from your service.</span>](client/index.md)
- [<span class="nr-doors__tab">Operate<span class="nr-sr">:</span></span> <span class="nr-doors__what">Deploy with Helm, monitor, scale, upgrade.</span>](operate/index.md)
- [<span class="nr-doors__tab">Understand<span class="nr-sr">:</span></span> <span class="nr-doors__what">Storage, Raft, rebalance, the delivery contract.</span>](internals/index.md)
- [<span class="nr-doors__tab">Compare<span class="nr-sr">:</span></span> <span class="nr-doors__what">Kafka, NATS, RabbitMQ, SQS, Redis, Pulsar.</span>](compare.md)

</div>

</div>

<section class="nr-band nr-tint nr-tint--mint" markdown>
<div class="nr-band__copy" markdown>

## Hit any pod. Narad does the rest. {#any-pod}

Put every pod behind one load balancer and send it every produce, consume and ack. Whichever pod catches the request is the right one: your client never looks for a leader, never learns a partition map and never speaks a metadata protocol.

That pod appends the message to its own write-ahead log and fsyncs before it answers `202`, so you wait for one local fsync. After the `202` it hands the message to the partition's owner and retries until the owner has fsynced it, read it back and verified it. Only then do consumers see it.

[The produce path, step by step](internals/produce-path.md){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="0 0 860 424" role="img" aria-labelledby="d1w-t d1w-d">
<title id="d1w-t">A produce, answered by whichever pod catches it</title>
<desc id="d1w-d">Your service sends POST /produce to the load balancer, which passes it to narad-1; narad-0 or any other pod would have done as well. narad-1 appends the message to its write-ahead log, fsyncs, and answers 202 Accepted, so the producer waited for one fsync. After the 202, in the background, narad-1 hands the message, ord_123, to narad-2, the partition's owner, and retries until narad-2 has fsynced it, read it back and verified it.</desc>
<rect class="rg" x="438" y="20" width="414" height="392" rx="6"/>
<text class="lb" x="20" y="116">Your service</text>
<rect class="ink" x="20" y="132" width="14" height="132"/>
<path class="ln" d="M34 176H238"/>
<path class="ah" d="M238 169L250 176L238 183Z"/>
<path class="ln2 dash" d="M250 212H46"/>
<path class="ah" d="M46 205L34 212L46 219Z"/>
<circle class="sc" cx="60" cy="152" r="13"/><text class="sn" x="60" y="157.5">1</text>
<text class="cd" x="82" y="158">POST /produce</text>
<circle class="sc" cx="60" cy="238" r="13"/><text class="sn" x="60" y="243.5">3</text>
<text class="cd" x="82" y="244">202 Accepted</text>
<text class="an" x="82" y="270">you waited for one fsync</text>
<rect class="bx" x="250" y="148" width="160" height="92" rx="2"/>
<text class="lb mid" x="330" y="201">Load balancer</text>
<path class="ln" d="M410 176H458"/>
<path class="ah" d="M458 169L470 176L458 183Z"/>
<path class="ln2 dash" d="M470 212H422"/>
<path class="ah" d="M422 205L410 212L422 219Z"/>
<path class="ln1 dash" d="M330 148V114Q330 98 346 98H460"/>
<path class="ah" d="M460 93L470 98L460 103Z"/>
<rect class="bx bxq" x="470" y="72" width="150" height="52" rx="2"/>
<text class="lb mid mu" x="545" y="105">narad-0</text>
<text class="an mu" x="636" y="104">or any other pod</text>
<rect class="bx bx3" x="470" y="148" width="150" height="92" rx="2"/>
<text class="lb mid" x="545" y="201">narad-1</text>
<path class="ln2" d="M620 194H636"/>
<path class="bx tn" d="M636 180V210A32 8 0 0 0 700 210V180"/>
<ellipse class="bx tn" cx="668" cy="180" rx="32" ry="8"/>
<text class="an mid" x="668" y="206">WAL</text>
<circle class="sc" cx="726" cy="194" r="13"/><text class="sn" x="726" y="199.5">2</text>
<text class="an" x="748" y="200">fsync</text>
<path class="ln" d="M545 240V258M545 296V312"/>
<path class="ah" d="M538 312L545 324L552 312Z"/>
<polygon class="msg" points="488,262 584,262 602,292 506,292"/>
<text class="msg-t" x="545" y="283.5">ord_123</text>
<text class="an mu" x="636" y="272">after the 202,</text>
<text class="an mu" x="636" y="294">in the background</text>
<rect class="bx" x="470" y="324" width="150" height="56" rx="2"/>
<text class="lb mid" x="545" y="359">narad-2</text>
<circle class="sc" cx="646" cy="338" r="13"/><text class="sn" x="646" y="343.5">4</text>
<text class="lb" x="668" y="344">The owner</text>
<text class="an" x="668" y="368">fsyncs, reads back,</text>
<text class="an" x="668" y="390">verifies</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 572" role="img" data-search-exclude aria-labelledby="d1n-t d1n-d">
<title id="d1n-t">A produce, answered by whichever pod catches it</title>
<desc id="d1n-d">Your service sends POST /produce to the load balancer, which passes it to narad-1; narad-0 or any other pod would have done as well. narad-1 appends the message to its write-ahead log, fsyncs, and answers 202 Accepted. After the 202, in the background, narad-1 hands the message, ord_123, to narad-2, the partition's owner, and retries until narad-2 has fsynced it, read it back and verified it.</desc>
<rect class="rg" x="8" y="228" width="344" height="332" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="320" height="12"/>
<path class="ln" d="M236 48V144"/>
<path class="ah" d="M230 144L236 156L242 144Z"/>
<path class="ln2 dash" d="M280 156V60"/>
<path class="ah" d="M274 60L280 48L286 60Z"/>
<circle class="sc" cx="32" cy="78" r="12"/><text class="sn" x="32" y="83">1</text>
<text class="cd" x="52" y="83">POST /produce</text>
<circle class="sc" cx="32" cy="112" r="12"/><text class="sn" x="32" y="117">3</text>
<text class="cd" x="52" y="117">202 Accepted</text>
<text class="an" x="52" y="140">you waited for one fsync</text>
<rect class="bx" x="150" y="156" width="190" height="48" rx="2"/>
<text class="lb mid" x="245" y="186">Load balancer</text>
<path class="ln" d="M236 204V264"/>
<path class="ah" d="M230 264L236 276L242 264Z"/>
<path class="ln2 dash" d="M280 276V216"/>
<path class="ah" d="M274 216L280 204L286 216Z"/>
<path class="ln1 dash" d="M150 180H100Q84 180 84 196V266"/>
<path class="ah" d="M79 266L84 276L89 266Z"/>
<rect class="bx bxq" x="24" y="276" width="120" height="48" rx="2"/>
<text class="lb mid mu" x="84" y="306">narad-0</text>
<text class="an mu" x="24" y="346">or any other pod</text>
<rect class="bx bx3" x="176" y="276" width="164" height="56" rx="2"/>
<text class="lb mid" x="258" y="310">narad-1</text>
<path class="ln2" d="M258 332V347"/>
<path class="bx tn" d="M224 354V382A34 7 0 0 0 292 382V354"/>
<ellipse class="bx tn" cx="258" cy="354" rx="34" ry="7"/>
<text class="an mid" x="258" y="379">WAL</text>
<circle class="sc" cx="140" cy="370" r="12"/><text class="sn" x="140" y="375">2</text>
<text class="an" x="160" y="375">fsync</text>
<path class="ln" d="M258 389V408M258 442V468"/>
<path class="ah" d="M252 468L258 480L264 468Z"/>
<polygon class="msg" points="208.5,412 290.5,412 307.5,438 225.5,438"/>
<text class="msg-t" x="258" y="430">ord_123</text>
<text class="an mu end" x="196" y="422">after the 202,</text>
<text class="an mu end" x="196" y="442">in the background</text>
<rect class="bx" x="176" y="480" width="164" height="48" rx="2"/>
<text class="lb mid" x="258" y="510">narad-2</text>
<circle class="sc" cx="36" cy="494" r="12"/><text class="sn" x="36" y="499">4</text>
<text class="lb" x="56" y="500">The owner</text>
<text class="an" x="56" y="520">fsyncs, reads it</text>
<text class="an" x="56" y="540">back, verifies</text>
</svg>
</div>
<figcaption>Steps 1 to 3 cost one local fsync. Step 4 happens after the <code>202</code> and survives a crash of either pod: the write-ahead log keeps its copy until the owner's copy is verified.</figcaption>
</figure>
</section>

<section class="nr-band nr-band--flip nr-tint nr-tint--sky" markdown>
<div class="nr-band__copy" markdown>

## Consume without the ceremony {#consume}

No consumer groups, no partition assignment, and nothing rebalances when a worker joins. Run one worker or a hundred against the same topic: each message goes to one of them at a time, under a lease that lasts 30 seconds by default. Ack it and it is settled.

If a worker dies mid-job, its lease runs out and the message goes to the next worker that asks. A slow worker extends its lease; one that gives up hands the message back at once. A late ack gets `410 Gone`, so you know the work may run twice.

[Consuming: leases, acks, extends and nacks](client/consuming.md){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="0 24 860 368" role="img" aria-labelledby="d2w-t d2w-d">
<title id="d2w-t">A crashed worker's message comes back</title>
<desc id="d2w-d">Three workers pull from the topic orders, with no consumer group. Worker 1 consumes and acks. Worker 2 takes ord_123 on a 30 second lease and crashes without acking. The lease runs out and ord_123 goes back into the topic. Worker 3, just started, takes it and acks with 204.</desc>
<defs><pattern id="d2w-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="20" y="96" width="260" height="220" rx="6"/>
<text class="rl" x="44" y="134">Topic</text>
<text class="cd cdb" x="44" y="168">orders</text>
<rect class="bx bxq tn" x="44" y="236" width="212" height="40" rx="2"/>
<path class="ln1" d="M79.3 236V276M114.7 236V276M150 236V276M185.3 236V276M220.7 236V276"/>
<path class="ln1" d="M280 128H304Q320 128 320 112V84Q320 68 336 68H562"/>
<path class="ah" d="M562 63L572 68L562 73Z"/>
<text class="an mu" x="340" y="56">consume, ack</text>
<rect class="you" x="572" y="40" width="160" height="56" rx="2"/>
<text class="lb mid on" x="652" y="75">worker 1</text>
<path class="ln" d="M280 196H560"/>
<path class="ah" d="M560 189L572 196L560 203Z"/>
<circle class="sc" cx="308" cy="172" r="13"/><text class="sn" x="308" y="177.5">1</text>
<text class="an" x="330" y="178">consume, 30 s lease</text>
<path class="ln dash" d="M572 224H489M385 224H292"/>
<path class="ah" d="M292 217L280 224L292 231Z"/>
<polygon class="msg" points="380,209 476,209 494,239 398,239"/>
<text class="msg-t" x="437" y="230.5">ord_123</text>
<circle class="sc" cx="308" cy="262" r="13"/><text class="sn" x="308" y="267.5">3</text>
<text class="an" x="330" y="268">Lease runs out: it comes back</text>
<rect class="dead" x="572" y="180" width="160" height="56" fill="url(#d2w-h)"/>
<rect class="plate" x="596" y="192" width="112" height="32"/>
<text class="lb mid" x="652" y="215">worker 2</text>
<circle class="sc" cx="762" cy="208" r="13"/><text class="sn" x="762" y="213.5">2</text>
<text class="an" x="784" y="214">crashed</text>
<path class="ln" d="M280 288H304Q320 288 320 304V332Q320 348 336 348H560"/>
<path class="ah" d="M560 341L572 348L560 355Z"/>
<circle class="sc" cx="348" cy="322" r="13"/><text class="sn" x="348" y="327.5">4</text>
<text class="an" x="370" y="328">consume, ack: 204</text>
<rect class="you" x="572" y="320" width="160" height="56" rx="2"/>
<text class="lb mid on" x="652" y="355">worker 3</text>
<text class="an mu" x="752" y="354">just started</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 528" role="img" data-search-exclude aria-labelledby="d2n-t d2n-d">
<title id="d2n-t">A crashed worker's message comes back</title>
<desc id="d2n-d">Three workers pull from the topic orders, with no consumer group. Worker 1 consumes and acks. Worker 2 takes ord_123 on a 30 second lease and crashes without acking. The lease runs out and ord_123 goes back into the topic. Worker 3, just started, takes it and acks with 204.</desc>
<defs><pattern id="d2n-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="8" y="12" width="344" height="92" rx="6"/>
<text class="rl" x="24" y="44">Topic</text>
<text class="cd cdb" x="24" y="76">orders</text>
<rect class="bx bxq tn" x="176" y="38" width="160" height="40" rx="2"/>
<path class="ln1" d="M208 38V78M240 38V78M272 38V78M304 38V78"/>
<path class="ln1" d="M64 104V290"/>
<path class="ah" d="M59 290L64 300L69 290Z"/>
<path class="ln" d="M150 104V288"/>
<path class="ah" d="M144 288L150 300L156 288Z"/>
<path class="ln dash" d="M210 300V217M210 183V116"/>
<path class="ah" d="M204 116L210 104L216 116Z"/>
<path class="ln" d="M296 104V288"/>
<path class="ah" d="M290 288L296 300L302 288Z"/>
<polygon class="msg" points="160.5,187 242.5,187 259.5,213 177.5,213"/>
<text class="msg-t" x="210" y="205">ord_123</text>
<circle class="sc" cx="150" cy="148" r="12"/><text class="sn" x="150" y="153">1</text>
<circle class="sc" cx="210" cy="256" r="12"/><text class="sn" x="210" y="261">3</text>
<circle class="sc" cx="296" cy="200" r="12"/><text class="sn" x="296" y="205">4</text>
<rect class="you" x="16" y="300" width="96" height="48" rx="2"/>
<text class="lb mid on" x="64" y="330">worker 1</text>
<rect class="dead" x="132" y="300" width="96" height="48" fill="url(#d2n-h)"/>
<rect class="plate" x="137" y="310" width="86" height="28"/>
<text class="lb mid" x="180" y="330">worker 2</text>
<rect class="you" x="248" y="300" width="96" height="48" rx="2"/>
<text class="lb mid on" x="296" y="330">worker 3</text>
<circle class="sc" cx="180" cy="372" r="12"/><text class="sn" x="180" y="377">2</text>
<path class="ln1" d="M16 404H344"/>
<circle class="sc" cx="28" cy="431" r="12"/><text class="sn" x="28" y="436">1</text>
<text class="an" x="50" y="436">Worker 2 takes ord_123 for 30 s</text>
<circle class="sc" cx="28" cy="459" r="12"/><text class="sn" x="28" y="464">2</text>
<text class="an" x="50" y="464">Worker 2 crashes, never acks</text>
<circle class="sc" cx="28" cy="487" r="12"/><text class="sn" x="28" y="492">3</text>
<text class="an" x="50" y="492">The lease runs out: it comes back</text>
<circle class="sc" cx="28" cy="515" r="12"/><text class="sn" x="28" y="520">4</text>
<text class="an" x="50" y="520">Worker 3 takes it, acks: 204</text>
</svg>
</div>
<figcaption>Worker 2 may have done part of the job before it died, so <code>ord_123</code> can run twice. That is at-least-once: make handlers idempotent.</figcaption>
</figure>
</section>

<section class="nr-band nr-tint nr-tint--lilac" markdown>
<div class="nr-band__copy" markdown>

## Built to say yes {#say-yes}

Any live node accepts a produce with a local fsync: no leader election and no quorum on the write path, so produces keep landing while one node is alive. If a partition's owner is down, the message is committed to a live partition of the same topic instead, and consumers keep consuming.

The price, stated up front: **ordering is not guaranteed.** Messages already stored on the dead node wait for it to come back, and their partition answers `503` until then. If you need a sequence, carry one in the payload.

[The availability trade, in full](client/guarantees-and-errors.md#availability-the-deliberate-trade){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="0 56 860 348" role="img" aria-labelledby="d3w-t d3w-d">
<title id="d3w-t">A produce while the partition's owner is down</title>
<desc id="d3w-d">Your service sends POST /produce to narad-0, which fsyncs it and answers 202 Accepted. The partition's owner, narad-1, is down, so the hand-off to it is blocked. The message, ord_123, is rerouted to a live partition of the same topic on narad-2, and consumers keep consuming from there.</desc>
<defs><pattern id="d3w-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb deep" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="236" y="64" width="560" height="284" rx="6"/>
<text class="lb" x="20" y="82">Your service</text>
<rect class="ink" x="20" y="100" width="14" height="120"/>
<path class="ln" d="M34 146H252"/>
<path class="ah" d="M252 139L264 146L252 153Z"/>
<path class="ln2 dash" d="M264 182H46"/>
<path class="ah" d="M46 175L34 182L46 189Z"/>
<circle class="sc" cx="60" cy="122" r="13"/><text class="sn" x="60" y="127.5">1</text>
<text class="cd" x="82" y="128">POST /produce</text>
<circle class="sc" cx="60" cy="206" r="13"/><text class="sn" x="60" y="211.5">2</text>
<text class="cd" x="82" y="212">202 Accepted</text>
<rect class="bx bx3" x="264" y="124" width="140" height="80" rx="2"/>
<text class="lb mid" x="334" y="171">narad-0</text>
<path class="ln2 dash" d="M404 164H584"/>
<path class="x" d="M485 155L503 173M503 155L485 173"/>
<text class="an mu" x="584" y="116">Owner of the partition</text>
<rect class="dead" x="584" y="134" width="140" height="60" fill="url(#d3w-h)"/>
<rect class="plate deep" x="606" y="148" width="96" height="32"/>
<text class="lb mid" x="654" y="171">narad-1</text>
<text class="an" x="740" y="170">down</text>
<path class="ln" d="M334 204V276Q334 292 350 292H405M509 292H572"/>
<path class="ah" d="M572 285L584 292L572 299Z"/>
<polygon class="msg" points="400,277 496,277 514,307 418,307"/>
<text class="msg-t" x="457" y="298.5">ord_123</text>
<circle class="sc" cx="366" cy="248" r="13"/><text class="sn" x="366" y="253.5">3</text>
<text class="an" x="388" y="254">rerouted to narad-2</text>
<rect class="bx bx3" x="584" y="262" width="140" height="60" rx="2"/>
<text class="lb mid" x="654" y="299">narad-2</text>
<path class="ln" d="M724 292H812"/>
<path class="ah" d="M812 285L824 292L812 299Z"/>
<rect class="ink" x="826" y="244" width="14" height="96"/>
<text class="lb end" x="840" y="370">Consumers</text>
<text class="an end" x="840" y="392">keep consuming</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 476" role="img" data-search-exclude aria-labelledby="d3n-t d3n-d">
<title id="d3n-t">A produce while the partition's owner is down</title>
<desc id="d3n-d">Your service sends POST /produce to narad-0, which fsyncs it and answers 202 Accepted. The partition's owner, narad-1, is down, so the hand-off to it is blocked. The message, ord_123, is rerouted to a live partition of the same topic on narad-2, and consumers keep consuming from there.</desc>
<defs><pattern id="d3n-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb deep" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="8" y="124" width="344" height="244" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="140" height="12"/>
<path class="ln" d="M64 48V124"/>
<path class="ah" d="M58 124L64 136L70 124Z"/>
<path class="ln2 dash" d="M96 136V60"/>
<path class="ah" d="M90 60L96 48L102 60Z"/>
<circle class="sc" cx="140" cy="76" r="12"/><text class="sn" x="140" y="81">1</text>
<text class="cd" x="160" y="81">POST /produce</text>
<circle class="sc" cx="140" cy="110" r="12"/><text class="sn" x="140" y="115">2</text>
<text class="cd" x="160" y="115">202 Accepted</text>
<rect class="bx bx3" x="20" y="136" width="120" height="48" rx="2"/>
<text class="lb mid" x="80" y="166">narad-0</text>
<path class="ln2 dash" d="M140 160H220"/>
<path class="x" d="M173 153L187 167M187 153L173 167"/>
<rect class="dead" x="220" y="136" width="120" height="48" fill="url(#d3n-h)"/>
<rect class="plate deep" x="236" y="146" width="88" height="28"/>
<text class="lb mid" x="280" y="166">narad-1</text>
<text class="an" x="220" y="208">owner, down</text>
<path class="ln" d="M80 184V233M80 267V308Q80 324 96 324H188"/>
<path class="ah" d="M188 318L200 324L188 330Z"/>
<polygon class="msg" points="30.5,237 112.5,237 129.5,263 47.5,263"/>
<text class="msg-t" x="80" y="255">ord_123</text>
<circle class="sc" cx="160" cy="250" r="12"/><text class="sn" x="160" y="255">3</text>
<text class="an" x="180" y="255">rerouted to narad-2</text>
<rect class="bx bx3" x="200" y="300" width="140" height="48" rx="2"/>
<text class="lb mid" x="270" y="330">narad-2</text>
<path class="ln" d="M270 348V396"/>
<path class="ah" d="M264 396L270 408L276 396Z"/>
<rect class="ink" x="200" y="408" width="140" height="12"/>
<text class="lb" x="200" y="446">Consumers</text>
<text class="an" x="200" y="468">keep consuming</text>
</svg>
</div>
<figcaption>narad-0's <code>202</code> depends only on its own disk. It reroutes at once if cluster membership already says the owner is dead, or after three failed hand-offs if it does not, which is why order across a failure is not kept.</figcaption>
</figure>
</section>

<section class="nr-band nr-band--flip nr-tint nr-tint--butter" markdown>
<div class="nr-band__copy" markdown>

## Deploys like it's nothing {#deploys}

A load balancer, a StatefulSet and a volume per pod: that is the whole architecture. Topics, users and partition owners live in Raft inside the same binary, so there is no ZooKeeper, no BookKeeper and no metadata store to run beside it.

To scale out, raise `replicaCount`. The new pod joins the cluster and the leader moves partitions onto it. The chart installs from a clone of the repository, once you have created a namespace and one secret:

```sh
helm install narad ./charts/narad \
  -n narad --set replicaCount=3
```

[Deployment, step by step](operate/index.md){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="0 28 860 450" role="img" aria-labelledby="d4w-t d4w-d">
<title id="d4w-t">The whole deployment: a StatefulSet with Raft inside every pod</title>
<desc id="d4w-d">A message from your service reaches a load balancer that spreads requests over three pods, narad-0 to narad-2, in one StatefulSet. Each pod runs Raft inside the same binary and has its own volume. A fourth pod, narad-3, is drawn dashed: raising replicaCount adds it.</desc>
<rect class="rg" x="24" y="176" width="812" height="294" rx="6"/>
<text class="lb" x="20" y="44">Your service</text>
<rect class="ink" x="20" y="60" width="14" height="76"/>
<path class="ln" d="M34 98H120M224 98H300"/>
<path class="ah" d="M300 91L312 98L300 105Z"/>
<polygon class="msg" points="115,83 211,83 229,113 133,113"/>
<text class="msg-t" x="172" y="104.5">ord_123</text>
<rect class="bx" x="312" y="70" width="200" height="56" rx="2"/>
<text class="lb mid" x="412" y="105">Load balancer</text>
<path class="ln2" d="M412 126V150M130 150H530M130 150V198M330 150V198M530 150V198"/>
<path class="ah" d="M124 198L130 210L136 198Z"/>
<path class="ah" d="M324 198L330 210L336 198Z"/>
<path class="ah" d="M524 198L530 210L536 198Z"/>
<path class="ln2 dash" d="M530 150H730V198"/>
<path class="ah" d="M724 198L730 210L736 198Z"/>
<rect class="bx" x="48" y="210" width="164" height="120" rx="2"/>
<text class="lb mid" x="130" y="243">narad-0</text>
<rect class="bx" x="248" y="210" width="164" height="120" rx="2"/>
<text class="lb mid" x="330" y="243">narad-1</text>
<rect class="bx" x="448" y="210" width="164" height="120" rx="2"/>
<text class="lb mid" x="530" y="243">narad-2</text>
<rect class="bx ghost" x="648" y="210" width="164" height="120" rx="2"/>
<text class="lb mid mu" x="730" y="243">narad-3</text>
<path class="ln2" d="M186 282H274M386 282H474"/>
<path class="ln2 dash" d="M586 282H674"/>
<rect class="bx bxq tn" x="74" y="262" width="112" height="40" rx="2"/>
<text class="an mid" x="130" y="288">Raft</text>
<rect class="bx bxq tn" x="274" y="262" width="112" height="40" rx="2"/>
<text class="an mid" x="330" y="288">Raft</text>
<rect class="bx bxq tn" x="474" y="262" width="112" height="40" rx="2"/>
<text class="an mid" x="530" y="288">Raft</text>
<rect class="bx bxq ghost" x="674" y="262" width="112" height="40" rx="2"/>
<text class="an mid mu" x="730" y="288">Raft</text>
<path class="ln2" d="M130 330V360M330 330V360M530 330V360"/>
<path class="ln2 dash" d="M730 330V360"/>
<path class="bx tn" d="M82 368V412A48 8 0 0 0 178 412V368"/><ellipse class="bx tn" cx="130" cy="368" rx="48" ry="8"/>
<text class="an mid" x="130" y="400">volume</text>
<path class="bx tn" d="M282 368V412A48 8 0 0 0 378 412V368"/><ellipse class="bx tn" cx="330" cy="368" rx="48" ry="8"/>
<text class="an mid" x="330" y="400">volume</text>
<path class="bx tn" d="M482 368V412A48 8 0 0 0 578 412V368"/><ellipse class="bx tn" cx="530" cy="368" rx="48" ry="8"/>
<text class="an mid" x="530" y="400">volume</text>
<path class="bx ghost" d="M682 368V412A48 8 0 0 0 778 412V368"/><ellipse class="bx ghost" cx="730" cy="368" rx="48" ry="8"/>
<text class="an mid mu" x="730" y="400">volume</text>
<text class="rl" x="48" y="452">StatefulSet: Raft inside every pod</text>
<text class="an mu mid" x="730" y="452">raise replicaCount</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 624" role="img" data-search-exclude aria-labelledby="d4n-t d4n-d">
<title id="d4n-t">The whole deployment: a StatefulSet with Raft inside every pod</title>
<desc id="d4n-d">A message from your service reaches a load balancer that spreads requests over three pods, narad-0 to narad-2, in one StatefulSet. Each pod runs Raft inside the same binary and has its own volume. A fourth pod, narad-3, is drawn dashed: raising replicaCount adds it.</desc>
<rect class="rg" x="44" y="222" width="308" height="394" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="140" height="12"/>
<path class="ln" d="M90 48V82M90 116V138"/>
<path class="ah" d="M84 138L90 150L96 138Z"/>
<polygon class="msg" points="40.5,86 122.5,86 139.5,112 57.5,112"/>
<text class="msg-t" x="90" y="104">ord_123</text>
<rect class="bx" x="20" y="150" width="140" height="48" rx="2"/>
<text class="lb mid" x="90" y="180">Load balancer</text>
<path class="ln2" d="M34 198V442M34 274H52M34 358H52M34 442H52"/>
<path class="ln2 dash" d="M34 442V526H52"/>
<path class="ah" d="M52 268L64 274L52 280Z"/>
<path class="ah" d="M52 352L64 358L52 364Z"/>
<path class="ah" d="M52 436L64 442L52 448Z"/>
<path class="ah" d="M52 520L64 526L52 532Z"/>
<path class="ln2" d="M197 292V340M197 376V424"/>
<path class="ln2 dash" d="M197 460V508"/>
<rect class="bx" x="64" y="246" width="180" height="56" rx="2"/>
<text class="lb" x="78" y="280">narad-0</text>
<rect class="bx bxq tn" x="160" y="256" width="74" height="36" rx="2"/>
<text class="an mid" x="197" y="279">Raft</text>
<rect class="bx" x="64" y="330" width="180" height="56" rx="2"/>
<text class="lb" x="78" y="364">narad-1</text>
<rect class="bx bxq tn" x="160" y="340" width="74" height="36" rx="2"/>
<text class="an mid" x="197" y="363">Raft</text>
<rect class="bx" x="64" y="414" width="180" height="56" rx="2"/>
<text class="lb" x="78" y="448">narad-2</text>
<rect class="bx bxq tn" x="160" y="424" width="74" height="36" rx="2"/>
<text class="an mid" x="197" y="447">Raft</text>
<rect class="bx ghost" x="64" y="498" width="180" height="56" rx="2"/>
<text class="lb mu" x="78" y="532">narad-3</text>
<rect class="bx bxq ghost" x="160" y="508" width="74" height="36" rx="2"/>
<text class="an mid mu" x="197" y="531">Raft</text>
<path class="ln2" d="M244 274H264M244 358H264M244 442H264"/>
<path class="ln2 dash" d="M244 526H264"/>
<path class="bx tn" d="M264 258V288A38 6 0 0 0 340 288V258"/><ellipse class="bx tn" cx="302" cy="258" rx="38" ry="6"/>
<text class="an mid" x="302" y="284">volume</text>
<path class="bx tn" d="M264 342V372A38 6 0 0 0 340 372V342"/><ellipse class="bx tn" cx="302" cy="342" rx="38" ry="6"/>
<text class="an mid" x="302" y="368">volume</text>
<path class="bx tn" d="M264 426V456A38 6 0 0 0 340 456V426"/><ellipse class="bx tn" cx="302" cy="426" rx="38" ry="6"/>
<text class="an mid" x="302" y="452">volume</text>
<path class="bx ghost" d="M264 510V540A38 6 0 0 0 340 540V510"/><ellipse class="bx ghost" cx="302" cy="510" rx="38" ry="6"/>
<text class="an mid mu" x="302" y="536">volume</text>
<text class="rl" x="64" y="596">StatefulSet</text>
<text class="an mu end" x="336" y="596">raise replicaCount</text>
</svg>
</div>
<figcaption>Raise <code>replicaCount</code> and a new pod, drawn dashed, joins the cluster. The leader then moves partitions onto it, copying each one before it cuts over.</figcaption>
</figure>
</section>

<section class="nr-print nr-tint nr-tint--slate" markdown>
<div class="nr-print__head" markdown>

## The fine print, up front {#fine-print}

What a `202` promises, and what Narad trades for it.

</div>
<div class="nr-rows nr-rows--one" markdown>

- **A `202` means fsynced to disk.** Delivery is at least once, so handlers must be idempotent. A nightly run kills and partitions a three-node cluster under load, and fails on any anomaly the contract does not explain. [The delivery contract, checked nightly](internals/linearizability.md){ .nr-more }
- **Ordering is not guaranteed.** Redelivery and rerouting around a dead node both reorder messages. Carry a sequence in the payload if you need one. [Every way order breaks](client/guarantees-and-errors.md#ordering-not-guaranteed){ .nr-more }
- **Each partition is one copy on one volume.** Crashes and restarts lose nothing; a destroyed disk loses that node's partitions. For a second copy, add a replica child or snapshot the volumes. [Replication, when you ask for it](client/fanout-and-delay.md#replication-when-you-ask-for-it){ .nr-more }
- **Fsync costs throughput.** On one shared 2 CPU / 2 GB box, Narad produced 5,597 msg/s, last of six brokers; RabbitMQ's quorum queue, the only other one there that fsyncs before it confirms, was about 2.3 times faster. Three nodes of 4.5 vCPU sustained 50,000 msg/s through produce, consume and ack, and the ceiling is not measured yet. Both runs predate [batch produce](client/producing.md#producing-in-batches), which sends up to 100 messages for one fsync. [Same compute, measured](compare.md#same-compute-measured-ourselves){ .nr-more }

</div>
</section>

<section class="nr-also" markdown>

## Also in the one binary {#also}

<div class="nr-rows" markdown>

- **[Fan-out children](client/fanout-and-delay.md)** Every message committed to a parent is copied into each child, with its own consumers and retention. Producers change nothing.
- **[Replica children](client/fanout-and-delay.md#replication-when-you-ask-for-it)** A child whose partitions are placed on other nodes than the parent's: an async second copy of a topic, from one API call.
- **[Delay children](client/fanout-and-delay.md#delay-children)** A child with `delay_ms` receives each message that long after the parent committed it. Retry queues need no scheduler.
- **[Schemas at the broker](client/schemas.md)** Give a topic a JSON Schema and a produce that does not fit gets `400` naming the field. It never reaches the log.
- **[Any payload](client/consuming.md#the-payload-comes-back-the-way-you-sent-it)** Send JSON, text or raw bytes as `application/octet-stream`. JSON comes back verbatim, text as text, and binary as base64 with a flag that says so.
- **[A Go SDK and a CLI](client/go-sdk.md)** The Go client renews leases, retries with jitter and trips per-node circuit breakers, on the standard library alone. The `narad` binary is the broker and the CLI in one.

</div>

</section>

<section class="nr-try nr-tint nr-tint--mint" markdown>
<div class="nr-try__copy" markdown>

## Sixty seconds on your laptop {#try-it}

The `narad` binary is both the broker and the CLI. `narad server start --dev` runs one node on `127.0.0.1:7942` with auth off, which is what the session at the top of this page talked to.

[The sixty-second demo](client/cli.md#the-sixty-second-demo){ .nr-more } · [Getting started](client/index.md){ .nr-more } · [Go SDK](client/go-sdk.md){ .nr-more }

</div>
<div class="nr-try__code" markdown>

=== "Homebrew"

    ```sh
    brew install debanganthakuria/narad/narad
    narad server start --dev
    ```

=== "Docker"

    ```sh
    docker run -p 7942:7942 -v narad-data:/var/lib/narad \
      -e NARAD_SECURITY_ENABLED=false \
      ghcr.io/debanganthakuria/narad:v3.0.1
    ```

</div>
</section>
