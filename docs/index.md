---
template: home.html
description: "Narad is a message broker in one Go binary. POST a message to any node and get 202 once it is fsynced to disk; consumers pull it, work under a lease and ack."
hide:
  - navigation
  - toc
  - path
---

<div class="nr-hero" markdown>

# <span class="nr-line">A small, sturdy message broker.</span> <span class="nr-line">HTTP in, <span class="nr-nowrap">at-least-once</span> out.</span>

<div class="nr-hero__pitch" markdown>

<p class="nr-lead">When Narad answers <code>202</code>, your message is already fsynced to disk. It is one Go binary: POST to any node, pull the message, work on it under a lease, ack it. Anything left unacked comes back.</p>

[Run it locally](#try-it){ .md-button .md-button--primary }

</div>

<figure class="nr-term" aria-labelledby="nr-term-caption">
<div class="nr-term__bar" aria-hidden="true"><span>Terminal</span></div>
<pre><code><span class="p">$</span> NARAD=http://127.0.0.1:7942
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
<figcaption id="nr-term-caption">A recorded session against <code>narad server start --dev</code>, after creating the topic <code>orders</code>. <span class="nr-when-wide">The consume response is one line; it is wrapped here to fit.</span><span class="nr-when-narrow">Long lines are wrapped here to fit.</span></figcaption>
</figure>

<div class="nr-doors" markdown>

- [<span class="nr-doors__tab">Build<span class="nr-sr">:</span></span> <span class="nr-doors__what">Produce, consume and ack from your service.</span>](build/connect.md)
- [<span class="nr-doors__tab">Operate<span class="nr-sr">:</span></span> <span class="nr-doors__what">Deploy with Helm, monitor, scale, upgrade.</span>](operate/deploy-kubernetes.md)
- [<span class="nr-doors__tab">Understand<span class="nr-sr">:</span></span> <span class="nr-doors__what">Storage, Raft, rebalance, the delivery contract.</span>](understand/index.md)
- [<span class="nr-doors__tab">Compare<span class="nr-sr">:</span></span> <span class="nr-doors__what">Kafka, NATS, RabbitMQ, SQS, Redis, Pulsar.</span>](get-started/compare.md)

</div>

</div>

<section class="nr-band nr-tint nr-tint--mint" markdown>
<div class="nr-band__copy" markdown>

## Hit any pod. Narad does the rest. {#any-pod}

Put every pod behind one load balancer and send it every produce, consume and ack. Whichever pod catches the request is the right one: your client never looks for a leader, never learns a partition map and never speaks a metadata protocol.

That pod appends the message to its own write-ahead log and fsyncs before it answers `202`, so you wait for one local fsync. After the `202` it hands the message to the partition's owner and retries until the owner has fsynced it, read it back and verified it. Only then do consumers see it.

[The produce path, step by step](understand/produce-path.md){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="20 0 840 424" role="img" aria-labelledby="d1w-t" aria-describedby="d1w-d">
<title id="d1w-t">A produce, answered by whichever pod catches it</title>
<desc id="d1w-d">Your service sends POST /produce to the load balancer, which passes it to narad-1; narad-0 or any other pod would have done as well. narad-1 appends the message to its write-ahead log, fsyncs, and answers 202 Accepted, so the producer waited for one fsync. After the 202, in the background, narad-1 hands the message, ord_123, to narad-2, the partition's owner, and retries until narad-2 has fsynced it, read it back and verified it.</desc>
<rect class="rg" x="438" y="20" width="414" height="392" rx="6"/>
<text class="rl" x="458" y="50">Narad cluster</text>
<text class="lb" x="20" y="116">Your service</text>
<rect class="ink" x="20" y="132" width="14" height="132"/>
<path class="ln" d="M34 176H238"/>
<path class="ah" d="M238 169L250 176L238 183Z"/>
<path class="ln1" d="M250 212H37"/>
<path class="ah-o" d="M47 205L36 212L47 219"/>
<circle class="sc" cx="60" cy="152" r="13"/><text class="sn" x="60" y="157.5">1</text>
<text class="cd" x="82" y="158">POST /produce</text>
<circle class="sc" cx="60" cy="238" r="13"/><text class="sn" x="60" y="243.5">3</text>
<text class="cd" x="82" y="244">202 Accepted</text>
<text class="an" x="82" y="270">you waited for one fsync</text>
<rect class="bx" x="250" y="148" width="160" height="92" rx="2"/>
<text class="lb mid" x="330" y="201">Load balancer</text>
<path class="ln" d="M410 176H458"/>
<path class="ah" d="M458 169L470 176L458 183Z"/>
<path class="ln1" d="M470 212H413"/>
<path class="ah-o" d="M423 205L412 212L423 219"/>
<path class="ln1 dash" d="M330 148V114Q330 98 346 98H460"/>
<path class="ah" d="M460 93L470 98L460 103Z"/>
<rect class="bx bxq" x="470" y="72" width="150" height="52" rx="2"/>
<text class="pod mu" x="545" y="104">narad-0</text>
<text class="an mu" x="636" y="104">or any other pod</text>
<rect class="bx bx3" x="470" y="148" width="150" height="92" rx="2"/>
<text class="pod" x="545" y="200">narad-1</text>
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
<text class="an mu" x="636" y="272">after step 3,</text>
<text class="an mu" x="636" y="294">in the background</text>
<rect class="bx" x="470" y="324" width="150" height="56" rx="2"/>
<text class="pod" x="545" y="358">narad-2</text>
<circle class="sc" cx="646" cy="338" r="13"/><text class="sn" x="646" y="343.5">4</text>
<text class="lb" x="668" y="344">The owner</text>
<text class="an" x="668" y="368">fsyncs, reads back,</text>
<text class="an" x="668" y="390">verifies</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 584" role="img" data-search-exclude aria-labelledby="d1n-t" aria-describedby="d1n-d">
<title id="d1n-t">A produce, answered by whichever pod catches it</title>
<desc id="d1n-d">Your service sends POST /produce to the load balancer, which passes it to narad-1; narad-0 or any other pod would have done as well. narad-1 appends the message to its write-ahead log, fsyncs, and answers 202 Accepted, so the producer waited for one fsync. After the 202, in the background, narad-1 hands the message, ord_123, to narad-2, the partition's owner, and retries until narad-2 has fsynced it, read it back and verified it.</desc>
<rect class="rg" x="8" y="228" width="344" height="344" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="320" height="12"/>
<path class="ln" d="M236 48V144"/>
<path class="ah" d="M230 144L236 156L242 144Z"/>
<path class="ln1" d="M280 156V51"/>
<path class="ah-o" d="M274 61L280 50L286 61"/>
<circle class="sc" cx="32" cy="78" r="12"/><text class="sn" x="32" y="83">1</text>
<text class="cd" x="52" y="83">POST /produce</text>
<circle class="sc" cx="32" cy="112" r="12"/><text class="sn" x="32" y="117">3</text>
<text class="cd" x="52" y="117">202 Accepted</text>
<text class="an" x="52" y="140">you waited for one fsync</text>
<rect class="bx" x="150" y="156" width="190" height="48" rx="2"/>
<text class="lb mid" x="245" y="186">Load balancer</text>
<path class="ln" d="M236 204V264"/>
<path class="ah" d="M230 264L236 276L242 264Z"/>
<path class="ln1" d="M280 276V207"/>
<path class="ah-o" d="M274 217L280 206L286 217"/>
<path class="ln1 dash" d="M150 180H100Q84 180 84 196V266"/>
<path class="ah" d="M79 266L84 276L89 266Z"/>
<rect class="bx bxq" x="24" y="276" width="120" height="48" rx="2"/>
<text class="pod mu" x="84" y="305">narad-0</text>
<text class="an mu" x="24" y="346">or any other pod</text>
<rect class="bx bx3" x="168" y="276" width="140" height="56" rx="2"/>
<text class="pod" x="238" y="309">narad-1</text>
<path class="ln2" d="M238 332V347"/>
<path class="bx tn" d="M208 354V382A30 7 0 0 0 268 382V354"/>
<ellipse class="bx tn" cx="238" cy="354" rx="30" ry="7"/>
<text class="an mid" x="238" y="379">WAL</text>
<circle class="sc" cx="288" cy="370" r="12"/><text class="sn" x="288" y="375">2</text>
<text class="an" x="306" y="375">fsync</text>
<path class="ln" d="M238 389V408M238 442V468"/>
<path class="ah" d="M232 468L238 480L244 468Z"/>
<polygon class="msg" points="188.5,412 270.5,412 287.5,438 205.5,438"/>
<text class="msg-t" x="238" y="430">ord_123</text>
<text class="an mu end" x="178" y="422">after step 3,</text>
<text class="an mu end" x="178" y="442">in the background</text>
<rect class="bx" x="168" y="480" width="140" height="48" rx="2"/>
<text class="pod" x="238" y="509">narad-2</text>
<circle class="sc" cx="36" cy="494" r="12"/><text class="sn" x="36" y="499">4</text>
<text class="lb" x="56" y="500">The owner</text>
<text class="an" x="56" y="520">fsyncs,</text>
<text class="an" x="56" y="540">reads back,</text>
<text class="an" x="56" y="560">verifies</text>
<text class="rl end" x="340" y="558">Narad cluster</text>
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

[Consuming: leases, acks, extends and nacks](build/consuming.md){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="20 24 840 352" role="img" aria-labelledby="d2w-t" aria-describedby="d2w-d">
<title id="d2w-t">A crashed worker's message comes back</title>
<desc id="d2w-d">Three workers pull from the topic orders, with no consumer group. Worker 1 consumes and acks. Worker 2 takes ord_123 on a 30 second lease and crashes without acking. The lease runs out and ord_123 goes back into the topic. Worker 3, just started, takes it and acks with 204.</desc>
<defs><pattern id="d2w-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="20" y="40" width="140" height="324" rx="6"/>
<text class="rl" x="40" y="72">Topic</text>
<text class="cd cdb" x="40" y="104">orders</text>
<rect class="bx bxq tn" x="40" y="124" width="100" height="216" rx="2"/>
<path class="ln1" d="M40 160H140M40 196H140M40 232H140M40 268H140M40 304H140"/>
<path class="ln1" d="M140 142H562"/>
<path class="ah" d="M562 137L572 142L562 147Z"/>
<text class="an mu" x="200" y="129">consume, ack</text>
<rect class="you" x="572" y="114" width="160" height="56" rx="2"/>
<text class="lb mid on" x="652" y="149">worker 1</text>
<path class="ln" d="M140 214H560"/>
<path class="ah" d="M560 207L572 214L560 221Z"/>
<circle class="sc" cx="178" cy="190" r="13"/><text class="sn" x="178" y="195.5">1</text>
<text class="an" x="200" y="196">consume, 30 s lease</text>
<path class="ln dash" d="M572 250H439M335 250H152"/>
<path class="ah" d="M152 243L140 250L152 257Z"/>
<polygon class="msg" points="330,235 426,235 444,265 348,265"/>
<text class="msg-t" x="387" y="256.5">ord_123</text>
<circle class="sc" cx="178" cy="284" r="13"/><text class="sn" x="178" y="289.5">3</text>
<text class="an" x="200" y="290">lease runs out: it comes back</text>
<rect class="dead" x="572" y="204" width="160" height="56" fill="url(#d2w-h)"/>
<rect class="plate" x="596" y="216" width="112" height="32"/>
<text class="lb mid" x="652" y="239">worker 2</text>
<circle class="sc" cx="762" cy="232" r="13"/><text class="sn" x="762" y="237.5">2</text>
<text class="an" x="784" y="238">crashed</text>
<path class="ln" d="M140 322H560"/>
<path class="ah" d="M560 315L572 322L560 329Z"/>
<circle class="sc" cx="178" cy="346" r="13"/><text class="sn" x="178" y="351.5">4</text>
<text class="an" x="200" y="352">consume and ack</text>
<rect class="you" x="572" y="294" width="160" height="56" rx="2"/>
<text class="lb mid on" x="652" y="329">worker 3</text>
<text class="an mu" x="752" y="328">just started</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 528" role="img" data-search-exclude aria-labelledby="d2n-t" aria-describedby="d2n-d">
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
<path class="sep" d="M16 404H344"/>
<circle class="sc" cx="28" cy="431" r="12"/><text class="sn" x="28" y="436">1</text>
<text class="an" x="50" y="436">Worker 2 takes it for 30 s</text>
<circle class="sc" cx="28" cy="459" r="12"/><text class="sn" x="28" y="464">2</text>
<text class="an" x="50" y="464">Worker 2 crashes, never acks</text>
<circle class="sc" cx="28" cy="487" r="12"/><text class="sn" x="28" y="492">3</text>
<text class="an" x="50" y="492">The lease runs out: it comes back</text>
<circle class="sc" cx="28" cy="515" r="12"/><text class="sn" x="28" y="520">4</text>
<text class="an" x="50" y="520">Worker 3 takes it and acks</text>
</svg>
</div>
<figcaption>Worker 2 may have done part of the job before it died, so <code>ord_123</code> can run twice. That is at-least-once: make handlers idempotent.</figcaption>
</figure>
</section>

<section class="nr-band nr-tint nr-tint--lilac" markdown>
<div class="nr-band__copy" markdown>

## Built to say yes {#say-yes}

Any live node accepts a produce with a local fsync: no leader election and no quorum on the write path, so losing a minority of nodes never stops produces. If a partition's owner is down, the message is committed to a live partition of the same topic instead, and consumers keep consuming.

The price, stated up front: **ordering is not guaranteed.** Messages already stored on the dead node wait for it to come back, and their partition answers `503` until then. If you need a sequence, carry one in the payload.

[The availability trade, in full](understand/delivery-contract.md#availability){ .nr-more }

</div>
<figure class="nr-dia">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="20 56 840 348" role="img" aria-labelledby="d3w-t" aria-describedby="d3w-d">
<title id="d3w-t">A produce while the partition's owner is down</title>
<desc id="d3w-d">Your service sends POST /produce to narad-0, which fsyncs it and answers 202 Accepted. The partition's owner, narad-1, is down, so the hand-off to it is blocked. The message, ord_123, is rerouted to a live partition of the same topic on narad-2, and consumers keep consuming from there.</desc>
<defs><pattern id="d3w-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb deep" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="236" y="64" width="560" height="284" rx="6"/>
<text class="rl" x="256" y="96">Narad cluster</text>
<text class="lb" x="20" y="82">Your service</text>
<rect class="ink" x="20" y="100" width="14" height="120"/>
<path class="ln" d="M34 146H252"/>
<path class="ah" d="M252 139L264 146L252 153Z"/>
<path class="ln1" d="M264 182H37"/>
<path class="ah-o" d="M47 175L36 182L47 189"/>
<circle class="sc" cx="60" cy="122" r="13"/><text class="sn" x="60" y="127.5">1</text>
<text class="cd" x="82" y="128">POST /produce</text>
<circle class="sc" cx="60" cy="206" r="13"/><text class="sn" x="60" y="211.5">2</text>
<text class="cd" x="82" y="212">202 Accepted</text>
<rect class="bx bx3" x="264" y="124" width="140" height="80" rx="2"/>
<text class="pod" x="334" y="170">narad-0</text>
<path class="ln2 dash" d="M404 164H584"/>
<path class="x" d="M485 155L503 173M503 155L485 173"/>
<text class="an mu" x="584" y="116">Owner of the partition</text>
<rect class="dead" x="584" y="134" width="140" height="60" fill="url(#d3w-h)"/>
<rect class="plate deep" x="606" y="148" width="96" height="32"/>
<text class="pod" x="654" y="170">narad-1</text>
<text class="an" x="740" y="170">down</text>
<path class="ln" d="M334 204V276Q334 292 350 292H405M509 292H572"/>
<path class="ah" d="M572 285L584 292L572 299Z"/>
<polygon class="msg" points="400,277 496,277 514,307 418,307"/>
<text class="msg-t" x="457" y="298.5">ord_123</text>
<circle class="sc" cx="366" cy="248" r="13"/><text class="sn" x="366" y="253.5">3</text>
<text class="an" x="388" y="254">rerouted to narad-2</text>
<rect class="bx bx3" x="584" y="262" width="140" height="60" rx="2"/>
<text class="pod" x="654" y="298">narad-2</text>
<path class="ln" d="M724 292H812"/>
<path class="ah" d="M812 285L824 292L812 299Z"/>
<rect class="ink" x="826" y="244" width="14" height="96"/>
<text class="lb end" x="840" y="370">Consumers</text>
<text class="an end" x="840" y="392">keep consuming</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 476" role="img" data-search-exclude aria-labelledby="d3n-t" aria-describedby="d3n-d">
<title id="d3n-t">A produce while the partition's owner is down</title>
<desc id="d3n-d">Your service sends POST /produce to narad-0, which fsyncs it and answers 202 Accepted. The partition's owner, narad-1, is down, so the hand-off to it is blocked. The message, ord_123, is rerouted to a live partition of the same topic on narad-2, and consumers keep consuming from there.</desc>
<defs><pattern id="d3n-h" width="10" height="10" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect class="hb deep" width="10" height="10"/><path class="hl" d="M0 0V10"/></pattern></defs>
<rect class="rg" x="8" y="124" width="344" height="244" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="140" height="12"/>
<path class="ln" d="M64 48V124"/>
<path class="ah" d="M58 124L64 136L70 124Z"/>
<path class="ln1" d="M96 136V51"/>
<path class="ah-o" d="M90 61L96 50L102 61"/>
<circle class="sc" cx="140" cy="76" r="12"/><text class="sn" x="140" y="81">1</text>
<text class="cd" x="160" y="81">POST /produce</text>
<circle class="sc" cx="140" cy="110" r="12"/><text class="sn" x="140" y="115">2</text>
<text class="cd" x="160" y="115">202 Accepted</text>
<rect class="bx bx3" x="20" y="136" width="120" height="48" rx="2"/>
<text class="pod" x="80" y="165">narad-0</text>
<path class="ln2 dash" d="M140 160H220"/>
<path class="x" d="M173 153L187 167M187 153L173 167"/>
<rect class="dead" x="220" y="136" width="120" height="48" fill="url(#d3n-h)"/>
<rect class="plate deep" x="236" y="146" width="88" height="28"/>
<text class="pod" x="280" y="165">narad-1</text>
<text class="an" x="220" y="208">owner, down</text>
<path class="ln" d="M80 184V233M80 267V308Q80 324 96 324H188"/>
<path class="ah" d="M188 318L200 324L188 330Z"/>
<polygon class="msg" points="30.5,237 112.5,237 129.5,263 47.5,263"/>
<text class="msg-t" x="80" y="255">ord_123</text>
<circle class="sc" cx="160" cy="250" r="12"/><text class="sn" x="160" y="255">3</text>
<text class="an" x="180" y="255">rerouted to narad-2</text>
<rect class="bx bx3" x="200" y="300" width="140" height="48" rx="2"/>
<text class="pod" x="270" y="329">narad-2</text>
<text class="rl" x="24" y="358">Narad cluster</text>
<path class="ln" d="M270 348V396"/>
<path class="ah" d="M264 396L270 408L276 396Z"/>
<rect class="ink" x="200" y="408" width="140" height="12"/>
<text class="lb" x="200" y="446">Consumers</text>
<text class="an" x="200" y="468">keep consuming</text>
</svg>
</div>
<figcaption>The <code>202</code> from <code>narad-0</code> depends only on its own disk. It reroutes at once if cluster membership already says the owner is dead, or after 3 seconds of failed hand-offs if it does not. Either way the message lands on a different partition from the messages before it, so order across a failure is not kept.</figcaption>
</figure>
</section>

<section class="nr-band nr-band--wide nr-tint nr-tint--butter" markdown>

## Deploys like it's nothing {#deploys}

<div class="nr-band__copy" markdown>

A load balancer, a StatefulSet and a volume per pod: that is the whole architecture. Topics, users and partition owners live in Raft inside the same binary, so there is no ZooKeeper, no BookKeeper and no metadata store to run beside it.

</div>
<div class="nr-band__copy" markdown>

To scale out, raise `replicaCount`. The new pod joins the cluster and the leader moves partitions onto it. The chart installs from a clone of the repository, once you have created a namespace and one secret:

```sh
helm install narad ./charts/narad \
  -n narad --set replicaCount=3 \
  --set image.tag=v3.0.1
```

[Deployment, step by step](operate/deploy-kubernetes.md){ .nr-more }

</div>
<figure class="nr-dia nr-dia--pano">
<div class="nr-dia__frame">
<svg class="nr-dia__wide" viewBox="20 16 1160 470" role="img" aria-labelledby="d4w-t" aria-describedby="d4w-d">
<title id="d4w-t">Scaling out: a new pod joins and a partition moves onto it</title>
<desc id="d4w-d">Your service sends requests to a load balancer that spreads them over the pods of one StatefulSet, narad-0 to narad-2, each with its own volume. Raft runs inside every pod: narad-0 holds the Raft leader, which replicates to each of the others. A fourth pod, narad-3, is drawn dashed: raising replicaCount adds it, it joins the cluster, and a partition, orders/1, is copied from narad-0 onto it before ownership cuts over.</desc>
<rect class="rg" x="28" y="166" width="1130" height="304" rx="6"/>
<text class="lb" x="20" y="40">Your service</text>
<rect class="ink" x="20" y="56" width="14" height="64"/>
<path class="ln" d="M34 88H250"/>
<path class="ah" d="M250 81L262 88L250 95Z"/>
<rect class="bx" x="262" y="60" width="200" height="56" rx="2"/>
<text class="lb mid" x="362" y="95">Load balancer</text>
<path class="ln2" d="M362 116V150M128 150H688M128 150V178M408 150V178M688 150V178"/>
<path class="ah" d="M122 178L128 190L134 178Z"/>
<path class="ah" d="M402 178L408 190L414 178Z"/>
<path class="ah" d="M682 178L688 190L694 178Z"/>
<path class="ln2 dash" d="M688 150H968V178"/>
<path class="ah" d="M962 178L968 190L974 178Z"/>
<rect class="bx" x="48" y="190" width="160" height="120" rx="2"/>
<text class="pod" x="128" y="224">narad-0</text>
<rect class="bx bxq tn" x="68" y="244" width="120" height="44" rx="2"/>
<text class="an mid" x="128" y="272">Raft leader</text>
<rect class="bx" x="328" y="190" width="160" height="120" rx="2"/>
<text class="pod" x="408" y="224">narad-1</text>
<rect class="bx bxq tn" x="348" y="244" width="120" height="44" rx="2"/>
<text class="an mid" x="408" y="272">Raft</text>
<rect class="bx" x="608" y="190" width="160" height="120" rx="2"/>
<text class="pod" x="688" y="224">narad-2</text>
<rect class="bx bxq tn" x="628" y="244" width="120" height="44" rx="2"/>
<text class="an mid" x="688" y="272">Raft</text>
<rect class="bx ghost" x="888" y="190" width="160" height="120" rx="2"/>
<text class="pod mu" x="968" y="224">narad-3</text>
<rect class="bx bxq ghost" x="908" y="244" width="120" height="44" rx="2"/>
<text class="an mid mu" x="968" y="272">Raft</text>
<path class="ln2" d="M208 250H222M488 250H502M768 250H782"/>
<path class="ln2 dash" d="M1048 250H1062"/>
<path class="bx tn" d="M222 222V278A38 8 0 0 0 298 278V222"/><ellipse class="bx tn" cx="260" cy="222" rx="38" ry="8"/>
<text class="an mid" x="260" y="260">volume</text>
<path class="bx tn" d="M502 222V278A38 8 0 0 0 578 278V222"/><ellipse class="bx tn" cx="540" cy="222" rx="38" ry="8"/>
<text class="an mid" x="540" y="260">volume</text>
<path class="bx tn" d="M782 222V278A38 8 0 0 0 858 278V222"/><ellipse class="bx tn" cx="820" cy="222" rx="38" ry="8"/>
<text class="an mid" x="820" y="260">volume</text>
<path class="bx ghost" d="M1062 222V278A38 8 0 0 0 1138 278V222"/><ellipse class="bx ghost" cx="1100" cy="222" rx="38" ry="8"/>
<text class="an mid mu" x="1100" y="260">volume</text>
<path class="ln1" d="M168 310V338H648M368 338V322M648 338V322"/>
<path class="ah" d="M363 322L368 312L373 322Z"/>
<path class="ah" d="M643 322L648 312L653 322Z"/>
<path class="ln1 dash" d="M648 338H928V322"/>
<path class="ah" d="M923 322L928 312L933 322Z"/>
<text class="an mu" x="184" y="364">Raft: the leader to each follower</text>
<path class="ln" d="M88 310V396H505M629 396H1008V322"/>
<path class="ah" d="M1001 322L1008 310L1015 322Z"/>
<polygon class="msg" points="500,381 616,381 634,411 518,411"/>
<text class="msg-t" x="567" y="402.5">orders/1</text>
<text class="an" x="518" y="438">copied, then cut over</text>
<text class="rl" x="48" y="456">StatefulSet</text>
<text class="an mu end" x="1138" y="456">raise replicaCount</text>
</svg>
<svg class="nr-dia__narrow" viewBox="0 0 360 712" role="img" data-search-exclude aria-labelledby="d4n-t" aria-describedby="d4n-d">
<title id="d4n-t">Scaling out: a new pod joins and a partition moves onto it</title>
<desc id="d4n-d">Your service sends requests to a load balancer that spreads them over the pods of one StatefulSet, narad-0 to narad-2, each with its own volume. Raft runs inside every pod: narad-0 holds the Raft leader, which replicates to each of the others. A fourth pod, narad-3, is drawn dashed: raising replicaCount adds it, it joins the cluster, and a partition, orders/1, is copied from narad-0 onto it before ownership cuts over.</desc>
<rect class="rg" x="44" y="222" width="308" height="478" rx="6"/>
<text class="lb" x="20" y="26">Your service</text>
<rect class="ink" x="20" y="36" width="140" height="12"/>
<path class="ln" d="M90 48V138"/>
<path class="ah" d="M84 138L90 150L96 138Z"/>
<rect class="bx" x="20" y="150" width="140" height="48" rx="2"/>
<text class="lb mid" x="90" y="180">Load balancer</text>
<text class="rl" x="60" y="248">StatefulSet</text>
<text class="an mu end" x="340" y="248">raise replicaCount</text>
<path class="ln2" d="M34 198V492M34 300H48M34 396H48M34 492H48"/>
<path class="ln2 dash" d="M34 492V588H48"/>
<path class="ah" d="M48 294L60 300L48 306Z"/>
<path class="ah" d="M48 390L60 396L48 402Z"/>
<path class="ah" d="M48 486L60 492L48 498Z"/>
<path class="ah" d="M48 582L60 588L48 594Z"/>
<rect class="bx" x="60" y="262" width="116" height="76" rx="2"/>
<text class="pod" x="118" y="289">narad-0</text>
<rect class="bx bxq tn" x="70" y="300" width="96" height="28" rx="2"/>
<text class="an mid" x="118" y="319">Raft leader</text>
<rect class="bx" x="60" y="358" width="116" height="76" rx="2"/>
<text class="pod" x="118" y="385">narad-1</text>
<rect class="bx bxq tn" x="70" y="396" width="96" height="28" rx="2"/>
<text class="an mid" x="118" y="415">Raft</text>
<rect class="bx" x="60" y="454" width="116" height="76" rx="2"/>
<text class="pod" x="118" y="481">narad-2</text>
<rect class="bx bxq tn" x="70" y="492" width="96" height="28" rx="2"/>
<text class="an mid" x="118" y="511">Raft</text>
<rect class="bx ghost" x="60" y="550" width="116" height="76" rx="2"/>
<text class="pod mu" x="118" y="577">narad-3</text>
<rect class="bx bxq ghost" x="70" y="588" width="96" height="28" rx="2"/>
<text class="an mid mu" x="118" y="607">Raft</text>
<path class="ln2" d="M176 306H188M176 402H188M176 498H188"/>
<path class="ln2 dash" d="M176 594H188"/>
<path class="bx tn" d="M188 286V322A30 6 0 0 0 248 322V286"/><ellipse class="bx tn" cx="218" cy="286" rx="30" ry="6"/>
<text class="an mid" x="218" y="314">volume</text>
<path class="bx tn" d="M188 382V418A30 6 0 0 0 248 418V382"/><ellipse class="bx tn" cx="218" cy="382" rx="30" ry="6"/>
<text class="an mid" x="218" y="410">volume</text>
<path class="bx tn" d="M188 478V514A30 6 0 0 0 248 514V478"/><ellipse class="bx tn" cx="218" cy="478" rx="30" ry="6"/>
<text class="an mid" x="218" y="506">volume</text>
<path class="bx ghost" d="M188 574V610A30 6 0 0 0 248 610V574"/><ellipse class="bx ghost" cx="218" cy="574" rx="30" ry="6"/>
<text class="an mid mu" x="218" y="602">volume</text>
<path class="ln" d="M176 270H290Q306 270 306 286V634Q306 650 290 650H261.5M157.5 650H134Q118 650 118 638"/>
<path class="ah" d="M112 638L118 626L124 638Z"/>
<polygon class="msg" points="153,637 249,637 266,663 170,663"/>
<text class="msg-t" x="209.5" y="655">orders/1</text>
<text class="an mid" x="209.5" y="688">copied, then cut over</text>
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

- **A `202` means fsynced to disk.** Delivery is at least once, so handlers must be idempotent. A nightly run kills and partitions a three-node cluster at 300 messages a second, and fails on any anomaly the contract does not explain. [The delivery contract, checked nightly](understand/linearizability.md){ .nr-more }
- **Ordering is not guaranteed.** Redelivery and rerouting around a dead node both reorder messages. Carry a sequence in the payload if you need one. [Every way order breaks](understand/delivery-contract.md#ordering){ .nr-more }
- **Each partition is one copy on one volume.** Crashes and restarts lose nothing; a destroyed disk loses that node's partitions. For a second copy, add a replica child or snapshot the volumes. [Replication, when you ask for it](operate/backups.md#replica-children){ .nr-more }
- **Fsync costs throughput.** On one shared 2 CPU / 2 GB box with 256-byte messages, Narad produced 5,597 msg/s, last of six brokers; RabbitMQ's quorum queue, the only other one there that fsyncs before it confirms, was about 2.3 times faster. [Same compute, measured](get-started/compare.md#same-compute-measured-ourselves){ .nr-more }

</div>
</section>

<section class="nr-also" markdown>

## Also in the one binary {#also}

<div class="nr-rows" markdown>

- **[Fan-out children](build/fanout-and-delay.md)** Every message committed to a parent is copied into each child, with its own consumers and retention. Producers change nothing.
- **[Replica children](operate/backups.md#replica-children)** A child whose partitions are placed on other nodes than the parent's: an async second copy of a topic, from one API call.
- **[Delay children](build/fanout-and-delay.md#delay-children)** A child with `delay_ms` receives each message that long after the parent committed it: delayed work with no scheduler.
- **[Schemas at the broker](build/schemas.md)** Give a topic a JSON Schema and a produce that does not fit gets `400` naming the field. It never reaches the log.
- **[Any payload](build/consuming.md#the-payload-comes-back-the-way-you-sent-it)** Send JSON, text or raw bytes as `application/octet-stream`. JSON comes back verbatim, text as text, and binary as base64 with a flag that says so.
- **[A Go SDK and a CLI](build/go-sdk.md)** The Go client renews leases, retries with jitter and trips per-node circuit breakers, on the standard library alone. The `narad` binary is the broker and the CLI in one.

</div>

</section>

<section class="nr-try nr-tint nr-tint--mint" markdown>
<div class="nr-try__copy" markdown>

## Sixty seconds on your laptop {#try-it}

The `narad` binary is both the broker and the CLI. `narad server start --dev` runs one node on `127.0.0.1:7942` with auth off, and the Docker command runs the same. That is what the session at the top of this page talked to.

[The sixty-second demo](build/cli.md#watch-messages-flow){ .nr-more } · [Getting started](get-started/quickstart.md){ .nr-more } · [Go SDK](build/go-sdk.md){ .nr-more }

</div>
<div class="nr-try__code" markdown>

=== "Docker"

    ```sh
    docker run --rm -p 127.0.0.1:7942:7942 \
      -v narad-data:/var/lib/narad \
      -e NARAD_SECURITY_ENABLED=false \
      -e NARAD_CLUSTER_ADDR=127.0.0.1:7943 \
      ghcr.io/debanganthakuria/narad:v3.0.1
    ```

=== "Homebrew"

    ```sh
    # builds from source: a minute or more
    brew install debanganthakuria/narad/narad
    narad server start --dev
    ```

Then, in a second terminal, create the topic, and produce, consume and ack it with [steps 3 to 5 of the Quickstart](get-started/quickstart.md#produce), acking with the `receipt_handle` your own consume returns:

```sh
curl http://127.0.0.1:7942/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders"}'
```

</div>
</section>
