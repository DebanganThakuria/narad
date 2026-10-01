/* Signal Line: Mermaid diagrams in the theme's lines and colours.

   Material renders each diagram in a closed shadow root and themes it only
   through the --md-mermaid-* colour variables, so line weights and the
   diagram types Material does not style cannot come from extra.css.
   Material lazy-loads Mermaid, which then assigns globalThis.mermaid; this
   setter catches that assignment and adds to the config Material passes
   to mermaid.initialize:

   - a few rules appended to themeCSS (1.5px lines, arrowheads, edge
     labels on the ground, and the timeline type in ink and paper);
   - useMaxWidth: false, so a wide diagram keeps its natural size and its
     host scrolls sideways, instead of shrinking its text to 7-10px.

   A host that scrolls is made keyboard-reachable by scroll-hosts.js. If
   anything here fails, diagrams still render with Material's own styles. */
(function () {
  var EXTRA =
    ".node rect,.node circle,.node ellipse,.node polygon,.node path{stroke-width:1.5px}" +
    ".flowchart-link,.edgePath .path,.transition,.relation{stroke-width:1.5px}" +
    "g.stateGroup rect,.statediagram-state rect,.statediagram-cluster rect{stroke-width:1.5px}" +
    ".actor{stroke-width:1.5px}" +
    ".actor-line,line.actor-line{stroke:var(--md-mermaid-sequence-actor-line-color);stroke-width:1px}" +
    ".messageLine0,.messageLine1{stroke-width:1.5px}" +
    ".note{stroke-width:1px}" +
    ".cluster rect{fill:var(--nr-surface);stroke:var(--nr-muted);stroke-width:1px}" +
    ".sequenceNumber{font-weight:600}" +
    /* Mermaid 11 prefixes marker ids with the diagram id, so Material's
       #arrowhead and #sequencenumber rules no longer match */
    "[id$='-arrowhead'] path,[id$='-filled-head'] path{fill:var(--md-mermaid-sequence-message-line-color)!important;stroke:none!important}" +
    "[id$='-crosshead'] path{fill:none!important;stroke:var(--md-mermaid-sequence-message-line-color)!important}" +
    "[id$='-sequencenumber']{fill:var(--md-mermaid-sequence-number-bg-color)!important}" +
    /* Edge labels sit on the ground with no box around them (Mermaid 11
       wraps them in a half-transparent grey .labelBkg) */
    ".labelBkg{background-color:var(--md-mermaid-label-bg-color)!important}" +
    ".edgeLabel rect{stroke:none!important}" +
    /* Timeline: Material leaves Mermaid's rainbow section fills in place,
       which puts paper text on yellow in the dark scheme. Paper boxes with
       a 1.5px ink outline, ink text, ink connectors, like every node. */
    ".timeline-node .node-bkg{fill:var(--md-mermaid-node-bg-color)!important;stroke:var(--md-mermaid-node-fg-color)!important;stroke-width:1.5px}" +
    ".timeline-node text,.timeline-node tspan{fill:var(--md-mermaid-node-fg-color)!important;font-family:var(--md-mermaid-font-family)}" +
    ".timeline-node line{stroke:none!important}" +
    ".lineWrapper line{stroke:var(--md-mermaid-edge-color)!important;stroke-width:1.5px}" +
    ".eventWrapper{filter:none!important}" +
    /* The timeline title is a bare <text font-size="4ex"> */
    "text[font-size='4ex']{fill:var(--md-mermaid-label-fg-color)!important;font-family:var(--md-mermaid-font-family)}";

  var NATURAL = { useMaxWidth: false };

  function patch(m) {
    if (!m || typeof m.initialize !== "function" || m.__signalLine) return m;
    var initialize = m.initialize.bind(m);
    m.initialize = function (config) {
      var next = Object.assign({}, config);
      next.themeCSS = (next.themeCSS || "") + EXTRA;
      ["flowchart", "sequence", "state", "timeline", "class", "er"].forEach(function (type) {
        next[type] = Object.assign({}, next[type], NATURAL);
      });
      return initialize(next);
    };
    m.__signalLine = true;
    return m;
  }

  try {
    if (typeof window.mermaid !== "undefined") {
      patch(window.mermaid);
    } else {
      var current;
      Object.defineProperty(window, "mermaid", {
        configurable: true,
        enumerable: true,
        get: function () {
          return current;
        },
        set: function (value) {
          current = patch(value);
        }
      });
    }
  } catch (e) {
    /* Leave Mermaid untouched */
  }
})();
