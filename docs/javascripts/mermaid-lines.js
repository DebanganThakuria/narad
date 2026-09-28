/* Signal Line: 1.5px outlines and lines in Mermaid diagrams.

   Material renders each diagram in a closed shadow root and themes it only
   through the --md-mermaid-* colour variables, so line weights cannot come
   from extra.css. Material lazy-loads Mermaid, which then assigns
   globalThis.mermaid; this setter catches that assignment and appends a few
   rules to the themeCSS that Material passes to mermaid.initialize. If
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
    "[id$='-sequencenumber']{fill:var(--md-mermaid-sequence-number-bg-color)!important}";

  function patch(m) {
    if (!m || typeof m.initialize !== "function" || m.__signalLine) return m;
    var initialize = m.initialize.bind(m);
    m.initialize = function (config) {
      var next = Object.assign({}, config);
      next.themeCSS = (next.themeCSS || "") + EXTRA;
      return initialize(next);
    };
    m.__signalLine = true;
    return m;
  }

  try {
    if (typeof window.mermaid !== "undefined") {
      patch(window.mermaid);
      return;
    }
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
  } catch (e) {
    /* Leave Mermaid untouched */
  }
})();
