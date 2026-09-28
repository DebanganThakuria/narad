/* Signal Line: anything that scrolls sideways can be reached by keyboard.

   A box that is wider than its column scrolls inside its host: a wide
   Mermaid diagram, a home-page drawing on a screen narrower than 375px,
   the recorded terminal session on a desktop. While, and only while, a
   host overflows, it becomes a named, focusable group, so the arrow keys
   can scroll it; when it fits again it leaves the Tab order. Chrome makes
   an overflowing box a tab stop on its own but gives it no name, and
   Safari makes it no tab stop at all.

   Hosts are watched as they arrive, so this survives instant navigation.
   If anything here fails, the hosts still scroll with a pointer. */
(function () {
  var HOSTS = "div.mermaid, .nr-dia__frame, .nr-term pre";

  /* A drawing's panel takes the name of the drawing it shows (the wide or
     the tall copy, whichever is displayed) */
  function name(host) {
    if (host.matches(".nr-dia__frame")) {
      var svgs = host.querySelectorAll("svg");
      for (var i = 0; i < svgs.length; i++) {
        var title = svgs[i].querySelector("title[id]");
        if (title && getComputedStyle(svgs[i]).display !== "none") {
          return { labelledby: title.id };
        }
      }
      return { label: "Diagram, scrolls sideways" };
    }
    if (host.matches(".nr-term pre")) return { label: "Terminal session, scrolls sideways" };
    return { label: "Diagram, scrolls sideways" };
  }

  function update(host) {
    if (host.scrollWidth > host.clientWidth + 1) {
      var n = name(host);
      host.setAttribute("tabindex", "0");
      host.setAttribute("role", "group");
      if (n.labelledby) {
        host.setAttribute("aria-labelledby", n.labelledby);
        host.removeAttribute("aria-label");
      } else {
        host.setAttribute("aria-label", n.label);
        host.removeAttribute("aria-labelledby");
      }
    } else if (host.hasAttribute("tabindex")) {
      host.removeAttribute("tabindex");
      host.removeAttribute("role");
      host.removeAttribute("aria-label");
      host.removeAttribute("aria-labelledby");
    }
  }

  try {
    var watching = new WeakSet();
    var sizes = new ResizeObserver(function (entries) {
      entries.forEach(function (entry) {
        update(entry.target);
      });
    });

    var watch = function (root) {
      var hosts = [];
      if (root.matches && root.matches(HOSTS)) hosts.push(root);
      if (root.querySelectorAll) hosts.push.apply(hosts, root.querySelectorAll(HOSTS));
      hosts.forEach(function (host) {
        if (watching.has(host)) return;
        watching.add(host);
        sizes.observe(host);
      });
    };

    new MutationObserver(function (records) {
      records.forEach(function (record) {
        record.addedNodes.forEach(function (node) {
          if (node.nodeType === 1) watch(node);
        });
      });
    }).observe(document.documentElement, { childList: true, subtree: true });

    watch(document);
  } catch (e) {
    /* Hosts still scroll with a pointer */
  }
})();
