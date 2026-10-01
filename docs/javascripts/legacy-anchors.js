/*
 * Forwards links to sections that moved in the five-tab layout.
 *
 * mkdocs-redirects sends an old page URL to its new page and keeps the
 * visitor's #hash. When the section behind that hash now lives on
 * another page, or was renamed, the hash matches nothing and the reader
 * lands at the top. This map sends such a hash to the section's new
 * home. It only acts when the current page has no element with that id,
 * so a section that still exists always wins.
 *
 * Keys are "<page path relative to the site root>#<old id>". Values are
 * a path relative to the site root, optionally with a new #id; a value
 * that starts with "#" stays on the same page.
 */
(function () {
  "use strict";

  var MAP = {
    // Delivery contract (was client/guarantees-and-errors)
    "understand/delivery-contract/#status-codes": "reference/status-codes/",
    "understand/delivery-contract/#retry-cheat-sheet": "reference/status-codes/#retry-rules",
    "understand/delivery-contract/#api-stability": "reference/api-stability/",
    "understand/delivery-contract/#the-delivery-guarantee": "#at-least-once",
    "understand/delivery-contract/#the-durability-contract-read-before-trusting-narad-with-anything-important": "#what-202-means",
    "understand/delivery-contract/#ordering-not-guaranteed": "#ordering",
    "understand/delivery-contract/#availability-the-deliberate-trade": "#availability",

    // Deploy on Kubernetes (was operate/)
    "operate/deploy-kubernetes/#tls-story": "operate/production-checklist/#raft-tls",
    "operate/deploy-kubernetes/#rate-limiting-bring-your-own": "operate/production-checklist/#rate-limiting",
    "operate/deploy-kubernetes/#ports-and-probes": "reference/helm-values/#ports-and-probes",
    "operate/deploy-kubernetes/#recovering-after-a-node-outage": "operate/troubleshooting/#quiet-after-outage",
    "operate/deploy-kubernetes/#single-node-laptop-mode": "get-started/quickstart/",
    "operate/deploy-kubernetes/#install-step-by-step": "#install",
    "operate/deploy-kubernetes/#verifying-the-image": "#verify-image",

    // Helm values (was operate/helm-chart)
    "reference/helm-values/#upgrades": "operate/upgrade/",
    "reference/helm-values/#rolling-back-to-an-earlier-release": "operate/upgrade/#roll-back",

    // Monitor and alert: the metric tables moved to the reference
    "operate/monitoring/#full-metric-reference": "reference/metrics/",
    "operate/monitoring/#traffic": "reference/metrics/#traffic",
    "operate/monitoring/#queue-health": "reference/metrics/#queue-health",
    "operate/monitoring/#fan-out": "reference/metrics/#fan-out",
    "operate/monitoring/#storage-engine": "reference/metrics/#storage-engine",
    "operate/monitoring/#storage-housekeeping": "reference/metrics/#storage-housekeeping",
    "operate/monitoring/#cluster-misc": "reference/metrics/#cluster-misc",
    "operate/monitoring/#reading-the-dashboards-under-failure": "operate/troubleshooting/",

    // Scale out and in (was operate/scaling-and-recovery)
    "operate/scaling/#what-failure-actually-does": "understand/delivery-contract/#failure-matrix",
    "operate/scaling/#measured-capacity": "reference/capacity/#measured-capacity",
    "operate/scaling/#disk-sizing": "reference/capacity/#disk-sizing",
    "operate/scaling/#backups": "operate/backups/",

    // Quickstart (was client/)
    "get-started/quickstart/#what-narad-gives-you": "get-started/concepts/",
    "get-started/quickstart/#authentication": "build/connect/#credentials",

    // Build pages (were under client/)
    "build/cli/#command-reference": "reference/cli/",
    "build/cli/#the-sixty-second-demo": "#watch-messages-flow",
    "build/consuming/#the-lifecycle-of-one-message": "get-started/concepts/#message-lifecycle",
    "build/fanout-and-delay/#replication-when-you-ask-for-it": "operate/backups/#replica-children",
    "build/topics/#who-can-do-this": "reference/access-model/",
    "build/schemas/#validation-on-produce": "reference/schema-rules/#validation",
    "build/schemas/#fan-out-children": "reference/schema-rules/#fan-out-children",
    "build/schemas/#when-does-a-new-version-take-effect": "reference/schema-rules/#when-a-version-takes-effect",
    "build/producing/#response-codes": "reference/status-codes/",

    // Access model (was client/users-and-access)
    "reference/access-model/#managing-users-admin-only": "operate/users/",
    "reference/access-model/#practical-advice": "operate/users/",

    // Rebalance (was internals/rebalance)
    "understand/rebalance/#operating-it": "operate/scaling/#decommission"
  };

  function root() {
    // Material sets __md_scope to the site root as an absolute URL.
    try {
      if (typeof __md_scope !== "undefined") return __md_scope.pathname;
    } catch (e) {}
    return "/";
  }

  function forward() {
    var hash = location.hash;
    if (!hash || hash.length < 2) return;
    var id;
    try {
      id = decodeURIComponent(hash.slice(1));
    } catch (e) {
      return;
    }
    if (document.getElementById(id)) return;

    var base = root();
    var path = location.pathname;
    if (path.indexOf(base) !== 0) return;
    var rel = path.slice(base.length).replace(/index\.html$/, "");
    if (rel && rel.charAt(rel.length - 1) !== "/") rel += "/";

    var target = MAP[rel + "#" + id];
    if (!target) return;
    if (target.charAt(0) === "#") {
      location.replace(target);
    } else {
      location.replace(base + target);
    }
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(forward);
  } else {
    document.addEventListener("DOMContentLoaded", forward);
  }
  window.addEventListener("hashchange", forward);
})();
